use std::borrow::Cow;

use anyhow::{bail, Result};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

use crate::{page::Cell, Schema, Type, Value};

#[derive(Parser)]
#[grammar = "sql.pest"]
struct SQLParser;

#[derive(Debug)]
pub enum SimpleColumn {
    Wildcard,
    String(String),
    Dotted(Vec<String>),
}

impl SimpleColumn {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        match pair.as_rule() {
            Rule::identifier => {
                let string = pair.as_span().as_str().to_string();
                Ok(Self::String(string))
            }
            Rule::wildcard => Ok(Self::Wildcard),
            Rule::dotted_field => {
                let inner = pair.into_inner();
                let strings = inner.map(|p| p.as_span().as_str().to_string()).collect();

                Ok(Self::Dotted(strings))
            }
            _ => bail!("Malformed query"),
        }
    }
}

#[derive(Debug)]
pub enum Column {
    Alias(SimpleColumn, String),
    Simple(SimpleColumn),
}

impl Column {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        match pair.as_rule() {
            Rule::simple_field => {
                let mut inner = pair.into_inner();
                let simple = inner.next().unwrap();
                let simple = SimpleColumn::new(simple)?;
                Ok(Self::Simple(simple))
            }
            Rule::aliased_field => {
                let mut inner = pair.into_inner();
                let simple = inner.next().unwrap();
                let simple = simple.into_inner().next().unwrap();
                let alias = inner.next().unwrap();

                let simple = SimpleColumn::new(simple)?;
                let alias = alias.as_span().as_str().to_string();

                Ok(Self::Alias(simple, alias))
            }
            _ => bail!("Malformed query"),
        }
    }
}

#[derive(Debug)]
pub struct SelectClause {
    pub columns: Vec<Column>,
}

impl SelectClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let columns = pair
            .into_inner()
            .map(Column::new)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { columns })
    }
}

#[derive(Debug)]
pub struct FromClause {
    pub table: String,
}

impl FromClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let inner = pair.into_inner().next().unwrap();
        let table = inner.as_span().as_str().to_string();
        Ok(Self { table })
    }
}

#[derive(Debug, PartialEq)]
pub enum Conditional {
    Equals,
}

#[derive(Debug)]
pub struct Condition<'a> {
    column: String,
    conditional: Conditional,
    expected: Value<'a>,
}

impl<'a> Condition<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let mut inner = pair.into_inner();
        let column = inner.next().unwrap().as_span().as_str().to_string();
        let conditional = match inner.next().unwrap().as_span().as_str() {
            "=" => Conditional::Equals,
            _ => bail!("Malformed query"),
        };

        let expected = inner.next().unwrap().as_span().as_str().to_string();

        let expected = if expected == "null" {
            Value::Null
        } else if expected.starts_with('\'') {
            Value::Text(Cow::Owned(expected[1..expected.len() - 1].to_string()))
        } else if expected.contains('.') {
            Value::Float(expected.parse::<f64>()?)
        } else {
            Value::Integer(expected.parse::<u32>()?)
        };

        Ok(Self {
            column,
            conditional,
            expected,
        })
    }

    pub fn r#match(&self, cell: &Cell<'_>) -> Result<bool> {
        let Ok(value) = cell.get(&self.column) else {
            bail!("column {:?} not found", self.column)
        };

        match self.conditional {
            Conditional::Equals => Ok(value == self.expected),
        }
    }

    pub fn filters(&self) -> Vec<(Cow<'_, str>, Value<'_>)> {
        if self.conditional == Conditional::Equals {
            vec![(self.column.clone().into(), self.expected.clone())]
        } else {
            vec![]
        }
    }
}

#[derive(Debug)]
pub enum WhereClause<'a> {
    Condition(Condition<'a>),
    And(Box<WhereClause<'a>>, Box<WhereClause<'a>>),
    Or(Box<WhereClause<'a>>, Box<WhereClause<'a>>),
}

impl<'a> WhereClause<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let binary = pair.into_inner().next().unwrap();

        let mut condition = None;
        let mut operation = None;

        for pair in binary.into_inner() {
            match pair.as_rule() {
                Rule::condition => {
                    let other = Self::Condition(Condition::new(pair)?);

                    let new = match (condition, operation) {
                        (None, _) => other,
                        (Some(cond), Some("or")) => {
                            operation = None;
                            Self::Or(Box::new(cond), Box::new(other))
                        }
                        (Some(cond), Some("and")) => {
                            operation = None;

                            if let Self::Or(l, r) = cond {
                                Self::Or(l, Box::new(Self::And(r, Box::new(other))))
                            } else {
                                Self::And(Box::new(cond), Box::new(other))
                            }
                        }
                        _ => bail!("Malformed query"),
                    };

                    condition = Some(new);
                }
                Rule::binary_operator => {
                    operation = Some(pair.as_span().as_str());
                }
                _ => bail!("Malformed query"),
            }
        }

        Ok(condition.unwrap())
    }

    pub fn r#match(&self, cell: &Cell<'_>) -> Result<bool> {
        match self {
            WhereClause::Condition(cond) => cond.r#match(cell),
            WhereClause::And(a, b) => Ok(a.r#match(cell)? && b.r#match(cell)?),
            WhereClause::Or(a, b) => Ok(a.r#match(cell)? || b.r#match(cell)?),
        }
    }

    // Returns a list of check needed
    pub fn filters(&self) -> Vec<(Cow<'_, str>, Value<'_>)> {
        match self {
            WhereClause::Condition(cond) => cond.filters(),
            WhereClause::And(a, b) => [a.filters(), b.filters()].concat(),
            WhereClause::Or(_, _) => vec![],
        }
    }
}

#[derive(Debug)]
pub struct LimitClause {
    pub limit: usize,
}

impl LimitClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let number = pair.into_inner().next().unwrap();
        let number = number.as_span().as_str();
        let limit = str::parse::<usize>(number)?;

        Ok(LimitClause { limit })
    }
}

#[derive(Debug)]
pub enum Command<'a> {
    Select {
        select: SelectClause,
        from: FromClause,
        r#where: Option<WhereClause<'a>>,
        limit: Option<LimitClause>,
    },

    CreateTable {
        table: String,
        schema: Schema,
    },

    CreateIndex {
        index: String,
        table: String,
        columns: Vec<String>,
    },
}

impl<'a> Command<'a> {
    pub fn parse(query: &str) -> Result<Self> {
        println!("query: {query:?}");
        let parsed = SQLParser::parse(Rule::query, query)?.next().unwrap();
        let pair = parsed.into_inner().next().unwrap();

        match pair.as_rule() {
            Rule::select_query => {
                let mut select = None;
                let mut from = None;
                let mut r#where = None;
                let mut limit = None;

                for p in pair.into_inner() {
                    match p.as_rule() {
                        Rule::EOI => {}
                        Rule::select_clause => select = Some(SelectClause::new(p)?),
                        Rule::from_clause => from = Some(FromClause::new(p)?),
                        Rule::where_clause => r#where = Some(WhereClause::new(p)?),
                        Rule::limit_clause => limit = Some(LimitClause::new(p)?),
                        _ => bail!("Malformed query"),
                    }
                }

                let (Some(select), Some(from)) = (select, from) else {
                    bail!("Malformed query")
                };

                Ok(Self::Select {
                    select,
                    from,
                    r#where,
                    limit,
                })
            }
            Rule::create_table => {
                let mut table = None;
                let mut schema = vec![];

                for p in pair.into_inner() {
                    match p.as_rule() {
                        Rule::EOI => {}
                        Rule::identifier => table = Some(p.as_span().as_str().to_string()),
                        Rule::table_column => {
                            let mut inner = p.into_inner();
                            let name = inner.next().unwrap();
                            let name = name.as_span().as_str().to_string();

                            let r#type = inner.next().unwrap();
                            let r#type = r#type.as_span().as_str().to_string();
                            let r#type = match r#type.to_lowercase().as_str() {
                                "int" | "integer" => Type::Integer,
                                "text" => Type::Text,
                                t => bail!("Missing type: {t:?}"),
                            };

                            schema.push((name, r#type));
                        }
                        _ => bail!("Malformed query"),
                    }
                }

                let Some(table) = table else {
                    bail!("Malformed query")
                };

                let schema = Schema(schema);

                Ok(Self::CreateTable { table, schema })
            }

            Rule::create_index => {
                let mut inner = pair.into_inner();
                let index = inner.next().unwrap();
                let index = index.as_span().as_str().to_string();

                let table = inner.next().unwrap();
                let table = table.as_span().as_str().to_string();

                let columns = inner.next().unwrap();
                let columns = columns
                    .into_inner()
                    .map(|p| p.as_span().as_str().to_string())
                    .collect::<Vec<_>>();

                Ok(Self::CreateIndex {
                    index,
                    table,
                    columns,
                })
            }

            _ => bail!("Wrong query"),
        }
    }
}
