use std::borrow::Cow;

use anyhow::{bail, Result};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

// use crate::{page::Cell, Rows, Schema, Sqlite, Type, Value};
use crate::{Schema, Sqlite, Type, Value};

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
            _ => bail!("[Simple Column] Malformed query"),
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
            _ => bail!("[Column] Malformed query"),
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
pub enum FromTable {
    Simple(String),
    Alias(String, String),
}

impl FromTable {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        match pair.as_rule() {
            Rule::identifier => {
                let string = pair.as_span().as_str().to_string();
                Ok(Self::Simple(string))
            }
            Rule::aliased_table => {
                let mut inner = pair.into_inner();
                let simple = inner.next().unwrap();
                let simple = simple.as_span().as_str().to_string();

                let alias = inner.next().unwrap();
                let alias = alias.as_span().as_str().to_string();

                Ok(Self::Alias(simple, alias))
            }
            _ => bail!("[From Table] Malformed query"),
        }
    }
}

#[derive(Debug)]
pub struct FromClause<'a> {
    pub tables: Vec<FromTable>,
    pub conditions: Vec<Condition<'a>>,
}

impl<'a> FromClause<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let inner = pair.into_inner();

        let mut tables = vec![];
        let mut conditions = vec![];

        for pair in inner {
            match pair.as_rule() {
                Rule::identifier | Rule::aliased_table => {
                    let table = FromTable::new(pair)?;
                    tables.push(table);
                }
                Rule::condition => {
                    dbg!(&pair);
                    let condition = Condition::new(pair)?;
                    conditions.push(condition);
                }
                _ => bail!("[From Clause] Malformed query"),
            }
        }

        dbg!(&tables);
        dbg!(&conditions);

        Ok(Self { tables, conditions })
    }
}

#[derive(Debug, PartialEq)]
pub enum Conditional {
    Equals,
}

#[derive(Debug)]
pub enum Expected<'a> {
    Value(Value<'a>),
    Column(Column),
}

impl<'a> Expected<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let expected = pair.as_span().as_str().to_string();

        let expected = if expected == "null" {
            Self::Value(Value::Null)
        } else if expected.starts_with('\'') {
            let string = expected[1..expected.len() - 1].to_string();
            let string = Cow::Owned(string);
            Self::Value(Value::Text(string))
        } else if let Ok(number) = expected.parse::<u32>() {
            Self::Value(Value::Integer(number))
        } else if let Ok(number) = expected.parse::<f64>() {
            Self::Value(Value::Float(number))
        } else {
            let column = SimpleColumn::new(pair)?;
            Self::Column(Column::Simple(column))
        };

        Ok(expected)
    }
}

#[derive(Debug)]
pub struct Condition<'a> {
    column: Column,
    conditional: Conditional,
    expected: Expected<'a>,
}

impl<'a> Condition<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let mut inner = pair.into_inner();
        let column = inner.next().unwrap();
        let column = SimpleColumn::new(column)?;
        let column = Column::Simple(column);

        let conditional = match inner.next().unwrap().as_span().as_str() {
            "=" => Conditional::Equals,
            _ => bail!("[Condition] Malformed query"),
        };

        let expected = inner.next().unwrap();
        let expected = Expected::new(expected)?;

        Ok(Self {
            column,
            conditional,
            expected,
        })
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
                        _ => bail!("[Where Condition] Malformed query"),
                    };

                    condition = Some(new);
                }
                Rule::binary_operator => {
                    operation = Some(pair.as_span().as_str());
                }
                _ => bail!("[Where Clause] Malformed query"),
            }
        }

        Ok(condition.unwrap())
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
pub struct Select<'a> {
    pub select: SelectClause,
    pub from: FromClause<'a>,
    pub r#where: Option<WhereClause<'a>>,
    pub limit: Option<LimitClause>,
}

impl<'a> Select<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
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
                _ => bail!("[Select Command] Malformed query"),
            }
        }

        let (Some(select), Some(from)) = (select, from) else {
            bail!("[Select Command] Malformed query")
        };

        Ok(Select {
            select,
            from,
            r#where,
            limit,
        })
    }

    pub fn execute(self, db: &Sqlite) -> Result<()> {
        let Select {
            select,
            from: FromClause { tables, conditions },
            r#where,
            limit,
        } = self;

        let condition = conditions
            .into_iter()
            .fold(None, |acc, cond| match (acc, cond) {
                (None, c) => Some(WhereClause::Condition(c)),
                (Some(a), b) => {
                    let a = Box::new(a);
                    let c = Box::new(WhereClause::Condition(b));
                    Some(WhereClause::And(a, c))
                }
            });

        let r#where = match (r#where, condition) {
            (None, None) => None,
            (Some(r), Some(c)) => Some(WhereClause::And(Box::new(r), Box::new(c))),
            (Some(x), _) | (_, Some(x)) => Some(x),
        };

        dbg!(&r#where);
        dbg!(&tables);

        Ok(())
    }
}

#[derive(Debug)]
pub struct CreateTable {
    pub table: String,
    pub schema: Schema,
}

impl CreateTable {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
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
                _ => bail!("[Create Table Command] Malformed query"),
            }
        }

        let Some(table) = table else {
            bail!("[Create Table Command] Malformed query")
        };

        let schema = Schema(schema);

        Ok(CreateTable { table, schema })
    }

    pub fn execute(self, db: &Sqlite) -> Result<()> {
        todo!()
    }
}

#[derive(Debug)]
pub struct CreateIndex {
    pub index: String,
    pub table: String,
    pub columns: Vec<String>,
}

impl CreateIndex {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
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

        Ok(CreateIndex {
            index,
            table,
            columns,
        })
    }

    pub fn execute(self, db: &Sqlite) -> Result<()> {
        todo!()
    }
}

#[derive(Debug)]
pub enum Command<'a> {
    Select(Select<'a>),
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
}

impl<'a> Command<'a> {
    pub fn parse(query: &str) -> Result<Self> {
        let parsed = SQLParser::parse(Rule::query, query)?.next().unwrap();
        let pair = parsed.into_inner().next().unwrap();

        match pair.as_rule() {
            Rule::select_query => {
                let select = Select::new(pair)?;
                Ok(Self::Select(select))
            }
            Rule::create_table => {
                let createtable = CreateTable::new(pair)?;
                Ok(Self::CreateTable(createtable))
            }
            Rule::create_index => {
                let createindex = CreateIndex::new(pair)?;
                Ok(Self::CreateIndex(createindex))
            }
            _ => bail!("Wrong query"),
        }
    }

    pub fn execute(self, db: &Sqlite) -> Result<()> {
        match self {
            Command::Select(select) => select.execute(db),
            Command::CreateTable(createtable) => createtable.execute(db),
            Command::CreateIndex(createindex) => createindex.execute(db),
        }
    }
}
