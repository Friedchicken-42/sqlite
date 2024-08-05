use std::{borrow::Cow, num::NonZeroUsize};

use anyhow::{bail, Result};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

use crate::{
    display::DisplayMode, materialized::Materialized, view::View, Column, Row, Schema, SchemaRow,
    Sqlite, Table, Type, Value,
};

#[derive(Parser)]
#[grammar = "sql.pest"]
struct SQLParser;

#[derive(Debug, PartialEq)]
pub enum SimpleColumn {
    String(String),
    Dotted {
        table: String,
        column: String,
    },
    Function {
        name: String,
        param: Box<InputColumn>,
    },
}

impl SimpleColumn {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        match pair.as_rule() {
            Rule::identifier => {
                let string = pair.as_span().as_str().to_string();
                Ok(Self::String(string))
            }
            Rule::dotted_field => {
                let mut inner = pair.into_inner();
                let table = inner.next().unwrap();
                let table = table.as_span().as_str().to_string();
                let column = inner.next().unwrap();
                let column = column.as_span().as_str().to_string();

                Ok(Self::Dotted { table, column })
            }
            Rule::function => {
                let mut inner = pair.into_inner();
                let name = inner.next().unwrap();
                let name = name.as_span().as_str().to_string();

                let param = inner.next().unwrap();
                let param = InputColumn::new(param)?;
                let param = Box::new(param);

                Ok(Self::Function { name, param })
            }
            _ => bail!("[Simple Column] Malformed query"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum InputColumn {
    Wildcard,
    Alias(SimpleColumn, String),
    Simple(SimpleColumn),
}

impl InputColumn {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        match pair.as_rule() {
            Rule::wildcard => Ok(Self::Wildcard),
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
    pub columns: Vec<InputColumn>,
}

impl SelectClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let columns = pair
            .into_inner()
            .map(InputColumn::new)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { columns })
    }
}

#[derive(Debug, Clone)]
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

    pub fn name(&self) -> &str {
        match self {
            FromTable::Simple(n) => n,
            FromTable::Alias(n, _) => n,
        }
    }

    pub fn alias(&self) -> &str {
        match self {
            FromTable::Simple(n) => n,
            FromTable::Alias(_, n) => n,
        }
    }
}

#[derive(Debug, Clone)]
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
                    let condition = Condition::new(pair)?;
                    conditions.push(condition);
                }
                _ => bail!("[From Clause] Malformed query"),
            }
        }

        Ok(Self { tables, conditions })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Conditional {
    Less,
    LessEqual,
    Equal,
    NotEqual,
    GreaterEqual,
    Greater,
}

#[derive(Debug, Clone)]
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
            let column = pair.as_str().into();
            Self::Column(column)
        };

        Ok(expected)
    }
}

#[derive(Debug, Clone)]
pub struct Condition<'a> {
    column: Column,
    conditional: Conditional,
    expected: Expected<'a>,
}

impl<'a> Condition<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let mut inner = pair.into_inner();
        let column = inner.next().unwrap();
        let column = column.as_str().into();

        let conditional = match inner.next().unwrap().as_span().as_str() {
            "<" => Conditional::Less,
            "<=" => Conditional::LessEqual,
            "=" => Conditional::Equal,
            "!=" => Conditional::NotEqual,
            ">=" => Conditional::GreaterEqual,
            ">" => Conditional::Greater,
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

    fn matches(&self, row: &dyn Row) -> bool {
        let value = row.get(&self.column).unwrap();

        let other = match &self.expected {
            Expected::Value(value) => value,
            Expected::Column(col) => &row.get(col).unwrap(),
        };

        match self.conditional {
            Conditional::Less => value < *other,
            Conditional::LessEqual => value <= *other,
            Conditional::Equal => value == *other,
            Conditional::NotEqual => value != *other,
            Conditional::GreaterEqual => value >= *other,
            Conditional::Greater => value > *other,
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

    pub fn matches(&self, row: &dyn Row) -> bool {
        match self {
            WhereClause::Condition(cond) => cond.matches(row),
            WhereClause::And(a, b) => a.matches(row) && b.matches(row),
            WhereClause::Or(a, b) => a.matches(row) || b.matches(row),
        }
    }
}

#[derive(Debug)]
pub struct LimitClause {
    pub limit: NonZeroUsize,
}

impl LimitClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Option<Self>> {
        let number = pair.into_inner().next().unwrap();
        let number = number.as_span().as_str();
        let limit = str::parse::<usize>(number)?;
        let limit = NonZeroUsize::new(limit);

        Ok(limit.map(|limit| LimitClause { limit }))
    }
}

#[derive(Debug)]
pub struct GroupbyClause {
    pub columns: Vec<SimpleColumn>,
}

impl GroupbyClause {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let columns = pair
            .into_inner()
            .map(SimpleColumn::new)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { columns })
    }
}

#[derive(Debug)]
pub struct Select<'a> {
    pub select: SelectClause,
    pub from: FromClause<'a>,
    pub r#where: Option<WhereClause<'a>>,
    pub limit: Option<LimitClause>,
    pub groupby: Option<GroupbyClause>,
}

impl<'a> Select<'a> {
    fn new(pair: Pair<'_, Rule>) -> Result<Self> {
        let mut select = None;
        let mut from = None;
        let mut r#where = None;
        let mut limit = None;
        let mut groupby = None;

        for p in pair.into_inner() {
            match p.as_rule() {
                Rule::EOI => {}
                Rule::select_clause => select = Some(SelectClause::new(p)?),
                Rule::from_clause => from = Some(FromClause::new(p)?),
                Rule::where_clause => r#where = Some(WhereClause::new(p)?),
                Rule::limit_clause => limit = LimitClause::new(p)?,
                Rule::groupby_clause => groupby = Some(GroupbyClause::new(p)?),
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
            groupby,
        })
    }

    pub fn execute(self, db: &'a Sqlite) -> Result<Box<dyn Table + '_>> {
        let materielized = self.groupby.is_some();
        let btreepage =
            self.select.columns == vec![InputColumn::Wildcard] && self.from.tables.len() == 1;

        if materielized {
            let primary = Self {
                select: SelectClause {
                    columns: vec![InputColumn::Wildcard], // TODO: check if this is necessaary
                },
                from: self.from.clone(),
                groupby: None,
                limit: None,
                r#where: None,
            };

            let table = primary.execute(db)?;

            let materialized = Materialized::new(self, table)?;
            Ok(Box::new(materialized))
        } else if btreepage {
            let table = &self.from.tables[0];
            db.search(table.name(), vec![])
        } else {
            let view = View::new(self, db)?;
            Ok(Box::new(view))
        }
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
        let mut schema = Schema::new();

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

                    schema.push(SchemaRow {
                        column: name.as_str().into(),
                        r#type,
                    });
                }
                _ => bail!("[Create Table Command] Malformed query"),
            }
        }

        let Some(table) = table else {
            bail!("[Create Table Command] Malformed query")
        };

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
            Command::Select(select) => {
                use std::io::Cursor;
                let mut f = Cursor::new(vec![]);
                let view = select.execute(db)?;
                db.display(&mut f, view, DisplayMode::Table)?;
                println!("{}", std::str::from_utf8(f.get_ref())?);
                Ok(())
            }
            Command::CreateTable(createtable) => createtable.execute(db),
            Command::CreateIndex(createindex) => createindex.execute(db),
        }
    }
}
