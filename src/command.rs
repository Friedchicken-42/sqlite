use std::borrow::Cow;

use anyhow::{bail, format_err, Result};

use crate::{Cell, Schema, Type, Value};

trait Parse {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized;

    fn check(statements: &mut Vec<&str>, name: &str) -> bool {
        statements
            .first()
            .is_some_and(|stmt| stmt.to_lowercase() == name)
    }
}

#[derive(Debug)]
pub struct SelectSt {
    pub columns: Vec<String>,
}

impl Parse for SelectSt {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "select") {
            return Ok(None);
        }
        statements.remove(0);

        let mut columns = vec![];

        loop {
            if statements.is_empty() {
                bail!("missing statement");
            }

            columns.push(statements.remove(0).to_string());
            if !statements.first().is_some_and(|s| *s == ",") {
                break;
            }
            statements.remove(0);
        }

        Ok(Some(Self { columns }))
    }
}

#[derive(Debug)]
pub struct FromSt {
    pub table: String,
}

impl Parse for FromSt {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "from") {
            return Ok(None);
        }
        statements.remove(0);

        if statements.is_empty() {
            bail!("missing statement");
        }

        let table = statements.remove(0).to_string();
        Ok(Some(Self { table }))
    }
}

#[derive(Debug)]
pub enum Condition {
    Equals,
}

#[derive(Debug)]
pub struct WhereSt<'a> {
    // TODO: add multiple conditions
    pub column: String,
    pub condition: Condition,
    pub expected: Value<'a>,
}

impl<'a> Parse for WhereSt<'a> {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "where") {
            return Ok(None);
        }
        statements.remove(0);

        let column = statements.remove(0).to_string();
        let condition = match statements.remove(0) {
            "=" => Condition::Equals,
            c => bail!("unmatched condition: {c:?}"),
        };
        let expected = statements.remove(0);

        let expected = if expected == "null" {
            Value::Null
        } else if expected.starts_with('\'') {
            Value::Text(Cow::Owned(expected[1..expected.len() - 1].to_string()))
        } else if expected.contains('.') {
            Value::Float(expected.parse::<f64>()?)
        } else {
            Value::Integer(expected.parse::<u32>()?)
        };

        Ok(Some(Self {
            condition,
            column,
            expected,
        }))
    }
}

impl WhereSt<'_> {
    pub fn r#match(&self, cell: &Cell<'_>) -> bool {
        let value = cell.get(&self.column).unwrap();
        match self.condition {
            Condition::Equals => value == self.expected,
        }
    }
}

impl Parse for Schema {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        let mut schema = vec![];
        if !Self::check(statements, "(") {
            bail!("missing \"(\"")
        }
        statements.remove(0);

        loop {
            let (name, r#type) = match statements[..] {
                [name, r#type, ..] => {
                    let r#type = match r#type {
                        "integer" | "int" => Type::Integer,
                        "text" => Type::Text,
                        s => bail!("[schema] unparsed {s}"),
                    };
                    (name.to_string(), r#type)
                }
                [")"] => break,
                [c] => bail!("unexpected pattern: {c:?}"),
                [] => bail!("malformed statement"),
            };

            schema.push((name, r#type));
            statements.drain(0..2);

            while let Some(other) = statements.first() {
                match *other {
                    "," => {
                        statements.remove(0);
                        break;
                    }
                    ")" => break,
                    _ => statements.remove(0),
                };
            }
        }

        Ok(Some(Self(schema)))
    }
}

#[derive(Debug)]
pub struct On {
    pub table: String,
    pub columns: Vec<String>,
}

impl Parse for On {
    fn parse(statements: &mut Vec<&str>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "on") {
            return Ok(None);
        }
        statements.remove(0);

        let table = statements.remove(0).to_string();

        statements.remove(0);

        let mut columns = vec![];

        loop {
            match statements.first() {
                Some(&")") => break,
                Some(&",") => {}
                Some(column) => columns.push(column.to_string()),
                None => break,
            };

            statements.remove(0);
        }

        Ok(Some(Self {
            table: table.to_string(),
            columns,
        }))
    }
}

#[derive(Debug)]
pub enum Command<'a> {
    Select {
        select: SelectSt,
        from: FromSt,
        r#where: Option<WhereSt<'a>>,
    },

    CreateTable {
        table: String,
        schema: Schema,
    },

    CreateIndex {
        index: String,
        on: On,
    },
}

impl<'a> Command<'a> {
    pub fn parse(string: &str) -> Result<Self> {
        let mut statements = Self::lexer(string)?;

        // TODO: better cases
        let command = match statements[..] {
            ["select" | "SELECT", ..] => {
                let select = SelectSt::parse(&mut statements)?
                    .ok_or(format_err!("[select] select statemente required"))?;
                let from = FromSt::parse(&mut statements)?
                    .ok_or(format_err!("[select] from statemente required"))?;
                let r#where = WhereSt::parse(&mut statements)?;

                Command::Select {
                    select,
                    from,
                    r#where,
                }
            }
            ["create" | "CREATE", "table" | "TABLE", name, ..] => {
                statements.drain(0..3);
                let schema = Schema::parse(&mut statements)?
                    .ok_or(format_err!("[create table] missing schema"))?;

                Command::CreateTable {
                    table: name.to_string(),
                    schema,
                }
            }
            ["create" | "CREATE", "index" | "INDEX", index, ..] => {
                statements.drain(0..3);

                let on = On::parse(&mut statements)?
                    .ok_or(format_err!("[create index] on statement requires"))?;

                Command::CreateIndex {
                    index: index.to_string(),
                    on,
                }
            }
            [] => bail!("Empty query"),
            _ => bail!("Unhandle query"),
        };

        Ok(command)
    }

    fn lexer(string: &str) -> Result<Vec<&str>> {
        let mut tokens = vec![];

        let mut starting = 0;
        let mut ending = 0;
        let mut quote = false;

        for char in string.chars() {
            match (quote, char) {
                (false, '\'' | '\"') => quote = !quote,
                (true, '\'' | '\"') => {
                    quote = !quote;
                    tokens.push(&string[starting..ending + 1]);
                    starting = ending + 1;
                }
                (false, ' ' | '\n' | '\t') => {
                    if starting != ending {
                        tokens.push(&string[starting..ending]);
                    }
                    starting = ending + 1;
                }
                (false, ',' | '(' | ')') => {
                    if starting != ending {
                        tokens.push(&string[starting..ending]);
                    }
                    tokens.push(&string[ending..ending + 1]);
                    starting = ending + 1;
                }
                _ => {}
            }
            ending += 1;
        }

        if starting != ending {
            tokens.push(&string[starting..ending]);
        }
        Ok(tokens)
    }
}
