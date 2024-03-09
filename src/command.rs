use std::borrow::Cow;

use anyhow::{bail, format_err, Result};

use crate::Value;

trait Parse {
    fn parse(statements: &mut Vec<String>) -> Result<Option<Self>>
    where
        Self: Sized;

    fn check(statements: &mut Vec<String>, name: &str) -> bool {
        statements.first().is_some_and(|stmt| stmt == name)
    }
}

#[derive(Debug)]
pub struct SelectSt {
    pub columns: Vec<String>,
}

impl Parse for SelectSt {
    fn parse(statements: &mut Vec<String>) -> Result<Option<Self>>
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
                anyhow::bail!("missing statement");
            }

            columns.push(statements.remove(0));
            if !statements.first().is_some_and(|s| s == ",") {
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
    fn parse(statements: &mut Vec<String>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "from") {
            return Ok(None);
        }
        statements.remove(0);

        if statements.is_empty() {
            anyhow::bail!("missing statement");
        }

        let table = statements.remove(0);
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
    fn parse(statements: &mut Vec<String>) -> Result<Option<Self>>
    where
        Self: Sized,
    {
        if !Self::check(statements, "where") {
            return Ok(None);
        }
        statements.remove(0);

        let column = statements.remove(0);
        let condition = match statements.remove(0).as_str() {
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

#[derive(Debug)]
pub enum Command<'a> {
    Select {
        select: SelectSt,
        from: FromSt,
        r#where: Option<WhereSt<'a>>,
    },
}

impl<'a> Command<'a> {
    pub fn parse(string: &str) -> Result<Self> {
        let mut statements = Self::lexer(string)?;

        let command = match statements[0].as_str() {
            "select" => {
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
            c => bail!("Unhandled command: {c:?}"),
        };

        Ok(command)
    }

    fn lexer(string: &str) -> Result<Vec<String>> {
        let mut tokens = vec![String::new()];
        let mut quote = false;

        for char in string.chars() {
            if quote {
                if char == '\'' {
                    quote = false
                }
                tokens.last_mut().unwrap().push(char);
            } else {
                match char {
                    'a'..='z' | 'A'..='Z' | '_' | '\'' => {
                        if char == '\'' {
                            quote = true;
                        }
                        tokens.last_mut().unwrap().push(char);
                    }
                    ' ' => {
                        if !tokens.last().unwrap().is_empty() {
                            tokens.push(String::new());
                        }
                    }
                    c => {
                        if let Some(last) = tokens.last_mut() {
                            if last.is_empty() {
                                last.push(c);
                            } else {
                                tokens.push(c.into());
                                tokens.push(String::new());
                            }
                        }
                    }
                }
            }
        }

        Ok(tokens)
    }
}
