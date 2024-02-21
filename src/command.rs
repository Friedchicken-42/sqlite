use anyhow::{bail, Result};

trait Parse {
    fn parse(statements: &mut Vec<String>) -> Result<Self>
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct SelectST {
    pub columns: Vec<String>,
}

impl Parse for SelectST {
    fn parse(statements: &mut Vec<String>) -> Result<Self> {
        let mut columns = vec![];

        loop {
            if statements.is_empty() {
                anyhow::bail!("missing statement");
            }

            columns.push(statements.remove(0));
            if !statements.get(0).is_some_and(|s| s == ",") {
                break;
            }
            statements.remove(0);
        }

        Ok(Self { columns })
    }
}

#[derive(Debug)]
pub struct FromST {
    pub table: String,
}

impl Parse for FromST {
    fn parse(statements: &mut Vec<String>) -> Result<Self> {
        if statements.is_empty() {
            anyhow::bail!("missing statement");
        }

        let table = statements.remove(0);
        Ok(Self { table })
    }
}

#[derive(Debug)]
pub enum Condition {
    Equals,
}

#[derive(Debug)]
pub struct WhereST {
    pub column: String,
    pub condition: Condition,
    pub expected: String,
}

impl Parse for WhereST {
    fn parse(statements: &mut Vec<String>) -> Result<Self> {
        let column = statements.remove(0);
        let condition = match statements.remove(0).as_str() {
            "=" => Condition::Equals,
            c => bail!("unmatched condition: {c:?}"),
        };
        let expected = statements.remove(0);
        // TODO: remove this and add type checking for rows
        let expected = if expected.starts_with("\'") {
            expected[1..expected.len() - 1].to_string()
        } else {
            expected
        };

        Ok(Self {
            column,
            condition,
            expected,
        })
    }
}

#[derive(Debug)]
pub enum Command {
    Select {
        select: SelectST,
        from: FromST,
        r#where: Option<WhereST>,
    },
}

impl Command {
    pub fn parse(str: &str) -> Result<Command> {
        let mut statements = Command::lexer(str)?;

        let command = match statements.remove(0).as_str() {
            "select" => {
                let select = SelectST::parse(&mut statements)?;

                statements.remove(0); // TODO: check from
                let from = FromST::parse(&mut statements)?;

                let r#where = if statements.get(0).is_some_and(|s| s == "where") {
                    statements.remove(0);
                    Some(WhereST::parse(&mut statements)?)
                } else {
                    None
                };

                Command::Select {
                    select,
                    from,
                    r#where,
                }
            }
            s => anyhow::bail!("unmatched statement {s:?}"),
        };

        Ok(command)
    }

    fn lexer(str: &str) -> Result<Vec<String>> {
        let mut tokens = vec![String::new()];

        for char in str.chars() {
            match char {
                'a'..='z' | 'A'..='Z' | '\'' => {
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
        println!("{tokens:?}");

        Ok(tokens)
    }
}
