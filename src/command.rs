use anyhow::Result;

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
pub enum Command {
    Select { select: SelectST, from: FromST },
}

impl Command {
    pub fn parse(str: &str) -> Result<Command> {
        let mut statements = Command::lexer(str)?;

        let command = match statements.remove(0).as_str() {
            "select" => {
                let select = SelectST::parse(&mut statements)?;
                statements.remove(0); // TODO: check from
                let from = FromST::parse(&mut statements)?;
                Command::Select { select, from }
            }
            s => anyhow::bail!("unmatched statement {s:?}"),
        };

        Ok(command)
    }

    fn lexer(str: &str) -> Result<Vec<String>> {
        let mut tokens = vec![String::new()];

        for char in str.chars() {
            match char {
                'a'..='z' => {
                    tokens.last_mut().unwrap().push(char);
                }
                ' ' => {
                    if !tokens.last().unwrap().is_empty() {
                        tokens.push(String::new());
                    }
                }
                ',' => {
                    tokens.push(",".into());
                    tokens.push(String::new());
                }
                _ => anyhow::bail!("unmatched char {char:?}"),
            }
        }

        Ok(tokens)
    }
}
