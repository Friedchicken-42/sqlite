use anyhow::{bail, Result};
use sqlite::{Row, Rows, Sqlite, Value};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db = Sqlite::read(&args[1])?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size());

            let page = db.root()?;
            println!("number of tables: {}", page.count());
        }
        ".tables" => {
            let mut table = db.root()?;

            while let Some(cell) = table.next() {
                let Value::Text(name) = cell.get("name")? else {
                    panic!("expected text");
                };

                if name != "sqlite_sequence" {
                    print!("{name} ");
                }
            }

            println!()
        }
        ".schema" => db.show_schema()?,
        _command => {
            todo!()
        }
    }

    Ok(())
}
