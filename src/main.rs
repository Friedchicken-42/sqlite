use anyhow::{bail, Result};
use sqlite::{Command, Sqlite, Value};

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
            println!("database page size: {}", db.header.page_size);

            let page = db.root()?;
            let cells = page.header.cells;
            println!("number of tables: {}", cells);
        }
        ".tables" => {
            let schema = db.root()?;

            for record in schema.records() {
                let Value::Text(name) = record.get("name") else {
                    panic!("expected text")
                };
                if name != "sqlite_sequence" {
                    print!("{} ", name);
                }
            }
            println!();
        }
        command => {
            let command = Command::parse(command)?;
            db.execute(command)?;
        }
    };
    Ok(())
}
