use anyhow::{bail, Result};
use sqlite::Sqlite;

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
        ".tables" => db.show_tables()?,
        ".schema" => db.show_schema()?,
        _command => {
            todo!()
        }
    }

    Ok(())
}
