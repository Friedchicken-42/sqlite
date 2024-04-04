use anyhow::{bail, Result};
use sqlite::{
    command::{Command, On},
    Sqlite, Value,
};

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
            println!("number of tables: {}", page.count());
        }
        ".tables" => {
            let table = db.root()?;
            let mut iter = table.rows();

            while let Some(cell) = iter.next() {
                let Value::Text(name) = cell.get("tbl_name")? else {
                    panic!("expected text");
                };

                if name != "sqlite_sequence" {
                    print!("{name} ");
                }
            }

            println!()
        }
        ".schema" => {
            let table = db.root()?;
            let mut iter = table.rows();

            while let Some(table) = iter.next() {
                let Value::Text(name) = table.get("name")? else {
                    panic!("expected text");
                };

                if name == "sqlite_sequence" || name.starts_with("sqlite_autoindex") {
                    continue;
                }

                println!("schema {name:?}");

                let Value::Text(sql) = table.get("sql")? else {
                    panic!("expected sql string");
                };

                match Command::parse(&sql)? {
                    Command::CreateTable { schema, .. } => {
                        for (name, r#type) in schema.0.iter() {
                            println!("{name:?}: {:?}", r#type);
                        }
                    }
                    Command::CreateIndex {
                        on: On { table, columns },
                        ..
                    } => {
                        let mut cols = String::new();

                        for (i, col) in columns.iter().enumerate() {
                            if i != 0 {
                                cols.push_str(", ");
                            }
                            cols += col;
                        }

                        println!("{table}({cols})");
                    }
                    _ => bail!("unepxected statements"),
                }
                println!();
            }
        }
        command => {
            let command = Command::parse(command)?;
            println!("command: {command:?}");
            db.execute(command)?;
        }
    };
    Ok(())
}
