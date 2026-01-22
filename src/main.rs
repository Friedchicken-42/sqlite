use ariadne::Source;
use clap::Parser;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use sqlite::{
    Access, Result, Sqlite, Tabular,
    display::{DisplayOptions, Printable},
    parser::Query,
};

#[derive(Parser)]
struct Args {
    file: String,
    query: Option<String>,
}

fn execute(db: &Sqlite, query: &str) -> Result<()> {
    match query {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size());

            let mut page = db.root()?;
            println!("number of tables: {}", page.count());
            Ok(())
        }
        ".tables" => db.show_tables(),
        ".schema" => db.show_schema(),
        ".test" => {
            use sqlite::Iterator;

            let query = Query::parse("select * from apples")?;
            let mut table = db.execute(query)?;
            let mut rows = table.rows();

            while let Some(row) = rows.next() {
                println!("{:?}", row.get("id".into()));
            }
            while let Some(row) = rows.next() {
                println!("{:?}", row.get("id".into()));
            }

            Ok(())
        }
        input if input.starts_with(".parse ") => {
            let input = input.trim_start_matches(".parse ");
            let query = Query::parse(input)?;
            println!("{query:#?}");

            Ok(())
        }
        input => {
            let query = Query::parse(input)?;
            let mut table = db.execute(query)?;

            let options = DisplayOptions::r#box();
            table.display(options)
        }
    }
}

fn run(db: &Sqlite, query: &str) {
    if let Err(e) = execute(db, query) {
        println!();
        e.write(("query", Source::from(query)), std::io::stdout());
    }
}

fn repl(db: &Sqlite) {
    let mut rl = DefaultEditor::new().expect("cannot create editor");

    loop {
        let readline = rl.readline("> ");

        match readline {
            Ok(line) if line == ".q" || line == ".quit" => return,
            Ok(line) => {
                rl.add_history_entry(line.as_str())
                    .expect("should be able to add line to history");

                run(db, &line);
                println!();
            }

            Err(ReadlineError::Interrupted | ReadlineError::Eof) => return,
            Err(err) => {
                println!("error: {err:?}");
                return;
            }
        }
    }
}

fn main() {
    let args = Args::parse();

    let db = Sqlite::read(&args.file).unwrap();

    match args.query {
        Some(query) => run(&db, &query),
        None => repl(&db),
    }
}
