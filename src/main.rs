use sqlite::{
    Result, Sqlite,
    display::{DisplayOptions, Printable},
    parser::Query,
};

fn database(args: &[String]) -> Result<()> {
    let db = Sqlite::read(&args[1])?;

    match args[2].as_str() {
        ".dbinfo" => {
            println!("database page size: {}", db.page_size());

            let mut page = db.root()?;
            println!("number of tables: {}", page.count());
        }
        ".tables" => db.show_tables()?,
        ".schema" => db.show_schema()?,
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
        }
        input => {
            let query = Query::parse(input)?;
            let mut table = db.execute(query)?;
            println!("{table:#?}");

            let options = DisplayOptions::r#box();
            table.display(options);
        }
    }

    Ok(())
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => panic!("Missing <database path> and <command>"),
        2 => panic!("Missing <command>"),
        _ => {}
    }

    if let Err(e) = database(&args) {
        println!("{e:?}");
    }
}
