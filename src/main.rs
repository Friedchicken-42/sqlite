use anyhow::Result;
use sqlite_trait::Sqlite;

fn main() -> Result<()> {
    let x = Sqlite::read("sample.db")?;
    Ok(())
}
