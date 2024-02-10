use anyhow::{bail, Result};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let data = std::fs::read(&args[1])?;
            let header = &data[0..100];
            let others = &data[100..];

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("database page size: {}", page_size);

            let pages = u16::from_be_bytes([others[3], others[4]]);
            println!("number of tables: {}", pages);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
