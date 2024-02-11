use anyhow::{bail, Result};

fn read_varint(data: &[u8]) -> (u64, &[u8], usize) {
    let mut i = 0;
    let mut number = 0u64;
    loop {
        let byte = data[i];

        if byte & 0b10000000 == 0 || i == 9 {
            break;
        }

        number = number << 7 | (byte & 0b01111111) as u64;

        i += 1;
    }

    if i != 9 {
        number = number << 7 | data[i] as u64;
    }

    return (number, &data[i + 1..], i + 1);
}

fn parse_bytearray(data: &[u8]) -> (u64, Vec<u64>) {
    let (header_bytes, data, size) = read_varint(data);

    let mut record_sizes = vec![];

    let mut data = data;
    let mut length = header_bytes - size as u64;
    while length > 0 {
        let (value, d, size) = read_varint(data);
        data = d;
        length -= size as u64;
        let colunm_size = match value {
            0 | 8 | 9 | 12 | 13 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            n if n >= 12 && n % 2 == 0 => (n - 12) / 2,
            n if n >= 13 && n % 2 == 1 => (n - 13) / 2,
            _ => unreachable!(),
        };
        record_sizes.push(colunm_size);
    }

    (header_bytes, record_sizes)
}

fn read_btree_page(data: &[u8]) -> &[u8] {
    let (payload_bytes, data, _) = read_varint(&data);
    let (_rowid, data, _) = read_varint(&data);
    let bytearray = &data[..payload_bytes as usize];

    let (offset, sizes) = parse_bytearray(bytearray);
    let columns = &bytearray[offset as usize..];
    let name = &columns[sizes[0] as usize..][..sizes[1] as usize];
    let name = std::str::from_utf8(name).unwrap();

    if name != "sqlite_sequence" {
        print!("{name} ");
    }
    // TODO: return this

    &data[payload_bytes as usize..]
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let data = std::fs::read(&args[1])?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let header = &data[0..100];

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);
            println!("database page size: {}", page_size);

            let schema_header = &data[100..108];
            let pages = u16::from_be_bytes([schema_header[3], schema_header[4]]);
            println!("number of tables: {}", pages);
        }
        ".tables" => {
            let schema_header = &data[100..108];
            // format:
            // type:      0]
            // freeblock: 1, 2
            // cells:     3, 4
            // offset:    5, 6
            // frag:      7
            let offset = u16::from_be_bytes([schema_header[5], schema_header[6]]);

            let cell = &data[offset as usize..];
            let cell = read_btree_page(cell);
            let cell = read_btree_page(cell);
            let _cell = read_btree_page(cell);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
