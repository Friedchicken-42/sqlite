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

    if header_bytes == 0 {
        return (0, vec![]);
    }
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

fn read_btree_page(data: &[u8]) -> (BTreePage, &[u8]) {
    let (payload_bytes, data, _) = read_varint(&data);
    let (_rowid, data, _) = read_varint(&data);
    let bytearray = &data[..payload_bytes as usize];

    let (offset, sizes) = parse_bytearray(bytearray);
    let columns = &bytearray[offset as usize..];
    let name = &columns[sizes[0] as usize..][..sizes[1] as usize];
    let name = std::str::from_utf8(name).unwrap();

    let offset_root: u64 = sizes[0..3].iter().sum();
    let rootpage = columns[offset_root as usize];

    let page = BTreePage { name, rootpage };

    (page, &data[payload_bytes as usize..])
}

struct Header {
    page_size: u16,
}

impl Header {
    fn new(data: &[u8]) -> Self {
        Self {
            page_size: u16::from_be_bytes([data[16], data[17]]),
        }
    }
}

#[derive(Debug)]
struct BTreeHeader {
    page_type: u8,
    freeblock: u16,
    cells: u16,
    offset: u16,
    frag: u8,
}

impl BTreeHeader {
    fn new(data: &[u8]) -> Self {
        Self {
            page_type: data[0],
            freeblock: u16::from_be_bytes([data[1], data[2]]),
            cells: u16::from_be_bytes([data[3], data[4]]),
            offset: u16::from_be_bytes([data[5], data[6]]),
            frag: data[7],
        }
    }
}

#[derive(Debug)]
struct BTreePage<'a> {
    name: &'a str,
    rootpage: u8,
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
            let header = Header::new(&data);

            println!("database page size: {}", header.page_size);

            let schema_header = BTreeHeader::new(&data[100..]);
            println!("number of tables: {}", schema_header.cells);
        }
        ".tables" => {
            let schema_header = BTreeHeader::new(&data[100..]);

            let mut cell = &data[schema_header.offset as usize..];
            for _ in 0..schema_header.cells {
                let (page, c) = read_btree_page(cell);
                cell = c;
                if page.name != "sqlite_sequence" {}
            }
            println!();
        }
        command => {
            let table = command.split(" ").last().unwrap();

            let header = Header::new(&data);
            let schema_header = BTreeHeader::new(&data[100..]);

            let mut cell = &data[schema_header.offset as usize..];
            for _ in 0..schema_header.cells {
                let (page, c) = read_btree_page(cell);
                cell = c;
                if page.name == table {
                    let index = (page.rootpage - 1) as usize * header.page_size as usize;
                    let page_header = BTreeHeader::new(&data[index..]);
                    println!("{}", page_header.cells);
                }
            }
        }
    }

    Ok(())
}
