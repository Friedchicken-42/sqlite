use anyhow::{bail, Result};

fn read_varint(data: &[u8]) -> (u64, usize) {
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

    (number, i + 1)
}

fn parse_bytearray(data: &[u8]) -> (Vec<usize>, usize) {
    let (header_bytes, size) = read_varint(data);

    if header_bytes == 0 {
        return (vec![], 0);
    }

    let mut offset = size;
    let mut record_sizes = vec![];

    let mut length = header_bytes - size as u64;
    while length > 0 {
        let (value, size) = read_varint(&data[offset..]);
        offset += size;
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
        record_sizes.push(colunm_size as usize);
    }

    (record_sizes, header_bytes as usize)
}

#[derive(Debug)]
struct BTreeHeader {
    page_type: usize,
    freeblock: usize,
    cells: usize,
    offset: usize,
    frag: usize,
}

impl BTreeHeader {
    fn read(data: &[u8]) -> Self {
        Self {
            page_type: data[0] as usize,
            freeblock: u16::from_be_bytes([data[1], data[2]]) as usize,
            cells: u16::from_be_bytes([data[3], data[4]]) as usize,
            offset: u16::from_be_bytes([data[5], data[6]]) as usize,
            frag: data[7] as usize,
        }
    }
}

#[derive(Debug)]
struct Cell {
    name: String,
    rootpage: usize,
}

#[derive(Debug)]
struct BTreePage {
    header: BTreeHeader,
    cells: Vec<Cell>,
}

impl BTreePage {
    fn read(data: &[u8], offset: usize) -> Self {
        let header_offset = if offset == 0 { 100 } else { offset };
        let header = BTreeHeader::read(&data[header_offset..]);

        let mut offset = header.offset + offset;
        let mut cells = Vec::with_capacity(header.cells);

        for _ in 0..header.cells {
            let (payload_bytes, size) = read_varint(&data[offset..]);
            offset += size;

            let (_rowid, size) = read_varint(&data[offset..]);
            offset += size;

            let (sizes, size) = parse_bytearray(&data[offset..]);

            let columns = offset + size;

            let name = &data[columns + sizes[0]..][..sizes[1]];
            // TODO: use slice
            let name = String::from_utf8(name.to_vec()).unwrap();

            let offset_rootpage: usize = sizes[0..3].iter().sum();
            let rootpage = data[columns + offset_rootpage] as usize;

            offset += payload_bytes as usize;

            cells.push(Cell { name, rootpage });
        }
        Self { header, cells }
    }
}

struct Header {
    page_size: usize,
}

impl Header {
    fn read(data: &[u8]) -> Self {
        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;
        Self { page_size }
    }
}

struct Sqlite {
    data: Vec<u8>,
    header: Header,
}

impl Sqlite {
    fn read(file: &str) -> Result<Self> {
        let data = std::fs::read(file)?;
        let header = Header::read(&data);
        Ok(Self { data, header })
    }

    fn btreepage(&self, index: usize) -> BTreePage {
        let offset = (index - 1) * self.header.page_size;
        BTreePage::read(&self.data, offset)
    }
}
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

            let page = db.btreepage(1);
            println!("number of tables: {}", page.header.cells);
        }
        ".tables" => {
            let page = db.btreepage(1);

            let tables = page.cells.iter().filter(|p| p.name != "sqlite_sequence");

            for table in tables {
                print!("{} ", table.name);
            }
        }
        command => {
            let commands: Vec<_> = command.split(" ").collect();
            let table = commands[3];

            let page = db.btreepage(1);
            let Some(cell) = page.cells.iter().find(|p| p.name == table) else {
                panic!("no table named \"{table}\"");
            };

            let page = db.btreepage(cell.rootpage);
            println!("{}", page.header.cells);

            // let name = commands[1];
            // let table = commands[3];

            // let header = Header::new(&data);
            // let schema_header = BTreeHeader::new(&data[100..]);

            // let mut cell = &data[schema_header.offset as usize..];
            // for i in 0..schema_header.cells {
            //     let (page, c) = read_btree_page(cell);
            //     cell = c;
            //     if page.name == table {
            //         println!("{table} {i} {page:?}");
            //         let index = (page.rootpage - 1) as usize * header.page_size as usize;
            //         let page_header = BTreeHeader::new(&data[index..]);
            //         println!("{page_header:?} {:#x}", page_header.offset as usize + index);

            //         let mut cell = &data[page_header.offset as usize + index..];
            //         let (page, c) = read_btree_page(cell);
            //     }
            // }
        }
    }

    Ok(())
}
