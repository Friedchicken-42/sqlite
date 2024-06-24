mod command;

use anyhow::{bail, Result};
use command::{Command, CreateIndex, CreateTable};
use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::{Debug, Display},
    fs::File,
    io::{Read, Seek, SeekFrom},
};
use tracing::{event, span, Level};

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

fn serial_size(serial: u64) -> u64 {
    match serial {
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
    }
}

fn parse_bytearray(data: &[u8]) -> (Vec<u64>, usize) {
    let (header_bytes, size) = read_varint(data);

    if header_bytes == 0 {
        return (vec![], 0);
    }

    let mut offset = size;
    let mut serials = vec![];

    let mut length = header_bytes - size as u64;

    while length > 0 {
        let (serial, size) = read_varint(&data[offset..]);
        offset += size;
        length -= size as u64;

        serials.push(serial);
    }

    (serials, offset)
}

fn bytearray_values(data: &[u8]) -> (Vec<&[u8]>, usize) {
    let (serials, size) = parse_bytearray(data);
    let mut offset = size;

    let mut records = Vec::with_capacity(serials.len());

    for serial in serials {
        let size = serial_size(serial) as usize;

        let value = match serial {
            8 => &[0],
            9 => &[1],
            _ => &data[offset..][..size],
        };

        records.push(value);
        offset += size;
    }

    (records, offset)
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Type {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value<'a> {
    Null,
    Integer(u32),
    Float(f64),
    Text(Cow<'a, str>),
    Blob(&'a [u8]),
}

impl<'a> Eq for Value<'a> {}

impl<'a> PartialOrd for Value<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Value<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Value::Integer(_) | Value::Float(_), _) => Ordering::Less,
            (_, Value::Integer(_) | Value::Float(_)) => Ordering::Greater,
            (Value::Text(_), _) => Ordering::Less,
            (_, Value::Text(_)) => Ordering::Greater,
        }
    }
}

impl<'a> Display for Value<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, ""),
            Value::Integer(value) => write!(f, "{value}"),
            Value::Float(value) => write!(f, "{value}"),
            Value::Text(str) => write!(f, "{str}"),
            Value::Blob(bin) => write!(f, "{bin:0x?}"),
        }
    }
}

#[derive(Debug)]
struct Schema(Vec<(String, Type)>);

#[derive(Debug, PartialEq, Clone, Copy)]
enum PageType {
    TableLeaf,
    IndexLeaf,
    TableInterior,
    IndexInterior,
}

#[derive(Debug)]
pub struct PageHeader {
    freeblock: usize,
    cells: usize,
    offset: usize,
    frag: usize,
}

impl PageHeader {
    fn read(data: &[u8]) -> Self {
        Self {
            freeblock: u16::from_be_bytes([data[1], data[2]]) as usize,
            cells: u16::from_be_bytes([data[3], data[4]]) as usize,
            offset: u16::from_be_bytes([data[5], data[6]]) as usize,
            frag: data[7] as usize,
        }
    }
}

#[derive(Debug)]
struct Page {
    r#type: PageType,
    header: PageHeader,
    pointers: Vec<usize>,
    data: Vec<u8>,
}

impl Page {
    fn read(data: Vec<u8>, index: usize) -> Result<Self> {
        let offset = if index == 1 { 100 } else { 0 };

        let header = PageHeader::read(&data[offset..]);

        let r#type = match data[offset] {
            0x0d => PageType::TableLeaf,
            0x0a => PageType::IndexLeaf,
            0x05 => PageType::TableInterior,
            0x02 => PageType::IndexInterior,
            p => bail!("wrong page_type: {p:?}"),
        };

        let offset = if r#type == PageType::TableInterior || r#type == PageType::IndexInterior {
            offset + 12
        } else {
            offset + 8
        };

        let pointers = (0..header.cells)
            .map(|i| {
                let off = offset + i * 2;
                u16::from_be_bytes([data[off], data[off + 1]]) as usize
            })
            .collect();

        Ok(Self {
            r#type,
            header,
            pointers,
            data,
        })
    }

    fn cell<'a>(&'a self, offset: usize, schema: &'a Schema) -> Cell<'a> {
        let data = &self.data[offset..];

        let mut offset = match self.r#type {
            PageType::TableLeaf | PageType::IndexLeaf => 0,
            PageType::TableInterior | PageType::IndexInterior => 4,
        };

        let (payload_bytes, size) = read_varint(&data[offset..]);
        offset += size;

        if self.r#type == PageType::TableLeaf {
            let (_, size) = read_varint(&data[offset..]);
            offset += size;
        }

        let (records, _) = if self.r#type != PageType::TableInterior {
            bytearray_values(&data[offset..])
        } else {
            (vec![], 0)
        };

        Cell {
            r#type: self.r#type,
            data,
            records,
            schema,
        }
    }

    fn right_pointer(&self) -> Result<usize> {
        match self.r#type {
            PageType::TableInterior | PageType::IndexInterior => {
                let pointer =
                    u32::from_be_bytes([self.data[8], self.data[9], self.data[10], self.data[11]])
                        as usize;
                Ok(pointer)
            }
            _ => bail!("right pointer is present only in interior page"),
        }
    }
}

pub trait Rows: Debug {
    fn next(&mut self) -> Option<impl Row>;
}

pub trait Row<'a>: Debug {
    fn get(&self, column: &str) -> Result<Value<'a>>;
}

#[derive(Debug)]
pub struct BTreePage<'a> {
    pages: Vec<Page>,
    indexes: Vec<usize>,
    schema: Schema,
    db: &'a Sqlite,
}

impl<'a> BTreePage<'a> {
    fn read(db: &'a Sqlite, index: usize, schema: Schema) -> Result<Self> {
        let first_page = db.page(index)?;

        Ok(Self {
            pages: vec![first_page],
            indexes: vec![0],
            schema,
            db,
        })
    }

    pub fn count(&self) -> usize {
        self.pages
            .first()
            .expect("Must be present at least one page")
            .header
            .cells
    }

    fn loop_until<F>(&mut self, filter: Option<F>) -> Option<Cell<'_>>
    where
        F: Fn(&Cell) -> Ordering,
    {
        loop {
            if self.pages.is_empty() {
                return None;
            }

            let page = self.pages.last().unwrap();
            let index = self.indexes.last_mut().unwrap();

            match &page.r#type {
                page_type @ (PageType::TableLeaf | PageType::IndexLeaf) => {
                    if *index >= page.header.cells {
                        self.pages.pop();
                        self.indexes.pop();
                    } else {
                        let pointers = &page.pointers;
                        let offset = pointers[*index];

                        *index += 1;

                        match (page_type, &filter) {
                            (_, None) => {
                                return Some(page.cell(offset, &self.schema));
                            }
                            (PageType::TableLeaf, Some(f)) => {
                                let cell = page.cell(offset, &self.schema);
                                if f(&cell).is_eq() {
                                    return Some(cell);
                                }
                            }
                            (PageType::IndexLeaf, Some(f)) => {
                                let cell = page.cell(offset, &self.schema);
                                match f(&cell) {
                                    Ordering::Less => {}
                                    Ordering::Equal => return Some(cell),
                                    Ordering::Greater => return None,
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                page_type @ (PageType::TableInterior | PageType::IndexInterior) => {
                    if *index > page.header.cells * 2 {
                        self.pages.pop();
                        self.indexes.pop();
                    } else if *index == page.header.cells * 2 {
                        *index += 1;

                        // TODO: find a way to return errors from here decently
                        let right_pointer = page.right_pointer().unwrap();

                        let next_page = self.db.page(right_pointer).unwrap();

                        self.pages.push(next_page);
                        self.indexes.push(0);
                    } else if *index % 2 == 1 {
                        match page_type {
                            PageType::TableInterior => *index += 1,
                            PageType::IndexInterior => {
                                let pointers = &page.pointers;
                                let offset = pointers[(*index - 1) / 2];

                                *index += 1;

                                let cell = page.cell(offset, &self.schema);

                                if let Some(f) = &filter {
                                    if f(&cell).is_ge() {
                                        return Some(cell);
                                    }
                                } else {
                                    return Some(cell);
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        let pointers = &page.pointers;
                        let offset = pointers[*index / 2];

                        *index += 1;

                        let next_page = match &filter {
                            None => {
                                let cell = page.cell(offset, &self.schema);
                                let page = cell.page().unwrap();
                                Some(page as usize)
                            }
                            Some(f) => {
                                let cell = page.cell(offset, &self.schema);

                                if f(&cell).is_ge() {
                                    let page = cell.page().unwrap();
                                    Some(page as usize)
                                } else {
                                    None
                                }
                            }
                        };

                        if let Some(page) = next_page {
                            let next_page = self.db.page(page).unwrap();
                            self.pages.push(next_page);
                            self.indexes.push(0);
                        }
                    }
                }
            }
        }
    }

    fn next_by_rowid(&mut self, rowid: usize) -> Option<Cell<'_>> {
        let filter = move |cell: &Cell| {
            let row = cell.rowid().unwrap() as usize;
            row.cmp(&rowid)
        };

        self.loop_until(Some(filter))
    }
}

impl<'a> Rows for BTreePage<'a> {
    fn next(&mut self) -> Option<impl Row> {
        // Type system hack, TODO: find a better way
        let filter = None.map(|_: ()| |_cell: &Cell| Ordering::Equal);
        self.loop_until(filter)
    }
}

#[derive(Debug)]
pub struct BTreeIndex<'a> {
    btreepage: BTreePage<'a>,
    filters: Vec<(Cow<'a, str>, Value<'a>)>,
}

impl<'a> BTreeIndex<'a> {
    fn next(&mut self) -> Option<usize> {
        let filters = &self.filters;

        let filter = if self.filters.is_empty() {
            None
        } else {
            Some(move |cell: &Cell| {
                for (key, value) in filters {
                    let res = cell.get(key).unwrap();

                    let cmp = res.cmp(value);

                    if cmp.is_ne() {
                        return cmp;
                    }
                }

                Ordering::Equal
            })
        };

        let cell = self.btreepage.loop_until(filter)?;

        let Value::Blob(blob) = cell.get("index").unwrap() else {
            panic!("wrong value");
        };

        let rowid = blob.iter().fold(0, |acc, x| (acc << 8) + (*x as usize));

        Some(rowid)
    }
}

#[derive(Debug)]
pub struct IndexedRows<'a> {
    btreepage: BTreePage<'a>,
    btreeindex: BTreeIndex<'a>,
}

impl<'a> Rows for IndexedRows<'a> {
    fn next(&mut self) -> Option<impl Row> {
        let rowid = self.btreeindex.next()?;
        self.btreepage.next_by_rowid(rowid)
    }
}

#[derive(Debug)]
pub struct Cell<'a> {
    r#type: PageType,
    data: &'a [u8],
    records: Vec<&'a [u8]>,
    schema: &'a Schema,
}

impl<'a> Cell<'a> {
    fn rowid(&self) -> Result<u32> {
        match self.r#type {
            PageType::TableLeaf => {
                let (_, size) = read_varint(self.data);
                let (rowid, _) = read_varint(&self.data[size..]);

                Ok(rowid as u32)
            }
            PageType::TableInterior => {
                let (key, _) = read_varint(&self.data[4..]);
                Ok(key as u32)
            }
            _ => bail!("wrong page type"),
        }
    }

    fn page(&self) -> Result<u32> {
        match self.r#type {
            PageType::TableInterior | PageType::IndexInterior => Ok(u32::from_be_bytes([
                self.data[0],
                self.data[1],
                self.data[2],
                self.data[3],
            ])),
            _ => bail!("wrong page type"),
        }
    }
}

impl<'a> Row<'a> for Cell<'a> {
    fn get(&self, column: &str) -> Result<Value<'a>> {
        let Some(index) = self.schema.0.iter().position(|(name, _)| name == column) else {
            bail!("column: {column:?} not found")
        };

        let (_, r#type) = self.schema.0[index];
        let data = self.records[index];

        if data.is_empty() && column == "id" {
            let rowid = self.rowid()?;
            return Ok(Value::Integer(rowid));
        } else if data.is_empty() {
            return Ok(Value::Null);
        }

        let value = match r#type {
            Type::Null => Value::Null,
            Type::Integer => Value::Integer(data[0] as u32),
            Type::Float => todo!(),
            Type::Text => Value::Text(Cow::Borrowed(std::str::from_utf8(data)?)),
            Type::Blob => Value::Blob(data),
        };

        Ok(value)
    }
}

#[derive(Debug)]
pub struct Header {
    pub reserved: u8,
    pub page_size: usize,
}

impl Header {
    fn read(data: &[u8]) -> Result<Self> {
        let reserved = u8::from_be_bytes([data[20]]);
        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        Ok(Self {
            reserved,
            page_size,
        })
    }
}

#[derive(Debug)]
pub struct Sqlite {
    file: File,
    header: Header,
}

impl Sqlite {
    pub fn read(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut buf = vec![0; 100];
        (&file).read_exact(&mut buf)?;
        let header = Header::read(&buf)?;

        Ok(Self { file, header })
    }

    pub fn page_size(&self) -> usize {
        self.header.page_size
    }

    pub fn root(&self) -> Result<BTreePage> {
        let schema = Schema(vec![
            ("type".into(), Type::Text),
            ("name".into(), Type::Text),
            ("tbl_name".into(), Type::Text),
            ("rootpage".into(), Type::Integer),
            ("sql".into(), Type::Text),
        ]);

        BTreePage::read(self, 1, schema)
    }

    fn page(&self, index: usize) -> Result<Page> {
        let offset = (index - 1) * self.header.page_size;

        (&self.file).seek(SeekFrom::Start(offset as u64))?;
        let mut data = vec![0; self.header.page_size];
        (&self.file).read_exact(&mut data)?;

        let page = Page::read(data, index)?;

        let span = span!(Level::INFO, "Page Read");
        let _enter = span.enter();

        event!(
            Level::INFO,
            index = index,
            r#type = tracing::field::debug(&page.r#type),
        );

        Ok(page)
    }

    fn find(&self, name: &str) -> Result<(u32, Command)> {
        let span = span!(Level::INFO, "Page Search", name = name);
        let _enter = span.enter();

        let mut root = self.root()?;

        while let Some(cell) = root.next() {
            let Value::Text(tbl_name) = cell.get("name")? else {
                bail!("Expected Text")
            };

            if name != tbl_name {
                continue;
            }

            let Value::Integer(rootpage) = cell.get("rootpage")? else {
                bail!("Expected integer")
            };

            let Value::Text(sql) = cell.get("sql")? else {
                bail!("Expected text")
            };

            let command = Command::parse(&sql.to_lowercase())?;

            return Ok((rootpage, command));
        }

        bail!("table {name:?} not found");
    }

    pub fn table(&self, name: &str) -> Result<BTreePage> {
        let (rootpage, command) = self.find(name)?;

        if let Command::CreateTable(CreateTable { table, schema }) = command {
            BTreePage::read(self, rootpage as usize, schema)
        } else {
            bail!("{name:?} is not a table");
        }
    }

    pub fn index(&self, name: &str) -> Result<BTreePage> {
        let (rootpage, command) = self.find(name)?;

        if let Command::CreateIndex(CreateIndex {
            index,
            table,
            columns,
        }) = command
        {
            let mut schema = vec![];
            let table = self.table(&table)?;

            for column in columns {
                for (name, r#type) in &table.schema.0 {
                    if column == *name {
                        schema.push((column.clone(), *r#type));
                    }
                }
            }

            // why the FUCK is this not a varint (even if it should be)?
            // blob.iter().fold(0, |acc, x| (acc << 8) + x)
            schema.push(("index".into(), Type::Blob));
            let schema = Schema(schema);
            BTreePage::read(self, rootpage as usize, schema)
        } else {
            bail!("table {name:?} not found");
        }
    }

    pub fn show_schema(&self) -> Result<()> {
        let mut table = self.root()?;

        while let Some(table) = table.next() {
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

            match Command::parse(&sql.to_lowercase())? {
                Command::CreateTable(CreateTable { schema, .. }) => {
                    for (name, r#type) in schema.0.iter() {
                        println!("{name:?}: {:?}", r#type);
                    }
                }
                Command::CreateIndex(CreateIndex { table, columns, .. }) => {
                    print!("{table}(");
                    for (i, col) in columns.iter().enumerate() {
                        if i != 0 {
                            print!(", ");
                        }
                        print!("{col}");
                    }

                    println!(")");
                }
                _ => bail!("unexpected statement"),
            }

            println!();
        }
        Ok(())
    }

    pub fn execute(&self, command: Command) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tracing::{span, Level};
    use tracing_test::traced_test;

    use crate::{BTreeIndex, IndexedRows, Row, Rows, Sqlite, Value};

    #[traced_test]
    #[test]
    fn simple_db() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let mut table = db.table("apples")?;

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 0;
        while let Some(cell) = table.next() {
            assert_eq!(cell.get("id")?, Value::Integer(i + 1));
            i += 1;
        }

        assert_eq!(i, 4);

        Ok(())
    }

    #[traced_test]
    #[test]
    fn complex_db() -> Result<()> {
        let db = Sqlite::read("companies.db")?;
        let mut table = db.table("companies")?;

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 0;
        while let Some(cell) = table.next() {
            cell.get("id")?;
            i += 1;
        }

        // TODO: shouldn't be table.count()?
        assert_eq!(i, 55991);

        Ok(())
    }

    #[traced_test]
    #[test]
    fn simple_index() -> Result<()> {
        let db = Sqlite::read("other.db")?;
        let table = db.table("apples")?;
        let index = db.index("apples_idx")?;

        let mut indexed = IndexedRows {
            btreepage: table,
            btreeindex: BTreeIndex {
                btreepage: index,
                filters: vec![("name".into(), Value::Text("Fuji".into()))],
            },
        };

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        while let Some(cell) = indexed.next() {
            cell.get("id")?;
            cell.get("name")?;
        }

        Ok(())
    }

    #[traced_test]
    #[test]
    fn complex_index() -> Result<()> {
        let db = Sqlite::read("companies.db")?;
        let table = db.table("companies")?;
        let index = db.index("idx_companies_country")?;

        let mut indexed = IndexedRows {
            btreepage: table,
            btreeindex: BTreeIndex {
                btreepage: index,
                filters: vec![("country".into(), Value::Text("tuvalu".into()))],
            },
        };

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 0;
        while let Some(cell) = indexed.next() {
            let country = cell.get("country")?;
            assert_eq!(country, Value::Text("tuvalu".into()));
            i += 1;
        }

        assert_eq!(i, 25);

        Ok(())
    }
}
