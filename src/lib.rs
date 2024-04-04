#![allow(dead_code)]

pub mod command;

use anyhow::{bail, Result};
use command::Command;
use std::{
    borrow::Cow,
    fmt::Display,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use crate::command::On;

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

    (record_sizes, offset)
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Type {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Integer(u32),
    Float(f64),
    Text(Cow<'a, str>),
    Blob(&'a [u8]),
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
pub struct Schema(pub Vec<(String, Type)>);

#[derive(Debug)]
pub struct PageHeader {
    page_type: usize,
    freeblock: usize,
    cells: usize,
    offset: usize,
    frag: usize,
    right_pointer: Option<usize>,
}

impl PageHeader {
    fn read(data: &[u8]) -> Self {
        Self {
            page_type: data[0] as usize,
            freeblock: u16::from_be_bytes([data[1], data[2]]) as usize,
            cells: u16::from_be_bytes([data[3], data[4]]) as usize,
            offset: u16::from_be_bytes([data[5], data[6]]) as usize,
            frag: data[7] as usize,
            right_pointer: if data[0] == 0x05 || data[0] == 0x02 {
                Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize)
            } else {
                None
            },
        }
    }
}

#[derive(Debug)]
pub struct Page {
    pub header: PageHeader,
    pub pointers: Vec<usize>,
    pub data: Vec<u8>,
}

impl Page {
    fn read(data: Vec<u8>, index: usize) -> Self {
        let offset = if index == 1 { 100 } else { 0 };
        let header = PageHeader::read(&data[offset..]);

        let offset = if header.right_pointer.is_some() {
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

        Self {
            header,
            pointers,
            data,
        }
    }
}

#[derive(Debug)]
pub enum Cell<'a> {
    Leaf(LeafCell<'a>),
    Interior(InteriorCell),
    LeafIndex(LeafIndexCell<'a>),
    InteriorIndex(InteriorIndexCell<'a>),
}

impl<'a> Cell<'a> {
    pub fn get(&self, column: &str) -> Result<Value<'a>> {
        let (data, schema, rowid) = match self {
            Cell::Leaf(leaf) => (&leaf.data, &leaf.schema, leaf.rowid),
            Cell::LeafIndex(leaf) => (&leaf.data, &leaf.schema, 0),
            Cell::InteriorIndex(index) => (&index.data, &index.schema, 0),
            _ => bail!("cannot `get` from an interior cell"),
        };

        let Some(index) = schema.0.iter().position(|(name, _)| name == column) else {
            bail!("column: {column} not found")
        };

        let (_, r#type) = &schema.0[index];
        let data = data[index];

        if data.is_empty() && column == "id" {
            return Ok(Value::Integer(rowid as u32));
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

    pub fn all(&self) -> Result<Vec<Value<'a>>> {
        let schema = match self {
            Cell::Leaf(leaf) => &leaf.schema,
            Cell::LeafIndex(leaf) => &leaf.schema,
            _ => bail!("cannot `get` from an interior cell"),
        };
        // TODO: optimize this
        schema
            .0
            .iter()
            .map(|(name, _)| self.get(name))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug)]
pub struct LeafCell<'a> {
    data: Vec<&'a [u8]>,
    rowid: u64,
    schema: &'a Schema,
}

impl<'a> LeafCell<'a> {
    fn read(data: &'a [u8], schema: &'a Schema) -> Self {
        let mut offset = 0;
        let (payload_bytes, size) = read_varint(&data[offset..]);
        offset += size;

        let (rowid, size) = read_varint(&data[offset..]);
        offset += size;

        let (sizes, size) = parse_bytearray(&data[offset..]);
        let mut values_offset = offset + size;

        let mut records = Vec::with_capacity(sizes.len());
        for size in sizes {
            let value = &data[values_offset..][..size];
            records.push(value);
            values_offset += size;
        }

        assert!(values_offset - offset == payload_bytes as usize);

        Self {
            data: records,
            rowid,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct InteriorCell {
    page: u32,
    key: u64,
}

impl InteriorCell {
    fn read(data: &[u8]) -> Self {
        let page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let (key, _) = read_varint(&data[4..]);

        Self { page, key }
    }
}

#[derive(Debug)]
pub struct LeafIndexCell<'a> {
    data: Vec<&'a [u8]>,
    schema: &'a Schema,
}

impl<'a> LeafIndexCell<'a> {
    fn read(data: &'a [u8], schema: &'a Schema) -> Self {
        let mut offset = 0;
        let (payload_bytes, size) = read_varint(&data[offset..]);
        offset += size;

        let (sizes, size) = parse_bytearray(&data[offset..]);
        let mut values_offset = offset + size;

        let mut records = Vec::with_capacity(sizes.len());
        for size in sizes {
            let value = &data[values_offset..][..size];
            records.push(value);
            values_offset += size;
        }

        assert!(values_offset - offset == payload_bytes as usize);

        Self {
            data: records,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct InteriorIndexCell<'a> {
    page: u32,
    data: Vec<&'a [u8]>,
    schema: &'a Schema,
}

impl<'a> InteriorIndexCell<'a> {
    fn read(data: &'a [u8], schema: &'a Schema) -> Self {
        let page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        let mut offset = 4;
        let (payload_bytes, size) = read_varint(&data[offset..]);
        offset += size;

        let (sizes, size) = parse_bytearray(&data[offset..]);
        let mut values_offset = offset + size;

        let mut records = Vec::with_capacity(sizes.len());
        for size in sizes {
            let value = &data[values_offset..][..size];
            records.push(value);
            values_offset += size;
        }

        assert!(values_offset - offset == payload_bytes as usize);
        // TODO: overflow page: u32

        Self {
            page,
            data: records,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct BTreePage<'a> {
    pub pages: Vec<Page>,
    pub schema: Schema,
    db: &'a Sqlite,
}

impl<'a> BTreePage<'a> {
    fn read(db: &'a Sqlite, index: usize, schema: Schema) -> Result<Self> {
        let first_page = db.page(index)?;

        Ok(Self {
            pages: vec![first_page],
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

    pub fn rows(self) -> Rows<'a> {
        Rows {
            btreepage: self,
            indexes: vec![0],
        }
    }
}

#[derive(Debug)]
pub struct Rows<'a> {
    btreepage: BTreePage<'a>,
    indexes: Vec<usize>,
}

impl<'a> Rows<'a> {
    pub fn filter(self, filters: &'a [&str], values: &'a [&Value]) -> Result<FilteredRows<'a>> {
        let db = self.btreepage.db;
        let root = db.root()?;
        let mut iter = root.rows();

        while let Some(index) = iter.next() {
            let Value::Text(tbl_name) = index.get("name")? else {
                panic!("expected \"tbl_name\" to be \"Text\"");
            };

            if tbl_name == "sqlite_sequence" {
                continue;
            }

            let Value::Text(sql) = index.get("sql")? else {
                bail!("expected sql string");
            };

            if let Command::CreateIndex {
                on: On { table, columns },
                ..
            } = Command::parse(&sql)?
            {
                // TODO: add table name check
                let mut count = 0;
                for col in filters {
                    for column in &columns {
                        if *col == column {
                            count += 1;
                        }
                    }
                }

                if count != filters.len() {
                    continue;
                }

                let btreeindex = db.table(&tbl_name)?;

                return Ok(FilteredRows {
                    btreepage: self.btreepage,
                    indexes: self.indexes,
                    btreeindex: btreeindex.rows(),
                    filters,
                    values,
                });
            };
        }

        bail!("no index found");
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Cell<'_>> {
        loop {
            if self.btreepage.pages.is_empty() {
                return None;
            }
            let page = self.btreepage.pages.last().unwrap();
            let index = self.indexes.last_mut().unwrap();

            match page.header.page_type {
                page_type @ (0x0a | 0x0d) => {
                    if *index >= page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else {
                        let page = self.btreepage.pages.last().unwrap();

                        let offset = page.pointers[*index];

                        *index += 1;

                        let cell = match page_type {
                            0x0a => {
                                let cell = LeafIndexCell::read(
                                    &page.data[offset..],
                                    &self.btreepage.schema,
                                );
                                Cell::LeafIndex(cell)
                            }
                            0x0d => {
                                let cell =
                                    LeafCell::read(&page.data[offset..], &self.btreepage.schema);
                                Cell::Leaf(cell)
                            }
                            _ => unreachable!(),
                        };

                        return Some(cell);
                    }
                }
                page_type @ (0x02 | 0x05) => {
                    if *index > page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else {
                        let page = if *index == page.header.cells {
                            page.header.right_pointer.unwrap()
                        } else {
                            let offset = page.pointers[*index];

                            match page_type {
                                0x02 => {
                                    let cell = InteriorIndexCell::read(
                                        &page.data[offset..],
                                        &self.btreepage.schema,
                                    );
                                    cell.page as usize
                                }
                                0x05 => {
                                    let cell = InteriorCell::read(&page.data[offset..]);
                                    cell.page as usize
                                }
                                _ => unreachable!(),
                            }
                        };

                        let next_page = self.btreepage.db.page(page).unwrap();

                        *index += 1;
                        self.indexes.push(0);
                        self.btreepage.pages.push(next_page);
                    }
                }
                p => panic!("unhandled page type: {p:?}"),
            }
        }
    }

    pub fn search(&mut self, filters: &'a [&str], values: &[&Value]) -> Option<usize> {
        loop {
            if self.btreepage.pages.is_empty() {
                return None;
            }
            let page = self.btreepage.pages.last().unwrap();
            let index = self.indexes.last_mut().unwrap();

            match page.header.page_type {
                0x0a => {
                    if *index >= page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else {
                        let offset = page.pointers[*index];

                        *index += 1;

                        let cell =
                            LeafIndexCell::read(&page.data[offset..], &self.btreepage.schema);
                        let cell = Cell::LeafIndex(cell);

                        let value = cell.get(filters[0]).unwrap(); // TODO: multiple filters

                        let cmp = match (value, values[0]) {
                            (Value::Text(a), Value::Text(b)) => a.cmp(b),
                            _ => todo!(),
                        };

                        if cmp.is_gt() {
                            return None;
                        } else if cmp.is_eq() {
                            let Value::Blob(blob) = cell.get("index").unwrap() else {
                                panic!("expected index");
                            };

                            let rowid = blob.iter().fold(0, |acc, x| (acc << 8) + (*x as usize));

                            return Some(rowid);
                        }
                    }
                }
                0x02 => {
                    #[allow(clippy::comparison_chain)]
                    if *index > page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else if *index == page.header.cells {
                        let page = page.header.right_pointer.unwrap();
                        let next_page = self.btreepage.db.page(page).unwrap();
                        *index += 1;
                        self.btreepage.pages.push(next_page);
                        self.indexes.push(0);
                    } else {
                        let offset = page.pointers[*index];

                        *index += 1;

                        let cell =
                            InteriorIndexCell::read(&page.data[offset..], &self.btreepage.schema);

                        let page = cell.page;

                        let cell = Cell::InteriorIndex(cell);

                        let value = cell.get(filters[0]).unwrap(); // TODO: multiple filters

                        let cmp = match (value, values[0]) {
                            (Value::Text(a), Value::Text(b)) => a.cmp(b),
                            _ => todo!(),
                        };

                        if cmp.is_ge() {
                            let next_page = self.btreepage.db.page(page as usize).unwrap();
                            self.btreepage.pages.push(next_page);
                            self.indexes.push(0);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

pub struct FilteredRows<'a> {
    btreepage: BTreePage<'a>,
    indexes: Vec<usize>,
    btreeindex: Rows<'a>,
    filters: &'a [&'a str],
    values: &'a [&'a Value<'a>],
}

impl<'a> FilteredRows<'a> {
    fn next_by_rowid(&mut self, rowid: usize) -> Option<Cell<'_>> {
        loop {
            if self.btreepage.pages.is_empty() {
                return None;
            }

            let page = self.btreepage.pages.last().unwrap();
            let index = self.indexes.last_mut().unwrap();

            match page.header.page_type {
                0x0d => {
                    if *index >= page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else {
                        let offset = page.pointers[*index];

                        *index += 1;

                        let cell = LeafCell::read(&page.data[offset..], &self.btreepage.schema);

                        if cell.rowid == rowid as u64 {
                            let cell = Cell::Leaf(cell);
                            return Some(cell);
                        }
                    }
                }
                0x05 =>
                {
                    #[allow(clippy::comparison_chain)]
                    if *index > page.header.cells {
                        self.indexes.pop();
                        self.btreepage.pages.pop();
                    } else if *index == page.header.cells {
                        let page = page.header.right_pointer.unwrap();
                        let next_page = self.btreepage.db.page(page).unwrap();
                        *index += 1;
                        self.btreepage.pages.push(next_page);
                        self.indexes.push(0);
                    } else {
                        let offset = page.pointers[*index];

                        *index += 1;

                        let cell = InteriorCell::read(&page.data[offset..]);

                        if rowid <= cell.key as usize {
                            let next_page = self.btreepage.db.page(cell.page as usize).unwrap();
                            self.btreepage.pages.push(next_page);
                            self.indexes.push(0);
                        }
                    }
                }
                _ => unreachable!(),
            };
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Cell<'_>> {
        let rowid = self.btreeindex.search(self.filters, self.values)?;

        self.next_by_rowid(rowid)
    }
}

#[derive(Debug)]
pub struct Header {
    pub page_size: usize,
}

impl Header {
    fn read(data: &[u8]) -> Result<Self> {
        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        Ok(Self { page_size })
    }
}

#[derive(Debug)]
pub struct Sqlite {
    file: File,
    pub header: Header,
}

impl Sqlite {
    pub fn read(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut buf = vec![0; 100];
        (&file).read_exact(&mut buf)?;
        let header = Header::read(&buf)?;

        Ok(Self { file, header })
    }

    pub fn page(&self, index: usize) -> Result<Page> {
        let offset = (index - 1) * self.header.page_size;

        (&self.file).seek(SeekFrom::Start(offset as u64))?;
        let mut data = vec![0; self.header.page_size];
        (&self.file).read_exact(&mut data)?;

        Ok(Page::read(data, index))
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

    pub fn table(&self, name: &str) -> Result<BTreePage> {
        let table = self.root()?;
        let mut iter = table.rows();

        while let Some(cell) = iter.next() {
            let Value::Text(tbl_name) = cell.get("name")? else {
                panic!("expected \"tbl_name\" to be \"Text\"");
            };

            if name != tbl_name {
                continue;
            }

            let Value::Integer(rootpage) = cell.get("rootpage")? else {
                bail!("expected integer");
            };

            let Value::Text(sql) = cell.get("sql")? else {
                bail!("expected sql string");
            };

            return match Command::parse(&sql)? {
                Command::CreateTable { schema, .. } => {
                    BTreePage::read(self, rootpage as usize, schema)
                }
                Command::CreateIndex {
                    on: On { table, columns },
                    ..
                } => {
                    // Each entry in the index b-tree corresponds to a single row in the associated SQL table.
                    // The key to an index b-tree is a record composed of the columns that are being indexed followed by the key of the corresponding table row.
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
                }
                _ => bail!("wrong sql command"),
            };
        }

        bail!("column {name:?} not found")
    }

    pub fn execute(&self, command: Command) -> Result<()> {
        // TODO: should return values here?
        match command {
            Command::Select {
                select,
                from,
                r#where,
            } => {
                let table = self.table(&from.table)?;
                let mut iter = table.rows();

                while let Some(cell) = iter.next() {
                    if r#where.as_ref().is_some_and(|w| !w.r#match(&cell)) {
                        continue;
                    }

                    let rows = select
                        .columns
                        .iter()
                        .flat_map(|name| {
                            if name == "*" {
                                cell.all().unwrap()
                            } else {
                                // TODO: error out if name does not exists
                                vec![cell.get(name).unwrap()]
                            }
                        })
                        .collect::<Vec<_>>();

                    for (i, row) in rows.iter().enumerate() {
                        if i != 0 {
                            print!("|")
                        }
                        print!("{row}");
                    }
                    println!();
                }
            }
            _ => bail!("cannot execute this command now"),
        }
        Ok(())
    }
}
