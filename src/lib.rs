#![allow(dead_code)]

pub mod command;

use anyhow::{bail, Result};
use command::Command;
use std::{
    borrow::Cow,
    cmp::Ordering,
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
        }
        if data.is_empty() {
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
            Cell::InteriorIndex(index) => &index.schema,
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

        let (records, values_offset) = bytearray_values(&data[offset..]);

        assert!(values_offset == payload_bytes as usize);

        let u = 4096;
        let x = u - 35;
        assert!(size <= x);

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

        let (records, values_offset) = bytearray_values(&data[offset..]);

        assert!(values_offset == payload_bytes as usize);

        // U = db.header.page_size - db.header.reserved
        let u = 4096;
        let x = ((u - 12) * 64 / 255) - 23;
        assert!(size <= x);

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

        let (records, values_offset) = bytearray_values(&data[offset..]);

        assert!(values_offset == payload_bytes as usize);

        // U = db.header.page_size - db.header.reserved
        let u = 4096;
        let x = ((u - 12) * 64 / 255) - 23;
        assert!(size <= x);
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

    pub fn rows(self) -> Rows<'a> {
        Rows {
            btreepage: self,
            btreeindex: None,
            filters: vec![],
        }
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

            match page.header.page_type {
                page_type @ (0x0a | 0x0d) => {
                    if *index >= page.header.cells {
                        self.pages.pop();
                        self.indexes.pop();
                    } else {
                        let offset = page.pointers[*index];

                        *index += 1;

                        match (page_type, &filter) {
                            (0x0a, None) => {
                                let cell = LeafIndexCell::read(&page.data[offset..], &self.schema);
                                return Some(Cell::LeafIndex(cell));
                            }
                            (0x0d, None) => {
                                let cell = LeafCell::read(&page.data[offset..], &self.schema);
                                return Some(Cell::Leaf(cell));
                            }
                            (0x0a, Some(f)) => {
                                let cell = LeafIndexCell::read(&page.data[offset..], &self.schema);
                                let cell = Cell::LeafIndex(cell);

                                match f(&cell) {
                                    Ordering::Less => {}
                                    Ordering::Equal => return Some(cell),
                                    Ordering::Greater => return None,
                                }
                            }
                            (0x0d, Some(f)) => {
                                let cell = LeafCell::read(&page.data[offset..], &self.schema);
                                let cell = Cell::Leaf(cell);

                                if f(&cell).is_eq() {
                                    return Some(cell);
                                }
                            }
                            _ => unreachable!(),
                        };
                    }
                }
                page_type @ (0x02 | 0x05) => {
                    if *index > page.header.cells * 2 {
                        self.indexes.pop();
                        self.pages.pop();
                    } else if *index == page.header.cells * 2 {
                        *index += 1;

                        let page = page.header.right_pointer.unwrap();
                        let next_page = self.db.page(page).unwrap();

                        self.pages.push(next_page);
                        self.indexes.push(0);
                    } else if *index % 2 == 1 && page_type == 0x02 {
                        let offset = page.pointers[(*index - 1) / 2];

                        *index += 1;

                        let cell = InteriorIndexCell::read(&page.data[offset..], &self.schema);
                        let cell = Cell::InteriorIndex(cell);

                        if let Some(f) = &filter {
                            if f(&cell).is_ge() {
                                return Some(cell);
                            }
                        } else {
                            return Some(cell);
                        }
                    } else if *index % 2 == 1 {
                        *index += 1;
                    } else if *index % 2 == 0 {
                        let offset = page.pointers[*index / 2];

                        *index += 1;

                        let next_page = match (page_type, &filter) {
                            (0x02, None) => {
                                let cell =
                                    InteriorIndexCell::read(&page.data[offset..], &self.schema);
                                Some(cell.page as usize)
                            }
                            (0x05, None) => {
                                let cell = InteriorCell::read(&page.data[offset..]);
                                Some(cell.page as usize)
                            }
                            (0x02, Some(f)) => {
                                let cell =
                                    InteriorIndexCell::read(&page.data[offset..], &self.schema);
                                let page = cell.page as usize;

                                let cell = Cell::InteriorIndex(cell);

                                if f(&cell).is_ge() {
                                    Some(page)
                                } else {
                                    None
                                }
                            }
                            (0x05, Some(f)) => {
                                let cell = InteriorCell::read(&page.data[offset..]);
                                let page = cell.page as usize;

                                let cell = Cell::Interior(cell);

                                if f(&cell).is_ge() {
                                    Some(page)
                                } else {
                                    None
                                }
                            }
                            _ => unreachable!(),
                        };

                        if let Some(page) = next_page {
                            let next_page = self.db.page(page).unwrap();
                            self.pages.push(next_page);
                            self.indexes.push(0);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn search(&mut self, filters: &[(Cow<'a, str>, Value<'a>)]) -> Option<usize> {
        let filter = if filters.is_empty() {
            None
        } else {
            Some(move |cell: &Cell| {
                for (key, value) in filters {
                    let res = cell.get(key).unwrap();

                    let cmp = match (res, value) {
                        (Value::Null, Value::Null) => Ordering::Equal,
                        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
                        (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
                        (Value::Text(a), Value::Text(b)) => a.cmp(b),
                        (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
                        (Value::Null, _) => Ordering::Less,
                        (_, Value::Null) => Ordering::Greater,
                        (Value::Integer(a), Value::Float(b)) => (a as f64).total_cmp(b),
                        (Value::Float(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),
                        (Value::Integer(_) | Value::Float(_), _) => Ordering::Less,
                        (_, Value::Integer(_) | Value::Float(_)) => Ordering::Greater,
                        (Value::Text(_), _) => Ordering::Less,
                        (_, Value::Text(_)) => Ordering::Greater,
                    };

                    if cmp.is_ne() {
                        return cmp;
                    }
                }

                Ordering::Equal
            })
        };

        let cell = self.loop_until(filter)?;

        let Value::Blob(blob) = cell.get("index").unwrap() else {
            panic!("wrong value");
        };

        let rowid = blob.iter().fold(0, |acc, x| (acc << 8) + (*x as usize));

        Some(rowid)
    }

    fn next(&mut self, rowid: Option<usize>) -> Option<Cell<'_>> {
        let filter = rowid.map(|rowid| {
            move |cell: &Cell| {
                let row = match cell {
                    Cell::Leaf(l) => l.rowid,
                    Cell::Interior(i) => i.key,
                    _ => unreachable!(),
                } as usize;

                row.cmp(&rowid)
            }
        });

        self.loop_until(filter)
    }
}

#[derive(Debug)]
pub struct Rows<'a> {
    btreepage: BTreePage<'a>,
    btreeindex: Option<BTreePage<'a>>,
    filters: Vec<(Cow<'a, str>, Value<'a>)>,
}

impl<'a> Rows<'a> {
    fn find_index(&self, filters: &[(Cow<'a, str>, Value<'a>)]) -> Result<Option<BTreePage<'a>>> {
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
                for (col, _) in filters {
                    for column in &columns {
                        // TODO: find nearest
                        if col == column {
                            count += 1;
                        }
                    }
                }

                if count != filters.len() {
                    continue;
                }

                let btreeindex = db.table(&tbl_name)?;
                return Ok(Some(btreeindex));
            };
        }

        Ok(None)
    }

    pub fn filter(self, filters: Vec<(Cow<'a, str>, Value<'a>)>) -> Result<Self> {
        let btreeindex = self.find_index(&filters)?;

        Ok(Self {
            btreeindex,
            filters,
            ..self
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Cell<'_>> {
        if let Some(ref mut btree_page) = self.btreeindex {
            let rowid = btree_page.search(&self.filters)?;
            self.btreepage.next(Some(rowid))
        } else {
            self.btreepage.next(None)
        }
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
                limit,
            } => {
                let mut count = 0;
                let table = self.table(&from.table)?;
                let mut iter = table.rows();

                if let Some(r#where) = &r#where {
                    let filters = r#where.filters();
                    iter = iter.filter(filters)?;
                }

                while let Some(cell) = iter.next() {
                    if limit.as_ref().is_some_and(|l| count >= l.limit) {
                        break;
                    }

                    if let Some(w) = &r#where {
                        if !w.r#match(&cell)? {
                            continue;
                        }
                    };

                    let rows = select
                        .columns
                        .iter()
                        .map(|name| {
                            if name == "*" {
                                cell.all()
                            } else {
                                vec![cell.get(name)].into_iter().collect()
                            }
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();

                    for (i, row) in rows.iter().enumerate() {
                        if i != 0 {
                            print!("|")
                        }
                        print!("{row}");
                    }
                    println!();

                    count += 1;
                }
            }
            _ => bail!("cannot execute this command now"),
        }
        Ok(())
    }
}
