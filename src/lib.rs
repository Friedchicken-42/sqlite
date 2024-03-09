#![allow(dead_code)]

pub mod command;

use anyhow::{anyhow, bail, Result};
use command::Command;
use std::{
    borrow::Cow,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use crate::command::{Condition, WhereSt};

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

fn parse_sql(s: &str) -> Schema {
    let s = s.replace(['\t', '\n'], "");
    let (_, s) = s.split_once('(').unwrap();
    let (s, _) = s.split_once(')').unwrap();

    let cols = s.split(',');

    let mut schema = vec![];

    for col in cols {
        let split = col.split_whitespace().collect::<Vec<_>>();
        let name = split[0].to_string();

        let r#type = match split[1] {
            "text" => Type::Text,
            "integer" => Type::Integer,
            t => panic!("missing type: {t}"),
        };

        schema.push((name, r#type));
    }

    Schema(schema)
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug)]
pub struct Schema(Vec<(String, Type)>);

pub struct PageHeader {
    page_type: usize,
    freeblock: usize,
    cells: usize,
    offset: usize,
    frag: usize,
}

impl PageHeader {
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

pub struct Page {
    pub header: PageHeader,
    pointers: Vec<usize>,
    data: Vec<u8>,
}

impl Page {
    fn read(data: Vec<u8>, index: usize) -> Self {
        let offset = if index == 1 { 100 } else { 0 };
        let header = PageHeader::read(&data[offset..]);

        let offset = offset + 8;
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
pub struct Cell<'a> {
    data: Vec<&'a [u8]>,
    rowid: u64,
    schema: &'a Schema,
}

impl<'a> Cell<'a> {
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

        Self {
            data: records,
            rowid,
            schema,
        }
    }

    pub fn get(&self, column: &str) -> Result<Value<'a>> {
        let Some(index) = self.schema.0.iter().position(|(name, _)| name == column) else {
            bail!("column: {column} not found")
        };

        let (_, r#type) = &self.schema.0[index];
        let data = self.data[index];

        if data.is_empty() && *r#type != Type::Null {
            bail!("Empty data")
        }

        let value = match r#type {
            Type::Null => Value::Null,
            Type::Integer => Value::Integer(data[0] as u32),
            Type::Float => todo!(),
            Type::Text => Value::Text(Cow::Borrowed(std::str::from_utf8(data)?)),
            Type::Blob => todo!(),
        };

        Ok(value)
    }

    pub fn all(&self) -> Result<Vec<Value<'a>>> {
        // TODO: optimize this
        self.schema
            .0
            .iter()
            .map(|(name, _)| match self.get(name) {
                Err(_) if name == "id" => Ok(Value::Integer(self.rowid as u32)),
                x => x,
            })
            .collect::<Result<Vec<_>>>()
    }
}

pub struct CellIter<'a> {
    btreepage: &'a BTreePage<'a>,
    index: usize,
}

impl<'a> Iterator for CellIter<'a> {
    type Item = Cell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.btreepage.count() {
            return None;
        }

        // TODO: support other page types
        let page = self.btreepage.pages.first().unwrap();
        match page.header.page_type {
            0x0d => (),
            p => panic!("unsupported page: {p}"),
        }
        let offset = page.pointers[self.index];

        let cell = Cell::read(&page.data[offset..], &self.btreepage.schema);

        self.index += 1;

        Some(cell)
    }
}

pub struct BTreePage<'a> {
    pages: Vec<Page>,
    schema: Schema,
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

    pub fn cells(&'a self) -> CellIter<'a> {
        CellIter {
            btreepage: self,
            index: 0,
        }
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
        let root = self.root()?;
        let cell = root
            .cells()
            .find(|cell| {
                let Value::Text(tbl_name) = cell.get("tbl_name").unwrap() else {
                    panic!("expected \"tbl_name\" to be \"Text\"");
                };
                name == tbl_name
            })
            .ok_or(anyhow!("table \"{name}\" not found"))?;

        let Value::Integer(rootpage) = cell.get("rootpage")? else {
            anyhow::bail!("expected integer");
        };

        let Value::Text(sql) = cell.get("sql")? else {
            anyhow::bail!("expected text");
        };

        let schema = parse_sql(&sql);

        BTreePage::read(self, rootpage as usize, schema)
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
                let cells = table
                    .cells()
                    .filter(|cell| {
                        let Some(WhereSt {
                            ref column,
                            ref condition,
                            ref expected,
                        }) = r#where
                        else {
                            return true;
                        };
                        let value = cell.get(column).unwrap();
                        match condition {
                            Condition::Equals => value == *expected,
                        }
                    })
                    .map(|cell| {
                        select
                            .columns
                            .iter()
                            .flat_map(|name| {
                                if name == "*" {
                                    cell.all().unwrap()
                                } else {
                                    vec![cell.get(name).unwrap()]
                                }
                            })
                            .collect::<Vec<_>>()
                    });

                for cell in cells {
                    println!("{cell:?}");
                }
            }
        }
        Ok(())
    }
}
