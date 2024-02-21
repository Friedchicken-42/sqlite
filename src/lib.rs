#![allow(dead_code)]
pub mod command;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use anyhow::{bail, Context, Result};
pub use command::Command;
use command::WhereST;

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

fn parse_sql<'a>(s: &str) -> Vec<(String, Value<'a>)> {
    let s = s.replace("\t", "").replace("\n", "");
    let (_, s) = s.split_once("(").unwrap();
    let (s, _) = s.split_once(")").unwrap();

    let cols = s.split(",");

    let mut schema = vec![];

    for col in cols {
        let split = col.split_whitespace().collect::<Vec<_>>();
        let name = split[0].to_string();
        let r#type = split[1];

        let value = match r#type {
            "text" => Value::Text(""),
            "integer" => Value::Integer(0),
            t => panic!("missing type: {t}"),
        };

        schema.push((name, value));
    }

    schema
}

#[derive(Debug)]
pub struct Header {
    pub page_size: usize,
}

impl Header {
    fn read(data: &[u8]) -> Self {
        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;
        Self { page_size }
    }
}

#[derive(Debug)]
pub struct BTreeHeader {
    pub page_type: usize,
    pub freeblock: usize,
    pub cells: usize,
    pub offset: usize,
    pub frag: usize,
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

pub struct Record<'a> {
    payload: Vec<Vec<u8>>,
    schema: &'a [(String, Value<'a>)],
    pub rowid: u64,
}

impl<'a> Record<'a> {
    fn read(data: &[u8], schema: &'a [(String, Value)]) -> (Self, usize) {
        let mut offset = 0;
        let (payload_bytes, size) = read_varint(&data[offset..]);
        offset += size;

        let (rowid, size) = read_varint(&data[offset..]);
        offset += size;

        let (sizes, size) = parse_bytearray(&data[offset..]);
        let mut values_offset = offset + size;

        let mut payload = Vec::with_capacity(sizes.len());
        for size in sizes {
            let value = data[values_offset..][..size].to_vec();
            payload.push(value);
            values_offset += size;
        }

        offset += payload_bytes as usize;

        (
            Record {
                payload,
                rowid,
                schema,
            },
            offset,
        )
    }

    pub fn get(&'a self, column: &str) -> Value {
        let (index, (_, r#type)) = self
            .schema
            .iter()
            .enumerate()
            .find(|(_, (name, _))| *name == column)
            .unwrap();

        let value = &self.payload[index];

        match r#type {
            &Value::Text(_) => {
                let text = std::str::from_utf8(&value).unwrap();
                Value::Text(text)
            }
            &Value::Integer(_) => Value::Integer(value[0] as u64),
            _ => unimplemented!(),
        }
    }
}

pub struct RecordIter<'a> {
    records: usize,
    current: usize,
    offset: usize,
    base: &'a BTreePage<'a>,
    page: &'a BTreePage<'a>,
    db: &'a Sqlite,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.records {
            return None;
        }

        let (record, size) = Record::read(&self.page.data[self.offset..], &self.base.schema);
        self.offset += size;

        // TODO: overflow page
        self.current += 1;
        Some(record)
    }
}

#[derive(Debug)]
pub enum Value<'a> {
    Null,
    Integer(u64),
    Float(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> ToString for Value<'a> {
    fn to_string(&self) -> String {
        match self {
            Value::Null => "null".into(),
            Value::Integer(n) => format!("{n}"),
            Value::Float(n) => format!("{n}"),
            Value::Text(t) => t.to_string(),
            Value::Blob(b) => format!("{b:02x?}"),
        }
    }
}

pub struct BTreePage<'a> {
    data: Vec<u8>,
    pub header: BTreeHeader,
    schema: Vec<(String, Value<'a>)>,
    index: usize,
    db: &'a Sqlite,
}

impl<'a> BTreePage<'a> {
    fn read(data: Vec<u8>, index: usize, db: &'a Sqlite, schema: Vec<(String, Value<'a>)>) -> Self {
        let offset = if index == 1 { 100 } else { 0 };
        let header = BTreeHeader::read(&data[offset..]);

        Self {
            data,
            header,
            index,
            schema,
            db,
        }
    }

    pub fn records(&self) -> RecordIter {
        RecordIter {
            records: self.header.cells,
            current: 0,
            offset: self.header.offset,
            base: self,
            page: self,
            db: self.db,
        }
    }
}

pub struct Sqlite {
    file: File,
    pub header: Header,
}

impl Sqlite {
    pub fn read(path: &str) -> Result<Self> {
        let file = File::open(path)?;

        let mut buf = vec![0; 100];
        (&file).read_exact(&mut buf)?;
        let header = Header::read(&buf);

        Ok(Self { file, header })
    }

    fn page<'a>(&'a self, index: usize, schema: Vec<(String, Value<'a>)>) -> Result<BTreePage> {
        let offset = (index - 1) * self.header.page_size;

        (&self.file).seek(SeekFrom::Start(offset as u64))?;
        let mut data = vec![0; self.header.page_size];
        (&self.file).read_exact(&mut data)?;

        Ok(BTreePage::read(data, index, self, schema))
    }

    pub fn root(&self) -> Result<BTreePage> {
        let schema = vec![
            ("type".into(), Value::Text("")),
            ("name".into(), Value::Text("")),
            ("tbl_name".into(), Value::Text("")),
            ("rootpage".into(), Value::Integer(0)),
            ("sql".into(), Value::Text("")),
        ];
        self.page(1, schema)
    }

    pub fn table(&self, name: &str) -> Result<BTreePage> {
        let schema = self.root()?;
        let record = schema
            .records()
            .find(|r| {
                let Value::Text(tbl_name) = r.get("tbl_name") else {
                    panic!("expected text")
                };
                name == tbl_name
            })
            .context(format!("no table named {name:?}"))?;

        let Value::Integer(rootpage) = record.get("rootpage") else {
            anyhow::bail!("expected integer");
        };

        let Value::Text(sql) = record.get("sql") else {
            anyhow::bail!("expected text");
        };

        let schema = parse_sql(sql);

        let page = self.page(rootpage as usize, schema)?;
        Ok(page)
    }

    pub fn execute(&self, command: Command) -> Result<()> {
        // TODO: output something
        match command {
            Command::Select {
                select,
                from,
                r#where,
            } => {
                let table = self.table(from.table.as_str())?;

                let data = table.records().map(|record| {
                    select
                        .columns
                        .iter()
                        .map(move |column| (column, record.get(column).to_string()))
                        .collect::<Vec<_>>()
                    // TODO: better handling
                });

                let data = data.filter(|record| {
                    let Some(WhereST {
                        ref column,
                        ref condition,
                        ref expected,
                    }) = r#where
                    else {
                        return true;
                    };

                    for (col, value) in record {
                        if *col == column {
                            return match condition {
                                command::Condition::Equals => *value == *expected,
                            };
                        }
                    }

                    return true;
                });

                for record in data {
                    for (i, (_, value)) in record.iter().enumerate() {
                        print!("{value}");
                        if i != record.len() - 1 {
                            print!("|");
                        }
                    }
                    println!();
                }
            }
        }

        Ok(())
    }
}
