use std::borrow::Cow;

use anyhow::{bail, Result};

use crate::{Schema, Type, Value};

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

#[derive(Debug)]
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

#[derive(Debug)]
struct Inner {
    header: PageHeader,
    pointers: Vec<usize>,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct TableLeaf {
    inner: Inner,
}

impl TableLeaf {
    pub fn cell<'a>(&'a self, offset: usize, schema: &'a Schema) -> TableLeafCell<'a> {
        let data = &self.inner.data[offset..];
        let mut offset = 0;

        let (payload_bytes, size) = read_varint(data);
        offset += size;

        let (rowid, size) = read_varint(&data[offset..]);
        offset += size;

        let (records, values_offset) = bytearray_values(&data[offset..]);

        assert!(values_offset == payload_bytes as usize);

        let u = 4096;
        let x = u - 35;
        assert!(size <= x);

        TableLeafCell {
            data: records,
            rowid,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct IndexLeaf {
    inner: Inner,
}

impl IndexLeaf {
    pub fn cell<'a>(&'a self, offset: usize, schema: &'a Schema) -> IndexLeafCell<'a> {
        let data = &self.inner.data[offset..];
        let mut offset = 0;

        let (payload_bytes, size) = read_varint(data);
        offset += size;

        let (records, values_offset) = bytearray_values(&data[offset..]);

        assert!(values_offset == payload_bytes as usize);

        // U = db.header.page_size - db.header.reserved
        let u = 4096;
        let x = ((u - 12) * 64 / 255) - 23;
        assert!(size <= x);

        IndexLeafCell {
            data: records,
            schema,
        }
    }
}

#[derive(Debug)]
pub struct TableInterior {
    inner: Inner,
    pub right_pointer: usize,
}

impl TableInterior {
    pub fn cell(&self, offset: usize) -> TableInteriorCell {
        let data = &self.inner.data[offset..];

        let page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        let (key, _) = read_varint(&data[4..]);

        TableInteriorCell { page, key }
    }
}

#[derive(Debug)]
pub struct IndexInterior {
    inner: Inner,
    pub right_pointer: usize,
}

impl IndexInterior {
    pub fn cell<'a>(&'a self, offset: usize, schema: &'a Schema) -> IndexInteriorCell<'a> {
        let data = &self.inner.data[offset..];

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

        IndexInteriorCell {
            page,
            data: records,
            schema,
        }
    }
}

#[derive(Debug)]
pub enum Page {
    TableLeaf(TableLeaf),
    IndexLeaf(IndexLeaf),
    TableInterior(TableInterior),
    IndexInterior(IndexInterior),
}

impl Page {
    pub fn read(data: Vec<u8>, index: usize) -> Result<Self> {
        let offset = if index == 1 { 100 } else { 0 };
        let header = PageHeader::read(&data[offset..]);

        let right_pointer = u32::from_be_bytes([
            data[offset + 8],
            data[offset + 9],
            data[offset + 10],
            data[offset + 11],
        ]) as usize;

        let page_type = header.page_type;

        let offset = if page_type == 0x02 || page_type == 0x05 {
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

        let inner = Inner {
            header,
            pointers,
            data,
        };

        match page_type {
            0x0d => Ok(Self::TableLeaf(TableLeaf { inner })),
            0x0a => Ok(Self::IndexLeaf(IndexLeaf { inner })),
            0x05 => Ok(Self::TableInterior(TableInterior {
                inner,
                right_pointer,
            })),
            0x02 => Ok(Self::IndexInterior(IndexInterior {
                inner,
                right_pointer,
            })),
            p => bail!("wrong page_type: {p:?}"),
        }
    }

    fn inner(&self) -> &Inner {
        match self {
            Page::TableLeaf(p) => &p.inner,
            Page::IndexLeaf(p) => &p.inner,
            Page::TableInterior(p) => &p.inner,
            Page::IndexInterior(p) => &p.inner,
        }
    }

    pub fn cells(&self) -> usize {
        self.inner().header.cells
    }

    pub fn pointers(&self) -> &[usize] {
        &self.inner().pointers
    }
}

#[derive(Debug)]
pub enum Cell<'a> {
    TableLeaf(TableLeafCell<'a>),
    IndexLeaf(IndexLeafCell<'a>),
    TableInterior(TableInteriorCell),
    IndexInterior(IndexInteriorCell<'a>),
}

impl<'a> Cell<'a> {
    pub fn get(&self, column: &str) -> Result<Value<'a>> {
        let (data, schema, rowid) = match self {
            Cell::TableLeaf(c) => (&c.data, &c.schema, c.rowid),
            Cell::IndexLeaf(c) => (&c.data, &c.schema, 0),
            Cell::TableInterior(_) => bail!("cannot `get` from a TableInterior cell"),
            Cell::IndexInterior(c) => (&c.data, &c.schema, 0),
        };

        let Some(index) = schema.0.iter().position(|(name, _)| name == column) else {
            bail!("column: {column:?} not found")
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
            Cell::TableLeaf(c) => &c.schema,
            Cell::IndexLeaf(c) => &c.schema,
            Cell::TableInterior(_) => bail!("TableInterior cell does not have a schema"),
            Cell::IndexInterior(c) => &c.schema,
        };

        schema
            .0
            .iter()
            .map(|(name, _)| self.get(name))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug)]
pub struct TableLeafCell<'a> {
    data: Vec<&'a [u8]>,
    pub rowid: u64,
    schema: &'a Schema,
}

#[derive(Debug)]
pub struct IndexLeafCell<'a> {
    data: Vec<&'a [u8]>,
    schema: &'a Schema,
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub page: u32,
    pub key: u64,
}

#[derive(Debug)]
pub struct IndexInteriorCell<'a> {
    pub page: u32,
    data: Vec<&'a [u8]>,
    schema: &'a Schema,
}
