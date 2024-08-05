pub mod command;
mod display;
mod materialized;
mod schema;
mod view;

use anyhow::{bail, Result};
use command::{Command, CreateIndex, CreateTable, InputColumn};
use display::{display, DisplayMode};
use schema::{Schema, SchemaRow};
use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::{Debug, Display},
    fs::File,
    hash::Hash,
    io::{Read, Seek, SeekFrom, Write},
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
    Blob(Cow<'a, [u8]>),
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

impl<'a> Hash for Value<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Float(f) if f.is_nan() => (-1_i8).hash(state),
            Value::Float(f) => {
                let bytes = f64::to_be_bytes(*f);
                bytes.hash(state);
            }
            v => core::mem::discriminant(v).hash(state),
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

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionParam {
    Wildcard,
    Column(Box<Column>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Function {
    Count(FunctionParam),
}

impl Function {
    pub fn default<'a>(&self, new: &[Option<Value>], schema: &Schema) -> Value<'a> {
        match self {
            Self::Count(FunctionParam::Wildcard) => Value::Integer(1),
            _ => todo!(),
        }
    }

    pub fn apply<'a>(
        &self,
        new: &[Option<Value>],
        old: &[Value],
        columns: &[Column],
        position: usize,
    ) -> Result<Value<'a>> {
        match self {
            Self::Count(FunctionParam::Wildcard) => {
                let Value::Integer(v) = old[position] else {
                    bail!("wrong value found")
                };
                Ok(Value::Integer(v + 1))
            }
            _ => todo!(),
        }
    }
}

impl Function {
    fn name(&self) -> &str {
        match self {
            Function::Count(_) => "count",
        }
    }

    fn full(&self) -> String {
        match self {
            Function::Count(FunctionParam::Wildcard) => "count(*)".to_string(),
            Function::Count(FunctionParam::Column(inner)) => format!("count({})", inner.full()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    String(String),
    Dotted { table: String, column: String },
    Function(Function),
}

impl From<&str> for Column {
    fn from(value: &str) -> Self {
        if value.contains("(") {
            let (function, inner) = value.split_once("(").unwrap();

            let inner = match &inner[..inner.len() - 1] {
                "*" => FunctionParam::Wildcard,
                param => FunctionParam::Column(Box::new(param.into())),
            };

            match function {
                "count" => Self::Function(Function::Count(inner)),
                _ => panic!(),
            }
        } else if value.contains(".") {
            let (table, column) = value.split_once(".").unwrap();
            Self::Dotted {
                table: table.to_string(),
                column: column.trim_end().to_string(),
            }
        } else {
            Self::String(value.to_string())
        }
    }
}

impl Column {
    pub fn name(&self) -> &str {
        match self {
            Column::String(n) => n,
            Column::Dotted { column, .. } => column,
            Column::Function(f) => f.name(),
        }
    }

    pub fn full(&self) -> String {
        match self {
            Column::String(s) => s.to_string(),
            Column::Dotted { table, column } => format!("{table}.{column}"),
            Column::Function(f) => f.full().to_string(),
        }
    }
}

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

        let (_, size) = read_varint(&data[offset..]);
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

pub trait Table: Debug {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>>;
    fn advance(&mut self);
    fn schema(&self) -> &Schema;

    fn next(&mut self) -> Option<Box<dyn Row<'_> + '_>> {
        self.advance();
        self.current()
    }
}

impl<T: Table + ?Sized> Table for Box<T> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        (**self).current()
    }

    fn advance(&mut self) {
        (**self).advance()
    }

    fn schema(&self) -> &Schema {
        (**self).schema()
    }
}

pub trait Row<'a>: Debug {
    fn get(&self, column: &Column) -> Result<Value<'a>>;
    fn all(&self) -> Result<Vec<Value<'a>>>;
}

impl<'a, T: Row<'a> + ?Sized> Row<'a> for Box<T> {
    fn get(&self, column: &Column) -> Result<Value<'a>> {
        (**self).get(column)
    }

    fn all(&self) -> Result<Vec<Value<'a>>> {
        (**self).all()
    }
}

#[derive(Debug, PartialEq)]
enum TableState {
    Start,
    Next(usize),
    End,
}

#[derive(Debug)]
pub struct BTreePage<'a> {
    pub name: String,
    pages: Vec<Page>,
    indexes: Vec<usize>,
    state: TableState,
    schema: Schema,
    db: &'a Sqlite,
}

impl<'a> BTreePage<'a> {
    fn read(db: &'a Sqlite, index: usize, name: String, schema: Schema) -> Result<Self> {
        let first_page = db.page(index)?;

        Ok(Self {
            name,
            pages: vec![first_page],
            indexes: vec![0],
            state: TableState::Start,
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

    fn loop_until<F>(&mut self, filter: Option<F>)
    where
        F: Fn(&Cell) -> Ordering,
    {
        loop {
            if self.pages.is_empty() {
                self.state = TableState::End;
                return;
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
                        self.state = TableState::Next(offset);

                        *index += 1;

                        match (page_type, &filter) {
                            (_, None) => {
                                return;
                            }
                            (PageType::TableLeaf, Some(f)) => {
                                let cell = page.cell(offset, &self.schema);
                                if f(&cell).is_eq() {
                                    return;
                                }
                            }
                            (PageType::IndexLeaf, Some(f)) => {
                                let cell = page.cell(offset, &self.schema);
                                match f(&cell) {
                                    Ordering::Less => {}
                                    Ordering::Equal => return,
                                    Ordering::Greater => {
                                        self.state = TableState::End;
                                        return;
                                    }
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
                                self.state = TableState::Next(offset);

                                *index += 1;

                                let cell = page.cell(offset, &self.schema);

                                if let Some(f) = &filter {
                                    if f(&cell).is_ge() {
                                        return;
                                    }
                                } else {
                                    return;
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        let pointers = &page.pointers;
                        let offset = pointers[*index / 2];
                        self.state = TableState::Next(offset);

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

    fn next_by_rowid(&mut self, rowid: usize) {
        let filter = move |cell: &Cell| {
            let row = cell.rowid().unwrap() as usize;
            row.cmp(&rowid)
        };

        self.loop_until(Some(filter));
    }
}

impl<'a> Table for BTreePage<'a> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        match self.state {
            TableState::Start | TableState::End => None,
            TableState::Next(offset) => {
                let page = self.pages.last().unwrap();

                let cell = page.cell(offset, &self.schema);
                Some(Box::new(cell))
            }
        }
    }

    fn advance(&mut self) {
        let filter = None.map(|_: ()| |_cell: &Cell| Ordering::Equal);
        self.loop_until(filter);
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug)]
pub struct BTreeIndex<'a> {
    btreepage: BTreePage<'a>,
    filters: Vec<(Column, Value<'a>)>,
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

        self.btreepage.loop_until(filter);
        let cell = self.btreepage.current()?;

        let Value::Blob(blob) = cell.get(&"index".into()).unwrap() else {
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

impl<'a> Table for IndexedRows<'a> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        self.btreepage.current()
    }

    fn advance(&mut self) {
        match self.btreeindex.next() {
            Some(rowid) => self.btreepage.next_by_rowid(rowid),
            None => self.btreepage.state = TableState::End,
        }
    }

    fn schema(&self) -> &Schema {
        &self.btreepage.schema
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
    fn get(&self, column: &Column) -> Result<Value<'a>> {
        let Some(index) = self
            .schema
            .iter()
            .position(|row| row.column.name() == column.name())
        else {
            bail!("column: {column:?} not found")
        };

        let r#type = self.schema[index].r#type;
        let data = self.records[index];

        if data.is_empty() && column.name() == "id" {
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
            Type::Blob => Value::Blob(Cow::Borrowed(data)),
        };

        Ok(value)
    }

    fn all(&self) -> Result<Vec<Value<'a>>> {
        self.schema
            .iter()
            .map(|row| self.get(&row.column))
            .collect::<Result<Vec<_>>>()
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
        let schema = vec![
            SchemaRow {
                column: "type".into(),
                r#type: Type::Text,
            },
            SchemaRow {
                column: "name".into(),
                r#type: Type::Text,
            },
            SchemaRow {
                column: "tbl_name".into(),
                r#type: Type::Text,
            },
            SchemaRow {
                column: "rootpage".into(),
                r#type: Type::Integer,
            },
            SchemaRow {
                column: "sql".into(),
                r#type: Type::Text,
            },
        ];

        BTreePage::read(self, 1, "root".into(), schema)
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

        while let Some(row) = root.next() {
            let Value::Text(tbl_name) = row.get(&"name".into())? else {
                bail!("Expected Text")
            };

            if name != tbl_name {
                continue;
            }

            let Value::Integer(rootpage) = row.get(&"rootpage".into())? else {
                bail!("Expected integer")
            };

            let Value::Text(sql) = row.get(&"sql".into())? else {
                bail!("Expected text")
            };

            let command = Command::parse(&sql.to_lowercase())?;

            return Ok((rootpage, command));
        }

        bail!("table {name:?} not found");
    }

    fn find_index(
        &self,
        from: &BTreePage,
        filters: &[(Column, Value<'_>)],
    ) -> Result<Option<BTreePage>> {
        let mut root = self.root()?;

        while let Some(index) = root.next() {
            let Value::Text(tbl_name) = index.get(&"name".into())? else {
                panic!("expected \"tbl_name\" to be \"Text\"");
            };

            if tbl_name == "sqlite_sequence" {
                continue;
            }

            let Value::Text(sql) = index.get(&"sql".into())? else {
                bail!("expected sql string");
            };

            if let Command::CreateIndex(CreateIndex {
                index,
                table,
                columns,
            }) = Command::parse(&sql.to_lowercase())?
            {
                if table != from.name {
                    continue;
                }

                let mut count = 0;
                for (col, _) in filters {
                    for column in &columns {
                        // TODO: find nearest
                        if col.name() == column {
                            count += 1;
                        }
                    }
                }

                if count != filters.len() {
                    continue;
                }

                let btreeindex = self.index(&tbl_name)?;
                return Ok(Some(btreeindex));
            };
        }

        Ok(None)
    }

    pub fn search<'a>(
        &'a self,
        name: &str,
        filters: Vec<(Column, Value<'a>)>,
    ) -> Result<Box<dyn Table + 'a>> {
        let table = self.table(name)?;

        if filters.is_empty() {
            Ok(Box::new(table))
        } else {
            let Some(index) = self.find_index(&table, &filters)? else {
                return Ok(Box::new(table));
            };

            let indexed = IndexedRows {
                btreepage: table,
                btreeindex: BTreeIndex {
                    btreepage: index,
                    filters,
                },
            };

            Ok(Box::new(indexed))
        }
    }

    pub fn table(&self, name: &str) -> Result<BTreePage> {
        let (rootpage, command) = self.find(name)?;

        if let Command::CreateTable(CreateTable { table, schema }) = command {
            BTreePage::read(self, rootpage as usize, name.into(), schema)
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

            for col in columns {
                for row in &table.schema {
                    if col == row.column.name() {
                        schema.push(SchemaRow {
                            column: col.as_str().into(),
                            ..row.clone()
                        });
                    }
                }
            }

            // why the FUCK is this not a varint (even if it should be)?
            // blob.iter().fold(0, |acc, x| (acc << 8) + x)
            schema.push(SchemaRow {
                column: "index".into(),
                r#type: Type::Blob,
            });

            BTreePage::read(self, rootpage as usize, name.into(), schema)
        } else {
            bail!("table {name:?} not found");
        }
    }

    pub fn show_tables(&self) -> Result<()> {
        let mut table = self.root()?;

        while let Some(row) = table.next() {
            let Value::Text(name) = row.get(&"name".into())? else {
                panic!("expected text");
            };

            if name != "sqlite_sequence" {
                print!("{name} ");
            }
        }

        println!();

        Ok(())
    }

    pub fn show_schema(&self) -> Result<()> {
        let mut table = self.root()?;

        while let Some(table) = table.next() {
            let Value::Text(name) = table.get(&"name".into())? else {
                panic!("expected text");
            };

            if name == "sqlite_sequence" || name.starts_with("sqlite_autoindex") {
                continue;
            }

            println!("schema {name:?}");

            let Value::Text(sql) = table.get(&"sql".into())? else {
                panic!("expected sql string");
            };

            match Command::parse(&sql.to_lowercase())? {
                Command::CreateTable(CreateTable { schema, .. }) => {
                    for row in schema.iter() {
                        println!("{row:?}");
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
        command.execute(self)
    }

    pub fn display(&self, f: &mut impl Write, table: impl Table, mode: DisplayMode) -> Result<()> {
        display(f, table, mode)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tracing::{span, Level};
    use tracing_test::traced_test;

    use crate::display::DisplayMode;
    use crate::{Row, Sqlite, Table, Value};

    #[traced_test]
    #[test]
    fn simple_db() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let mut table = db.search("apples", vec![])?;

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        // assert!(match &table {
        //     Table::Rows(rows) => rows.name == "apples",
        //     _ => false,
        // });

        let mut i = 0;
        while let Some(row) = table.next() {
            assert_eq!(row.get(&"id".into())?, Value::Integer(i + 1));
            i += 1;
        }

        assert_eq!(i, 4);

        Ok(())
    }

    #[test]
    fn current() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let mut table = db.search("apples", vec![])?;

        table.advance();

        let first = table.current().unwrap();
        let second = table.current().unwrap();

        assert!(first.get(&"id".into())? == second.get(&"id".into())?);
        assert!(first.get(&"name".into())? == second.get(&"name".into())?);

        Ok(())
    }

    #[traced_test]
    #[test]
    fn complex_db() -> Result<()> {
        let db = Sqlite::read("companies.db")?;
        let mut table = db.search("companies", vec![])?;

        // assert!(match &table {
        //     Table::Rows(rows) => rows.name == "companies",
        //     _ => false,
        // });

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 0;
        while let Some(row) = table.next() {
            row.get(&"id".into())?;
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
        let filters = vec![("name".into(), Value::Text("Fuji".into()))];
        let mut indexed = db.search("apples", filters)?;

        // assert!(match &indexed {
        //     Table::IndexedRows(IndexedRows {
        //         btreepage,
        //         btreeindex,
        //     }) => {
        //         btreepage.name == "apples" && btreeindex.btreepage.name == "apples_idx"
        //     }
        //     _ => false,
        // });

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        while let Some(row) = indexed.next() {
            row.get(&"id".into())?;
            row.get(&"name".into())?;
        }

        Ok(())
    }

    #[traced_test]
    #[test]
    fn complex_index() -> Result<()> {
        let db = Sqlite::read("companies.db")?;
        let filters = vec![("country".into(), Value::Text("tuvalu".into()))];
        let mut indexed = db.search("companies", filters)?;

        // assert!(match &indexed {
        //     Table::IndexedRows(IndexedRows {
        //         btreepage,
        //         btreeindex,
        //     }) => {
        //         btreepage.name == "companies"
        //             && btreeindex.btreepage.name == "idx_companies_country"
        //     }
        //     _ => false,
        // });

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 0;
        while let Some(row) = indexed.next() {
            let country = row.get(&"country".into())?;
            assert_eq!(country, Value::Text("tuvalu".into()));
            i += 1;
        }

        assert_eq!(i, 25);

        Ok(())
    }

    #[traced_test]
    #[test]
    fn simple_all() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let mut table = db.search("apples", vec![])?;

        let span = span!(Level::INFO, "Loop");
        let _enter = span.enter();

        let mut i = 1;

        while let Some(row) = table.next() {
            let all = row.all()?;

            assert_eq!(all.len(), 3);
            assert_eq!(all[0], Value::Integer(i));
            assert!(matches!(all[1], Value::Text(_)));
            assert!(matches!(all[2], Value::Text(_)));

            i += 1;
        }

        Ok(())
    }

    #[test]
    fn display() -> Result<()> {
        use std::io::Cursor;
        let mut f = Cursor::new(vec![]);

        let db = Sqlite::read("sample.db")?;

        let table = db.search("apples", vec![])?;
        db.display(&mut f, table, DisplayMode::List)?;

        let table = db.search("apples", vec![])?;
        db.display(&mut f, table, DisplayMode::Table)?;

        println!("{}", std::str::from_utf8(f.get_ref())?);

        Ok(())
    }
}
