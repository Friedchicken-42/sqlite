use std::{cmp::Ordering, num::NonZeroUsize};

use crate::{Column, Iterator, Result, Row, Rows, Schema, Sqlite, SqliteError, Value};

#[derive(Debug)]
pub struct Varint {
    pub value: usize,
    pub length: usize,
}

impl Varint {
    pub fn new(n: usize) -> Self {
        let data = usize::to_le_bytes(n);
        Self::read(&data)
    }

    pub fn read(data: &[u8]) -> Self {
        let mut length = 0;
        let mut number = 0;

        loop {
            let byte = data[length];

            if byte & 0x80 == 0 || length == 9 {
                break;
            }

            number = number << 7 | (byte & 0x7f) as usize;
            length += 1;
        }

        if length != 9 {
            number = number << 7 | data[length] as usize;
        }

        Self {
            value: number,
            length: length + 1,
        }
    }
}

#[derive(Debug)]
pub struct Serial {
    varint: Varint,
    size: usize,
}
impl Serial {
    fn read(data: &[u8]) -> Self {
        let varint = Varint::read(data);
        let size = match varint.value {
            n @ 0..=4 => n,
            5 => 6,
            6 | 7 => 8,
            8 | 9 => 0,
            n if n >= 12 && n % 2 == 0 => (n - 12) / 2,
            n if n >= 13 && n % 2 == 1 => (n - 13) / 2,
            _ => panic!("not supported"),
        };

        Self { varint, size }
    }
}

pub struct Cell<'page> {
    data: &'page [u8],
    schema: &'page Schema,
    page_type: PageType,
    names: &'page [String],
}

impl<'page> Cell<'page> {
    fn page(&self) -> usize {
        u32::from_be_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize
    }

    pub fn get(&self, column: Column) -> Result<Value<'page>> {
        let name = match column {
            Column::Single(name) => name,
            Column::Dotted { table, column } if self.names.contains(&table) => column,
            Column::Dotted { table, .. } => return Err(SqliteError::TableNotFound(table)),
        };

        let Some(index) = self
            .schema
            .0
            .iter()
            .position(|sr| matches!(&sr.column, Column::Single(c) if *c == name))
        else {
            return Err(SqliteError::ColumnNotFound(Column::Single(name)));
        };

        let mut offset = if self.page_type == PageType::IndexInterior {
            4
        } else {
            0
        };

        let payload_size = Varint::read(&self.data[offset..]);
        offset += payload_size.length;

        let rowid = if self.page_type == PageType::TableLeaf {
            let rowid = Varint::read(&self.data[offset..]);
            offset += rowid.length;
            rowid.value
        } else {
            0
        };

        let payload_header_size = Varint::read(&self.data[offset..]);
        let mut payload = offset + payload_header_size.value;
        offset += payload_header_size.length;

        for _ in 0..index {
            let Serial { varint, size } = Serial::read(&self.data[offset..]);

            offset += varint.length;
            payload += size;
        }

        let serial = Serial::read(&self.data[offset..]);
        let value = Value::read(&self.data[payload..], &serial.varint)?;

        match value {
            Value::Null if name == "id" => Ok(Value::Integer(rowid as u64)),
            other => Ok(other),
        }
    }
}

#[derive(Debug)]
pub struct Page {
    pub data: Vec<u8>,
    pub header_file_size: Option<NonZeroUsize>,
}

impl Page {
    fn offset(&self) -> usize {
        match self.header_file_size {
            Some(n) => n.into(),
            None => 0,
        }
    }

    fn r#type(&self) -> Result<PageType> {
        match self.data[self.offset()] {
            0x0d => Ok(PageType::TableLeaf),
            0x0a => Ok(PageType::IndexLeaf),
            0x05 => Ok(PageType::TableInterior),
            0x02 => Ok(PageType::IndexInterior),
            p => Err(SqliteError::WrongPageType(p)),
        }
    }

    fn cells(&self) -> u16 {
        let data = &self.data[self.offset()..];
        u16::from_be_bytes([data[3], data[4]])
    }

    fn pointers_offset(&self) -> Result<usize> {
        let r#type = self.r#type()?;

        if r#type == PageType::TableInterior || r#type == PageType::IndexInterior {
            Ok(self.offset() + 12)
        } else {
            Ok(self.offset() + 8)
        }
    }

    fn pointers(&self) -> Result<impl std::iter::Iterator<Item = usize>> {
        let offset = self.pointers_offset()?;

        let pointers = (0..self.cells()).map(move |i| {
            let off = offset + i as usize * 2;
            u16::from_be_bytes([self.data[off], self.data[off + 1]]) as usize
        });

        Ok(pointers)
    }

    fn cell<'page>(
        &'page self,
        offset: usize,
        schema: &'page Schema,
        names: &'page [String],
    ) -> Cell<'page> {
        let data = &self.data[offset..];
        Cell {
            data,
            schema,
            page_type: self.r#type().unwrap(),
            names,
        }
    }

    fn right_pointer(&self) -> usize {
        u32::from_be_bytes([self.data[8], self.data[9], self.data[10], self.data[11]]) as usize
    }
}

#[derive(PartialEq)]
enum PageType {
    TableLeaf,
    IndexLeaf,
    TableInterior,
    IndexInterior,
}

enum Storage<'db> {
    Memory {
        pages: Vec<Page>,
        stack: Vec<usize>,
    },
    File {
        db: &'db Sqlite,
        root: usize,
        stack: Vec<Page>,
    },
}

impl Storage<'_> {
    fn first(&self) -> &Page {
        match self {
            Storage::Memory { pages, .. } => &pages[0],
            Storage::File { stack, .. } => &stack[0],
        }
    }

    fn last(&self) -> &Page {
        match self {
            Storage::Memory { pages, stack } => {
                let last = *stack.last().unwrap();
                &pages[last]
            }
            Storage::File { stack, .. } => stack.last().unwrap(),
        }
    }

    fn last_mut(&mut self) -> &mut Page {
        match self {
            Storage::Memory { pages, stack } => {
                let last = *stack.last().unwrap();
                &mut pages[last]
            }
            Storage::File { stack, .. } => stack.last_mut().unwrap(),
        }
    }

    fn clear(&mut self) {
        match self {
            Storage::Memory { stack, .. } => {
                stack.clear();
                stack.push(0);
            }
            Storage::File {
                db, stack, root, ..
            } => {
                stack.clear();
                let first = db.page(*root).unwrap();
                stack.push(first);
            }
        }
    }

    fn push(&mut self, pointer: usize) {
        match self {
            Storage::Memory { stack, .. } => stack.push(pointer),
            Storage::File { db, stack, .. } => {
                let page = db.page(pointer).unwrap();
                stack.push(page);
            }
        }
    }

    fn pop(&mut self) {
        match self {
            Storage::Memory { stack, .. } => {
                stack.pop();
            }
            Storage::File { stack, .. } => {
                stack.pop();
            }
        }
    }
}

pub struct BTreePage<'db> {
    storage: Storage<'db>,
    schema: Schema,
    rowid: usize,
    names: Vec<String>,
}

impl<'db> BTreePage<'db> {
    pub fn read(
        db: &'db Sqlite,
        index: usize,
        schema: Schema,
        name: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            storage: Storage::File {
                db,
                root: index,
                stack: vec![],
            },
            schema,
            rowid: 0,
            names: match name {
                Some(s) => vec![s],
                None => vec![],
            },
        })
    }

    pub fn rows(&mut self) -> Rows<'_, 'db> {
        Rows::BTreePage(BTreeRows {
            btree: self,
            indexes: vec![],
        })
    }

    pub fn count(&mut self) -> usize {
        self.storage.clear();
        let first = self.storage.first();
        first.cells() as usize
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn add_alias(self, alias: String) -> Self {
        let mut names = self.names;
        names.push(alias);

        Self { names, ..self }
    }
}

pub struct BTreeRows<'rows, 'db> {
    btree: &'rows mut BTreePage<'db>,
    indexes: Vec<usize>,
}

impl Iterator for BTreeRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        let last = self.indexes.last()?;
        let page = &self.btree.storage.last();

        let index = match page.r#type().unwrap() {
            PageType::TableLeaf | PageType::IndexLeaf => *last,
            PageType::IndexInterior => *last / 2,
            _ => unreachable!(),
        };

        let offset = page.pointers().unwrap().nth(index).unwrap();
        let cell = page.cell(offset, &self.btree.schema, &self.btree.names);

        Some(Row::Cell(cell))
    }

    fn advance(&mut self) {
        self.loop_until(|_| Ordering::Equal);
    }
}

impl BTreeRows<'_, '_> {
    fn loop_until<F: Fn(&Cell) -> Ordering>(&mut self, filter: F) {
        let mut begin = self.indexes.is_empty();

        if begin {
            self.btree.storage.clear();
            self.indexes = vec![0];
        }

        loop {
            let Some(index) = self.indexes.last_mut() else {
                return;
            };

            let page = self.btree.storage.last();

            if !begin {
                *index += 1;
            }

            match page.r#type().unwrap() {
                page_type @ (PageType::TableLeaf | PageType::IndexLeaf) => {
                    if *index >= page.cells().into() {
                        // Reached the end of this page, pop it
                        self.indexes.pop();
                        self.btree.storage.pop();
                    } else {
                        let mut pointers = page.pointers().unwrap();
                        let offset = pointers.nth(*index).unwrap();

                        match page_type {
                            PageType::TableLeaf => {
                                let cell = page.cell(offset, &self.btree.schema, &self.btree.names);

                                if filter(&cell).is_eq() {
                                    return;
                                }
                            }
                            PageType::IndexLeaf => return,
                            _ => unreachable!(),
                        }

                        if begin {
                            *index += 1;
                        }
                    }
                }
                page_type @ (PageType::TableInterior | PageType::IndexInterior) => {
                    #[allow(clippy::comparison_chain)]
                    if *index > (page.cells() * 2).into() {
                        // Already read the right_pointer
                        self.indexes.pop();
                        self.btree.storage.pop();
                    } else if *index == (page.cells() * 2).into() {
                        let pointer = page.right_pointer();

                        begin = true;

                        self.indexes.push(0);
                        self.btree.storage.push(pointer);
                    } else if *index % 2 == 1 {
                        match page_type {
                            PageType::TableInterior => {}
                            PageType::IndexInterior => {
                                let mut pointers = page.pointers().unwrap();
                                let offset = pointers.nth((*index - 1) / 2).unwrap();

                                let cell = page.cell(offset, &self.btree.schema, &self.btree.names);

                                if filter(&cell).is_eq() {
                                    return;
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        let mut pointers = page.pointers().unwrap();
                        let offset = pointers.nth(*index / 2).unwrap();
                        drop(pointers);

                        // TODO: add function check
                        let cell = page.cell(offset, &self.btree.schema, &self.btree.names);
                        let pointer = cell.page();

                        begin = true;

                        self.indexes.push(0);
                        self.btree.storage.push(pointer);
                    }
                }
            }
        }
    }
}
