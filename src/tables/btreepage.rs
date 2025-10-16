use std::{cmp::Ordering, num::NonZeroUsize};

use crate::{
    Column, ErrorKind, Iterator, Result, Row, Rows, Schema, Serialized, Sqlite, Type, Value,
};
use std::fmt::Debug;

#[derive(Debug)]
pub struct Varint {
    pub value: usize,
    pub length: usize,
}

impl Varint {
    pub fn new(value: usize) -> Self {
        Self {
            value,
            length: value.div_ceil(0x80),
        }
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
            n if n >= 12 && n.is_multiple_of(2) => (n - 12) / 2,
            n if n >= 13 && !n.is_multiple_of(2) => (n - 13) / 2,
            _ => panic!("not supported"),
        };

        Self { varint, size }
    }
}

pub struct Cell<'page> {
    data: &'page [u8],
    schema: &'page Schema,
    page_type: PageType,
}

impl<'page> Cell<'page> {
    fn page(&self) -> usize {
        u32::from_be_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize
    }

    pub fn rowid(&self) -> Result<usize> {
        match self.page_type {
            PageType::TableLeaf => {
                let Varint { length, .. } = Varint::read(self.data);
                let Varint { value, .. } = Varint::read(&self.data[length..]);
                Ok(value)
            }
            PageType::TableInterior => {
                let Varint { value, .. } = Varint::read(&self.data[4..]);
                Ok(value)
            }
            PageType::IndexLeaf => Err(ErrorKind::WrongPageType(0x0a).into()),
            PageType::IndexInterior => Err(ErrorKind::WrongPageType(0x02).into()),
        }
    }

    pub fn get(&self, column: Column) -> Result<Value<'page>> {
        let name = match column {
            Column::Single(name) => name,
            Column::Dotted { table, column } if self.schema.names.contains(&table) => column,
            Column::Dotted { table, .. } => return Err(ErrorKind::TableNotFound(table).into()),
        };

        let Some(index) = self
            .schema
            .columns
            .iter()
            .position(|sr| matches!(&*sr.column, Column::Single(c) if *c == name))
        else {
            return Err(ErrorKind::ColumnNotFound(Column::Single(name)).into());
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
    fn new(header_file_size: Option<NonZeroUsize>, page_size: usize, page_type: PageType) -> Page {
        let mut data = vec![0; page_size];

        data[0] = match page_type {
            PageType::TableLeaf => 0x0d,
            PageType::IndexLeaf => 0x0a,
            PageType::TableInterior => 0x05,
            PageType::IndexInterior => 0x02,
        };

        Page {
            data,
            header_file_size,
        }
    }

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
            p => Err(ErrorKind::WrongPageType(p).into()),
        }
    }

    fn cells(&self) -> u16 {
        let data = &self.data[self.offset()..];
        u16::from_be_bytes([data[3], data[4]])
    }

    fn increment_cells(&mut self) {
        let cells = self.cells();
        let offset = self.offset();
        let data = &mut self.data[offset..];
        let cells = u16::to_be_bytes(cells + 1);
        data[3] = cells[0];
        data[4] = cells[1];
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

    fn cell<'page>(&'page self, offset: usize, schema: &'page Schema) -> Cell<'page> {
        let data = &self.data[offset..];
        Cell {
            data,
            schema,
            page_type: self.r#type().unwrap(),
        }
    }

    fn right_pointer(&self) -> usize {
        u32::from_be_bytes([self.data[8], self.data[9], self.data[10], self.data[11]]) as usize
    }
}

#[derive(PartialEq, Debug)]
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

    fn first_mut(&mut self) -> &mut Page {
        match self {
            Storage::Memory { pages, .. } => &mut pages[0],
            Storage::File { stack, .. } => &mut stack[0],
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

pub struct BTreePageBuilder {
    header_page_size: Option<NonZeroUsize>,
    page_size: usize,
    is_index: bool,
    schema: Schema,
}

impl BTreePageBuilder {
    pub fn new(schema: Schema) -> Self {
        Self {
            header_page_size: None,
            page_size: 200,
            is_index: false,
            schema,
        }
    }

    pub fn build<'db>(self) -> Result<BTreePage<'db>> {
        let page_type = match self.is_index {
            false => PageType::TableLeaf,
            true => PageType::IndexLeaf,
        };

        let page = Page::new(self.header_page_size, self.page_size, page_type);

        Ok(BTreePage {
            storage: Storage::Memory {
                pages: vec![page],
                stack: vec![0],
            },
            schema: self.schema,
            rowid: Some(0),
        })
    }
}

pub struct BTreePage<'db> {
    storage: Storage<'db>,
    schema: Schema,
    rowid: Option<usize>,
}

impl<'db> BTreePage<'db> {
    pub fn read(db: &'db Sqlite, index: usize, schema: Schema) -> Result<Self> {
        Ok(Self {
            storage: Storage::File {
                db,
                root: index,
                stack: vec![],
            },
            schema,
            rowid: Some(0),
        })
    }

    pub fn add_alias(&mut self, alias: String) {
        self.schema.names.push(alias);
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

    pub fn insert(&mut self, values: &[Value]) -> Result<()> {
        let mut rows = vec![];

        for (sr, value) in self.schema.columns.iter().zip(values.iter()) {
            if value.r#type() != Type::Null && sr.r#type != value.r#type() {
                return Err(ErrorKind::WrongValueType {
                    expected: sr.r#type,
                    actual: value.r#type(),
                }
                .into());
            }

            rows.push(value.serialize());
        }

        // FIXME: temp implementation for group by
        let page = self.storage.last_mut();
        let mut offset = page.pointers()?.last().unwrap_or(page.data.len());
        offset -= 1;

        rows.reverse();

        let mut size = 0;

        for Serialized { data, .. } in &rows {
            for (i, d) in data.iter().rev().enumerate() {
                page.data[offset - i] = *d;
            }

            offset -= data.len();
            size += data.len();
        }

        for Serialized { varint, .. } in &rows {
            page.data[offset] = varint.value as u8;
            offset -= 1;
        }

        page.data[offset] = (rows.len() as u8) + 1;
        offset -= 1;

        page.data[offset] = 1;
        offset -= 1;

        page.data[offset] = size as u8;

        // pointers
        let cells = page.pointers()?.count();
        let ptr = page.pointers_offset()?;
        page.data[ptr + cells * 2 + 0] = (offset >> 8) as u8;
        page.data[ptr + cells * 2 + 1] = (offset & 0xff) as u8;

        self.storage.first_mut().increment_cells();

        Ok(())
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let names = self.schema.names.join(", ");
        let indent = "  ".repeat(indent);

        writeln!(f, "{:<width$} │ {}{}", "from", indent, names)
    }
}

pub struct BTreeRows<'rows, 'db> {
    pub btree: &'rows mut BTreePage<'db>,
    pub indexes: Vec<usize>,
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
        let cell = page.cell(offset, &self.btree.schema);

        Some(Row::Cell(cell))
    }

    fn advance(&mut self) {
        self.loop_until(|_| Ordering::Equal);
    }
}

impl BTreeRows<'_, '_> {
    pub fn loop_until<F: Fn(&Cell) -> Ordering>(&mut self, filter: F) {
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
                        begin = false;
                    } else {
                        let mut pointers = page.pointers().unwrap();
                        let offset = pointers.nth(*index).unwrap();
                        drop(pointers);

                        match page_type {
                            PageType::TableLeaf => {
                                let cell = page.cell(offset, &self.btree.schema);

                                if filter(&cell).is_eq() {
                                    return;
                                }
                            }
                            PageType::IndexLeaf => {
                                let cell = page.cell(offset, &self.btree.schema);
                                match filter(&cell) {
                                    Ordering::Less => {}
                                    Ordering::Equal => return,
                                    Ordering::Greater => {
                                        self.btree.storage.clear();
                                        self.indexes = vec![];
                                        return;
                                    }
                                }
                            }
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

                                let cell = page.cell(offset, &self.btree.schema);

                                if filter(&cell).is_ge() {
                                    return;
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        let mut pointers = page.pointers().unwrap();
                        let offset = pointers.nth(*index / 2).unwrap();
                        drop(pointers);

                        let cell = page.cell(offset, &self.btree.schema);
                        if filter(&cell).is_ge() {
                            let pointer = cell.page();

                            begin = true;

                            self.indexes.push(0);
                            self.btree.storage.push(pointer);
                        } else {
                            begin = false;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Result, SchemaRow, Table, Type,
        parser::{Query, Spanned},
    };

    use super::*;

    #[test]
    fn btreepage() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select * from apples")?;
        let mut table = db.execute(query)?;

        assert!(matches!(table, Table::BTreePage(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("id".into())?;
            assert_eq!(value, Value::Integer(i));

            let value = row.get("apples.id".into())?;
            assert_eq!(value, Value::Integer(i));

            assert!(row.get("name".into()).is_ok());
            assert!(row.get("color".into()).is_ok());
            assert!(row.get("apple".into()).is_err());
        }

        assert_eq!(table.count(), i as usize);

        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
        let schema = Schema {
            names: vec![],
            columns: vec![
                SchemaRow {
                    column: Spanned::empty("a".into()),
                    r#type: Type::Integer,
                },
                SchemaRow {
                    column: Spanned::empty("b".into()),
                    r#type: Type::Integer,
                },
            ],
            primary: vec![0],
        };

        let mut btree = BTreePage {
            storage: Storage::Memory {
                pages: vec![Page::new(None, 200, PageType::TableLeaf)],
                stack: vec![0],
            },
            schema,
            rowid: Some(0),
        };

        // .index(vec![column])
        // .rowid(false)
        // .build()?;
        // btree.insert(&[Value::Integer(10), Value::Integer(20)]);
        // - insert
        //   - if btree has `rowid` => walk until rowid match
        //   - else                 => walk until primary key match (?)

        btree.insert(&[Value::Integer(1), Value::Integer(10)])?;
        btree.insert(&[Value::Integer(2), Value::Integer(20)])?;
        btree.insert(&[Value::Integer(3), Value::Integer(30)])?;
        btree.insert(&[Value::Integer(4), Value::Integer(40)])?;

        let mut table = Table::BTreePage(btree);
        let mut rows = table.rows();

        let mut i = 1;
        while let Some(row) = rows.next() {
            assert_eq!(row.get("a".into())?, Value::Integer(i));
            assert_eq!(row.get("b".into())?, Value::Integer(i * 10));

            i += 1;
        }

        Ok(())
    }
}
