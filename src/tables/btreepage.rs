use std::{cmp::Ordering, num::NonZeroUsize};

use crate::{
    Access, Column, ErrorKind, Iterator, Result, Row, Rows, Schema, Serialized, Sqlite, Tabular,
    Type, Value,
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

    pub fn serialize(self) -> Vec<u8> {
        let mut data = Vec::with_capacity(9);
        let mut n = self.value;

        loop {
            let byte = (n & 0x7f) as u8;
            data.insert(0, byte | 0x80);

            n >>= 7;

            if n == 0 {
                break;
            }
        }

        *data.last_mut().unwrap() &= 0x7f;

        data
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

impl<'page> Access<'page> for Cell<'page> {
    fn get(&self, column: Column) -> Result<Value<'page>> {
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

    fn set_cells(&mut self, cells: u16) {
        let offset = self.offset();
        let data = &mut self.data[offset..];
        let cells = u16::to_be_bytes(cells);
        data[3] = cells[0];
        data[4] = cells[1];
    }

    fn increment_cells(&mut self) {
        let cells = self.cells();
        self.set_cells(cells + 1);
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

    fn serialize(&self, values: &[Value]) -> Vec<u8> {
        let mut serialized = vec![];
        let mut serials = vec![];

        for value in values {
            let Serialized { varint, data } = value.serialize();
            serialized.extend(data);
            serials.extend(varint.serialize());
        }

        // FIXME: this length could be larger than 127
        // aka could take more than 1 byte
        let length = Varint::new(serials.len() + 1);
        serials.insert(0, length.serialize()[0]);

        [serials, serialized].concat()
    }

    #[warn(clippy::identity_op)]
    fn insert(
        &mut self,
        pointer: u32,
        payload: Vec<u8>,
        rowid: usize,
        schema: &Schema,
    ) -> Result<()> {
        let data = match self.r#type()? {
            PageType::TableLeaf => [
                Varint::new(payload.len()).serialize(),
                Varint::new(rowid).serialize(),
                payload,
            ]
            .concat(),
            PageType::IndexLeaf => todo!(),
            PageType::TableInterior => [
                u32::to_be_bytes(pointer).to_vec(),
                Varint::new(rowid).serialize(),
            ]
            .concat(),
            PageType::IndexInterior => todo!(),
        };

        let page_size = self.data.len();

        let pointers = self.pointers()?;
        let end = pointers.min().unwrap_or(page_size);

        if data.len() > end {
            // Need overflow page or another page
            todo!();
        }

        let start = end - data.len();

        let mut pointers_offset = self.pointers_offset()? + 2 * self.cells() as usize;

        if start < pointers_offset + 3 {
            return Err(ErrorKind::PageFull.into());
        }

        let pointers = self.pointers()?.collect::<Vec<_>>();

        if pointers.is_empty()
            && matches!(self.r#type()?, PageType::TableInterior)
            && self.right_pointer() == 0
        {
            self.data[8..12].copy_from_slice(&data[..4]);
            return Ok(());
        }

        self.data[start..start + data.len()].copy_from_slice(&data);

        let mut index = 0;

        for offset in pointers.into_iter() {
            let cell = self.cell(offset, schema);

            match self.r#type()? {
                PageType::TableLeaf if cell.rowid()? < rowid => {
                    break;
                }
                PageType::IndexLeaf => todo!(),
                PageType::TableInterior if cell.rowid()? < rowid => {
                    break;
                }
                PageType::IndexInterior => todo!(),
                _ => {}
            }

            index += 1;
        }

        for _ in 0..index {
            pointers_offset -= 2;
            self.data[pointers_offset + 2] = self.data[pointers_offset + 0];
            self.data[pointers_offset + 3] = self.data[pointers_offset + 1];
        }

        self.data[pointers_offset + 0] = (start >> 8) as u8;
        self.data[pointers_offset + 1] = (start & 0xff) as u8;

        self.increment_cells();

        Ok(())
    }

    fn append(&mut self, payload: Vec<u8>) -> Result<()> {
        let page_size = self.data.len();

        let pointers = self.pointers()?;
        let end = pointers.min().unwrap_or(page_size);

        // TODO: add check

        let start = end - payload.len();
        self.data[start..end].copy_from_slice(&payload);

        let pointers_offset = self.pointers_offset()? + 2 * self.cells() as usize;
        self.data[pointers_offset + 0] = (start >> 8) as u8;
        self.data[pointers_offset + 1] = (start & 0xff) as u8;

        self.increment_cells();

        Ok(())
    }

    fn split(&self) -> Result<[(Page, usize); 2]> {
        let mut a = Page::new(None, self.data.len(), self.r#type()?);
        let mut b = Page::new(None, self.data.len(), self.r#type()?);

        let mut rowid_a = 0;
        let mut rowid_b = 0;

        let count = self.pointers()?.count();

        let pointers = self.pointers()?.collect::<Vec<_>>();

        for (i, offset) in pointers.into_iter().enumerate() {
            let current = if i < count / 2 { &mut a } else { &mut b };

            let Varint {
                value: payload_size,
                length: size_len,
            } = Varint::read(&self.data[offset..]);

            let Varint {
                value: rowid,
                length: rowid_len,
            } = Varint::read(&self.data[offset + size_len..]);

            let payload = self.data[offset..offset + size_len + rowid_len + payload_size].to_vec();

            current.append(payload)?;

            if i < count / 2 {
                rowid_a = rowid_a.max(rowid);
            } else {
                rowid_b = rowid_b.max(rowid);
            }
        }

        Ok([(a, rowid_a), (b, rowid_b)])
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

    fn append_page(&mut self, page: Page) {
        match self {
            Storage::Memory { pages, .. } => {
                pages.push(page);
            }
            Storage::File { .. } => todo!(),
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

impl<'db> Tabular<'db> for BTreePage<'db> {
    fn rows(&mut self) -> Rows<'_, 'db> {
        Rows::BTreePage(BTreeRows {
            btree: self,
            indexes: vec![],
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, _prefix: &str) -> std::fmt::Result {
        let names = self.schema.names.join(", ");
        writeln!(f, "Table Scan {{ {names} }}")
    }
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
        self.schema.name = Some(self.schema.names.len());
        self.schema.names.push(alias);
    }

    pub fn count(&self) -> usize {
        match &self.storage {
            Storage::Memory { pages, .. } => {
                let mut count = 0;

                for page in pages {
                    let r#type = page.r#type().unwrap();
                    if r#type == PageType::TableLeaf || r#type == PageType::IndexLeaf {
                        count += page.cells() as usize;
                    }
                }

                count
            }
            Storage::File { db, root, .. } => {
                let mut table = BTreePage::read(db, *root, self.schema.clone()).unwrap();
                let mut rows = table.rows();
                let mut count = 0;

                while rows.next().is_some() {
                    count += 1;
                }

                count
            }
        }
    }

    fn check_schema(&self, values: &[Value]) -> Result<()> {
        for (sr, value) in self.schema.columns.iter().zip(values.iter()) {
            if value.r#type() != Type::Null && sr.r#type != value.r#type() {
                return Err(ErrorKind::WrongValueType {
                    expected: sr.r#type,
                    actual: value.r#type(),
                }
                .into());
            }
        }

        Ok(())
    }

    // TableLeaf     -> rowid + values => append
    // IndexLeaf     -> values (sorted key -> others)
    // TableInterior -> pointer + rowid => append
    // IndexInterior -> pointer + values (sorted)
    #[allow(clippy::identity_op)]
    pub fn insert(&mut self, mut values: Vec<Value>) -> Result<()> {
        match self.storage.first().r#type()? {
            PageType::IndexLeaf | PageType::IndexInterior => {
                for i in self.schema.primary.iter().rev() {
                    let v = values.remove(*i);
                    values.insert(0, v);
                }
            }
            _ => {}
        };

        self.check_schema(&values)?;

        let rowid = self.count();

        let mut rows = BTreeRows {
            btree: self,
            indexes: vec![0],
        };

        rows.walk(&values, rowid);

        let pages = match &self.storage {
            Storage::Memory { pages, .. } => pages.len() as u32,
            Storage::File { .. } => todo!(),
        };

        let page = self.storage.last_mut();

        let payload = page.serialize(&values);

        match page.insert(0, payload, rowid, &self.schema) {
            Ok(_) => Ok(()),
            Err(e) if e.kind == ErrorKind::PageFull => {
                let [(a, rowid_a), (b, rowid_b)] = page.split()?;

                let size = page.data.len();
                *page = Page::new(None, size, PageType::TableInterior);

                page.insert(pages + 1, vec![], rowid_b, &self.schema)?;
                page.insert(pages + 0, vec![], rowid_a, &self.schema)?;

                self.storage.append_page(a);
                self.storage.append_page(b);

                self.insert(values)
            }
            e => e,
        }
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

    // This should be merged someway into `loop_until`
    fn walk(&mut self, values: &[Value], rowid: usize) {
        self.btree.storage.clear();

        loop {
            let page = self.btree.storage.last();

            match page.r#type().unwrap() {
                PageType::TableLeaf | PageType::IndexLeaf => return,
                PageType::TableInterior | PageType::IndexInterior => {}
            }

            let mut pointer = None;

            for (idx, offset) in page.pointers().unwrap().enumerate() {
                let cell = page.cell(offset, &self.btree.schema);

                match page.r#type().unwrap() {
                    PageType::TableInterior => {
                        if cell.rowid().unwrap() > rowid {
                            pointer = Some(offset);
                            *self.indexes.last_mut().unwrap() = idx;

                            break;
                        }
                    }
                    PageType::IndexInterior => todo!(),
                    _ => unreachable!(),
                }
            }

            let pointer = match pointer {
                Some(ptr) => {
                    let cell = page.cell(ptr, &self.btree.schema);
                    cell.page()
                }
                None => page.right_pointer(),
            };

            self.indexes.push(0);
            self.btree.storage.push(pointer);
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

        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
        let schema = Schema {
            names: vec![],
            name: None,
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

        for i in 1..100 {
            let mut btree = BTreePage {
                storage: Storage::Memory {
                    pages: vec![Page::new(None, 200, PageType::TableLeaf)],
                    stack: vec![0],
                },
                schema: schema.clone(),
                rowid: Some(0),
            };

            for j in 0..i {
                btree.insert(vec![Value::Integer(j), Value::Integer(j * 10)])?;
            }

            let mut table = Table::BTreePage(btree);
            let mut rows = table.rows();

            let mut count = 0;
            while let Some(row) = rows.next() {
                assert_eq!(row.get("a".into())?, Value::Integer(count));
                assert_eq!(row.get("b".into())?, Value::Integer(count * 10));
                count += 1;
            }

            assert_eq!(count, i, "Missing rows");
        }

        // .index(vec![column])
        // .rowid(false)
        // .build()?;
        // btree.insert(&[Value::Integer(10), Value::Integer(20)]);
        // - insert
        //   - if btree has `rowid` => walk until rowid match
        //   - else                 => walk until primary key match (?)

        Ok(())
    }
}
