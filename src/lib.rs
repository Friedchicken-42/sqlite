#![allow(dead_code)]

pub mod command;
pub mod page;

use anyhow::{bail, Result};
use command::{Column, Command, SimpleColumn};
use page::{Cell, Page};
use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::Display,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

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

impl<'a> Value<'a> {
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
pub struct Schema(pub Vec<(String, Type)>);

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
            .cells()
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

            match page {
                page @ (Page::TableLeaf(_) | Page::IndexLeaf(_)) => {
                    if *index >= page.cells() {
                        self.pages.pop();
                        self.indexes.pop();
                    } else {
                        let pointers = page.pointers();
                        let offset = pointers[*index];

                        *index += 1;

                        match (page, &filter) {
                            (Page::TableLeaf(t), None) => {
                                let cell = t.cell(offset, &self.schema);
                                return Some(Cell::TableLeaf(cell));
                            }
                            (Page::IndexLeaf(i), None) => {
                                let cell = i.cell(offset, &self.schema);
                                return Some(Cell::IndexLeaf(cell));
                            }
                            (Page::TableLeaf(t), Some(f)) => {
                                let cell = t.cell(offset, &self.schema);
                                let cell = Cell::TableLeaf(cell);

                                if f(&cell).is_eq() {
                                    return Some(cell);
                                }
                            }
                            (Page::IndexLeaf(i), Some(f)) => {
                                let cell = i.cell(offset, &self.schema);
                                let cell = Cell::IndexLeaf(cell);

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
                page @ (Page::TableInterior(_) | Page::IndexInterior(_)) => {
                    if *index > page.cells() * 2 {
                        self.indexes.pop();
                        self.pages.pop();
                    } else if *index == page.cells() * 2 {
                        *index += 1;

                        let right_pointer = match page {
                            Page::TableInterior(t) => t.right_pointer,
                            Page::IndexInterior(i) => i.right_pointer,
                            _ => unreachable!(),
                        };

                        let next_page = self.db.page(right_pointer).unwrap();

                        self.pages.push(next_page);
                        self.indexes.push(0);
                    } else if *index % 2 == 1 {
                        match page {
                            Page::TableInterior(_) => *index += 1,
                            Page::IndexInterior(i) => {
                                let pointers = page.pointers();
                                let offset = pointers[(*index - 1) / 2];

                                *index += 1;

                                let cell = i.cell(offset, &self.schema);
                                let cell = Cell::IndexInterior(cell);

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
                        let pointers = page.pointers();
                        let offset = pointers[*index / 2];

                        *index += 1;

                        let next_page = match (page, &filter) {
                            (Page::TableInterior(t), None) => {
                                let cell = t.cell(offset);
                                Some(cell.page as usize)
                            }
                            (Page::IndexInterior(i), None) => {
                                let cell = i.cell(offset, &self.schema);
                                Some(cell.page as usize)
                            }
                            (Page::TableInterior(t), Some(f)) => {
                                let cell = t.cell(offset);
                                let page = cell.page as usize;

                                let cell = Cell::TableInterior(cell);

                                if f(&cell).is_ge() {
                                    Some(page)
                                } else {
                                    None
                                }
                            }
                            (Page::IndexInterior(i), Some(f)) => {
                                let cell = i.cell(offset, &self.schema);
                                let page = cell.page as usize;

                                let cell = Cell::IndexInterior(cell);

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

                    let cmp = res.cmp(value);

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
                    Cell::TableLeaf(l) => l.rowid,
                    Cell::TableInterior(i) => i.key,
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

            if let Command::CreateIndex { table, columns, .. } =
                Command::parse(&sql.to_lowercase())?
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

        Page::read(data, index)
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

            println!("{sql:?}");

            return match Command::parse(&sql.to_lowercase())? {
                Command::CreateTable { schema, .. } => {
                    BTreePage::read(self, rootpage as usize, schema)
                }
                Command::CreateIndex { table, columns, .. } => {
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
                    if !filters.is_empty() {
                        iter = iter.filter(filters)?;
                    }
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
                        .map(|column| match column {
                            Column::Simple(s) => match s {
                                SimpleColumn::Wildcard => cell.all(),
                                SimpleColumn::String(name) => {
                                    vec![cell.get(name)].into_iter().collect()
                                }
                                _ => todo!(),
                            },
                            _ => todo!(),
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
            Command::CreateTable { table, schema } => todo!(),
            Command::CreateIndex {
                index,
                table,
                columns,
            } => todo!(),
        }

        Ok(())
    }
}
