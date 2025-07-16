mod btreepage;
pub mod display;
pub mod parser;
mod view;

use ariadne::{Color, Label, Report, ReportKind, Source};
use btreepage::{BTreePage, BTreeRows, Cell, Page, Varint};
use parser::{CreateTableStatement, From, Query, Select, SelectStatement};
use std::{
    fmt::{Debug, Display},
    fs::File,
    io::{Read, Seek, SeekFrom},
    num::NonZeroUsize,
    ops::Range,
};
use thiserror::Error;
use view::{View, ViewRow, ViewRows};

#[derive(Error)]
pub enum SqliteError {
    FileNotFound(String),
    WrongColumn(Column),
    ColumnNotFound(Column),
    UnhandledSerial(usize),
    PageRead {
        index: usize,
        offset: usize,
    },
    WrongPageType(u8),
    WrongValueType {
        expected: Type,
        actual: Type,
    },
    TableNotFound(String),
    WrongCommand(Query),
    Parser {
        query: String,
        span: Range<usize>,
        message: String,
    },
}

impl SqliteError {
    fn format(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buffer = vec![];
        let source = Source::from("");

        match self {
            SqliteError::FileNotFound(f) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("File \"{f}\" not found"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::WrongColumn(column) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Unexpected column: \"{column:?}\""))
                .finish()
                .write(source, &mut buffer),
            SqliteError::ColumnNotFound(column) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Column \"{column:?}\" not found"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::UnhandledSerial(n) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Unhandled erial value: {n}"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::PageRead { index, offset } => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Failed to read page {index} at offset {offset}"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::WrongPageType(n) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Wrong page type: {n:02x}"))
                .with_note(format!("Page type must be 0x02, 0x05, 0x0a or 0x0d"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::WrongValueType { expected, actual } => {
                Report::build(ReportKind::Error, 1..2)
                    .with_message(format!("Wrong type, expected {expected:?}, got {actual:?}"))
                    .finish()
                    .write(source, &mut buffer)
            }
            SqliteError::TableNotFound(table) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Table {table:?} not found"))
                .finish()
                .write(source, &mut buffer),
            SqliteError::WrongCommand(_) => Report::build(ReportKind::Error, 1..2)
                .with_message(format!("Wrong command provided"))
                .with_note("todo")
                .finish()
                .write(source, &mut buffer),
            SqliteError::Parser {
                query,
                span,
                message,
            } => Report::build(ReportKind::Error, ("query", span.clone()))
                .with_config(ariadne::Config::new().with_index_type(ariadne::IndexType::Char))
                .with_message(message)
                .with_label(
                    Label::new(("query", span.clone()))
                        .with_message(message)
                        .with_color(Color::Red),
                )
                .finish()
                .write(("query", Source::from(query)), &mut buffer),
        }
        .map_err(|_| std::fmt::Error)?;

        let str = std::str::from_utf8(&buffer).map_err(|_| std::fmt::Error)?;
        f.write_str(str)
    }
}

impl Debug for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f)
    }
}

impl Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format(f)
    }
}

pub type Result<T> = std::result::Result<T, SqliteError>;

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

#[derive(Debug)]
pub enum Value<'src> {
    Null,
    Integer(u64),
    Float(f64),
    Text(&'src str),
    Blob(&'src [u8]),
}

#[derive(Debug)]
struct Serialized {
    pub varint: Varint,
    pub data: Vec<u8>,
}

impl<'src> Value<'src> {
    pub fn read(data: &'src [u8], varint: &Varint) -> Result<Self> {
        match varint.value {
            0 => Ok(Value::Null),
            n @ 1..=4 => {
                let mut number = 0;
                for i in 0..n {
                    number = (number << 8) + data[i] as u64;
                }
                Ok(Value::Integer(number))
            }
            8 => Ok(Value::Integer(0)),
            9 => Ok(Value::Integer(1)),
            // 10 | 11 => Err(SqliteError::UnhandledSerial(varint.value)),
            n if n >= 12 && n % 2 == 0 => {
                let length = (n - 12) / 2;
                Ok(Value::Blob(&data[..length]))
            }
            n if n >= 13 && n % 2 == 1 => {
                let length = (n - 13) / 2;
                let string = str::from_utf8(&data[..length]).unwrap();
                Ok(Value::Text(string))
            }
            n => Err(SqliteError::UnhandledSerial(n)),
        }
    }

    fn serialize(&self) -> Serialized {
        match self {
            Value::Null => Serialized {
                varint: Varint::new(0),
                data: vec![],
            },
            Value::Integer(0) => Serialized {
                varint: Varint::new(8),
                data: vec![],
            },
            Value::Integer(1) => Serialized {
                varint: Varint::new(9),
                data: vec![],
            },
            Value::Integer(n) if *n <= 0xffff => Serialized {
                varint: Varint::new(4),
                data: u32::to_be_bytes(*n as u32).to_vec(),
            },
            // TODO: clean up this
            Value::Integer(n) => Serialized {
                varint: Varint::new(8),
                data: u64::to_be_bytes(*n).to_vec(),
            },
            Value::Float(_) => todo!(),
            Value::Text(string) => {
                let data: Vec<u8> = string.bytes().collect();
                let varint = Varint::new(data.len() * 2 + 13);

                Serialized { varint, data }
            }
            Value::Blob(blob) => {
                let data = blob.to_vec();
                let varint = Varint::new(data.len() * 2 + 12);

                Serialized { varint, data }
            }
        }
    }

    pub fn r#type(&self) -> Type {
        match self {
            Value::Null => Type::Null,
            Value::Integer(_) => Type::Integer,
            Value::Float(_) => Type::Float,
            Value::Text(_) => Type::Text,
            Value::Blob(_) => Type::Blob,
        }
    }
}

impl Display for Value<'_> {
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

#[derive(Debug, PartialEq, Clone)]
pub struct Schema(Vec<SchemaRow>);

impl Schema {
    fn new(sr: Vec<SchemaRow>) -> Self {
        Self(sr)
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new(vec![])
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SchemaRow {
    column: Column,
    r#type: Type,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Column {
    Single(String),
    Dotted { table: String, column: String },
}

impl std::convert::From<&str> for Column {
    fn from(value: &str) -> Self {
        if let Some((table, column)) = value.split_once('.') {
            Self::Dotted {
                table: table.to_string(),
                column: column.to_string(),
            }
        } else {
            Self::Single(value.to_string())
        }
    }
}

impl Column {
    pub fn name(&self) -> &str {
        match self {
            Self::Single(name) => name,
            Self::Dotted { column, .. } => column,
        }
    }
}

pub enum Table<'db> {
    BTreePage(BTreePage<'db>),
    View(View<'db>),
}

impl<'db> Table<'db> {
    pub fn rows(&mut self) -> Rows<'_, 'db> {
        match self {
            Table::BTreePage(btreepage) => btreepage.rows(),
            Table::View(view) => view.rows(),
        }
    }

    pub fn count(&mut self) -> usize {
        match self {
            Table::BTreePage(btreepage) => btreepage.count(),
            Table::View(view) => view.count(),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Table::BTreePage(btreepage) => btreepage.schema(),
            Table::View(view) => view.schema(),
        }
    }
}

pub trait Iterator {
    fn current(&self) -> Option<Row<'_>>;
    fn advance(&mut self);

    fn next(&mut self) -> Option<Row<'_>> {
        self.advance();
        self.current()
    }
}

pub enum Rows<'rows, 'db> {
    BTreePage(BTreeRows<'rows, 'db>),
    View(ViewRows<'rows, 'db>),
}

impl Iterator for Rows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        match self {
            Rows::BTreePage(btreerows) => btreerows.current(),
            Rows::View(viewrows) => viewrows.current(),
        }
    }

    fn advance(&mut self) {
        match self {
            Rows::BTreePage(btreerows) => btreerows.advance(),
            Rows::View(viewrows) => viewrows.advance(),
        }
    }
}

pub enum Row<'page> {
    Cell(Cell<'page>),
    View(ViewRow<'page>),
}

impl<'page> Row<'page> {
    fn get(&self, column: Column) -> Result<Value<'page>> {
        match self {
            Row::Cell(cell) => cell.get(column),
            Row::View(view) => view.get(column),
        }
    }
}

#[derive(Debug)]
struct Header {
    page_size: usize,
}

impl Header {
    fn read(data: &[u8]) -> Self {
        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        Self { page_size }
    }
}

#[derive(Debug)]
pub struct Sqlite {
    file: File,
    header: Header,
}

impl Sqlite {
    pub fn read(path: &str) -> Result<Self> {
        let file = File::open(path).map_err(|_| SqliteError::FileNotFound(path.into()))?;

        let mut buf = vec![0; 100];
        (&file)
            .read_exact(&mut buf)
            .map_err(|_| SqliteError::FileNotFound(path.into()))?;
        let header = Header::read(&buf);

        Ok(Self { file, header })
    }

    pub fn page_size(&self) -> usize {
        self.header.page_size
    }

    pub fn root(&self) -> Result<Table<'_>> {
        let schema = Schema::new(vec![
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
        ]);

        let btreepage = BTreePage::read(self, 1, schema, Some("root".into()))?;
        Ok(Table::BTreePage(btreepage))
    }

    pub fn show_tables(&self) -> Result<()> {
        let mut root = self.root()?;
        let mut rows = root.rows();

        while let Some(row) = rows.next() {
            let name = match row.get("name".into())? {
                Value::Text(s) => s,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
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
        let mut rows = table.rows();

        while let Some(table) = rows.next() {
            let name = match table.get("name".into())? {
                Value::Text(s) => s,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
            };

            if name == "sqlite_sequence" || name.starts_with("sqlite_autoindex") {
                continue;
            }

            println!("schema {name:?}");

            let sql = match table.get("sql".into())? {
                Value::Text(s) => s,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
            };

            println!("{sql}");
        }

        Ok(())
    }

    fn page(&self, index: usize) -> Result<Page> {
        let offset = (index - 1) * self.header.page_size;

        (&self.file)
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|_| SqliteError::PageRead { index, offset })?;
        let mut data = vec![0; self.header.page_size];
        (&self.file)
            .read_exact(&mut data)
            .map_err(|_| SqliteError::PageRead { index, offset })?;

        let header_file_size = match index - 1 {
            0 => NonZeroUsize::new(100),
            _ => None,
        };

        Ok(Page {
            data,
            header_file_size,
        })
    }

    fn find(&self, name: &str) -> Result<(usize, Query)> {
        let mut root = self.root()?;
        let mut rows = root.rows();

        while let Some(row) = rows.next() {
            let tbl_name = match row.get("name".into())? {
                Value::Text(s) => s,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
            };

            if name != tbl_name {
                continue;
            }

            let rootpage = match row.get("rootpage".into())? {
                Value::Integer(r) => r,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
            };

            let sql = match row.get("sql".into())? {
                Value::Text(s) => s,
                other => {
                    return Err(SqliteError::WrongValueType {
                        expected: Type::Text,
                        actual: other.r#type(),
                    });
                }
            };

            let query = Query::parse(&sql.to_lowercase())?;

            return Ok((rootpage as usize, query));
        }

        Err(SqliteError::TableNotFound(name.to_string()))
    }

    pub fn table(&self, name: &str) -> Result<Table<'_>> {
        let (rootpage, query) = self.find(name)?;

        match query {
            Query::CreateTable(CreateTableStatement { schema, .. }) => {
                BTreePage::read(self, rootpage, schema, Some(name.to_string()))
                    .map(Table::BTreePage)
            }
            Query::CreateIndex(create_index_statement) => todo!(),
            _ => Err(SqliteError::WrongCommand(query)),
        }
    }

    fn query_builder(&self, select: SelectStatement) -> Result<Table<'_>> {
        let SelectStatement {
            select_clause,
            from_clause,
            where_clause,
        } = select;

        let table = match from_clause {
            From::Table { table, alias } => {
                let table = self.table(&table)?;

                let Some(alias) = alias else {
                    return Ok(table);
                };

                match table {
                    Table::BTreePage(btreepage) => {
                        let btreepage = btreepage.add_alias(alias);
                        Table::BTreePage(btreepage)
                    }
                    _ => unreachable!(),
                }
            }
            From::Subquery { query, alias } => todo!(),
            From::Join { left, right, on } => todo!(),
        };

        if select_clause == vec![Select::Wildcard] {
            Ok(table)
        } else {
            let view = View::new(select_clause, table);
            Ok(Table::View(view))
        }

        // match select {
        //     SelectStatement {
        //         select_clause,
        //         from_clause: From::Table { table, .. },
        //         where_clause: None,
        //     } if select_clause == vec![Select::Wildcard] => self.table(&table),
        //     SelectStatement {
        //         select_clause,
        //         from_clause: From::Table { table, alias },
        //         where_clause: None,
        //     } => {
        //         let inner = SelectStatement {
        //             select_clause: vec![Select::Wildcard],
        //             from_clause: From::Table {
        //                 table: table.clone(),
        //                 alias: None,
        //             },
        //             where_clause: None,
        //         };

        //         let mut names = vec![table];
        //         names.extend(alias.into_iter());

        //         let inner = Inner {
        //             table: self.query_builder(inner)?,
        //             names,
        //         };

        //         let view = View::new(select_clause, inner);
        //         Ok(Table::View(view))
        //     }
        //     _ => todo!(),
        // }
    }

    pub fn execute(&self, query: Query) -> Result<Table<'_>> {
        match query {
            Query::Select(select_statement) => self.query_builder(select_statement),
            Query::CreateTable(create_table_statement) => todo!(),
            Query::CreateIndex(create_index_statement) => todo!(),
        }
    }
}
