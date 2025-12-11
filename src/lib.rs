#![feature(trait_alias)]

pub mod display;
pub mod parser;
pub mod tables;

use crate::{
    parser::{
        CreateIndexStatement, Expression, From, Query, Select, SelectStatement, Spanned,
        WhereStatement,
    },
    tables::{
        btreepage::{BTreePage, BTreePageBuilder, BTreeRows, Cell, Page, Varint},
        groupby::GroupBy,
        indexed::{Indexed, IndexedRows},
        join::{Join, JoinRow, JoinRows},
        limit::{Limit, LimitRows},
        view::{View, ViewRow, ViewRows},
        r#where::{Where, WhereRows, write_stmt},
    },
};
use ariadne::{Color, Label, Report, ReportKind};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    ops::Range,
};

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    FileNotFound(String),
    DuplicateColumn(Vec<Spanned<Column>>),
    ColumnNotFound(Column), // TODO: could add spanned here
    UnhandledSerial(usize),
    PageRead { index: usize, offset: usize },
    WrongPageType(u8),
    PageFull,
    WrongValueType { expected: Type, actual: Type },
    TableNotFound(String),
    WrongCommand(Spanned<Query>),
    Parser(String),
}

#[derive(Debug)]
pub struct SqliteError {
    kind: ErrorKind,
    span: Range<usize>,
}

impl SqliteError {
    pub fn new(kind: ErrorKind, span: Range<usize>) -> Self {
        Self { kind, span }
    }

    pub fn write<'src, C: ariadne::Cache<&'src str>>(self, cache: C, writer: impl Write) {
        let report = Report::build(ReportKind::Error, ("query", self.span.clone()));

        let label = |msg: String| {
            Label::new(("query", self.span))
                .with_message(msg)
                .with_color(Color::Red)
        };

        let report = match self.kind {
            ErrorKind::FileNotFound(file) => {
                report.with_message(format!("File {file:?} not found"))
            }
            ErrorKind::DuplicateColumn(columns) => {
                let report = report.with_message("Duplicate columns");

                if columns.is_empty() {
                    report.with_label(label("duplicated".into()))
                } else {
                    let labels = columns.into_iter().map(|col| {
                        Label::new(("query", col.span))
                            .with_message("here")
                            .with_color(Color::Red)
                    });

                    report.with_labels(labels)
                }
            }
            ErrorKind::ColumnNotFound(column) => report
                .with_message("Not found")
                .with_label(label(format!("Column \"{column}\" not found"))),
            ErrorKind::UnhandledSerial(n) => report.with_message(format!("Wrong serial: {n}")),
            ErrorKind::PageRead { index, offset } => {
                report.with_message(format!("Failed to read page {index} at offset {offset}"))
            }
            ErrorKind::WrongPageType(n) => report
                .with_message(format!("Wrong page type: {n:02x}"))
                .with_note("Page type must be 0x02, 0x05, 0x0a or 0x0d"),
            ErrorKind::PageFull => report.with_message("Data cannot fit in the current page"),
            ErrorKind::WrongValueType { expected, actual } => {
                report.with_message(format!("Wrong type, expected {expected:?}, got {actual:?}"))
            }
            ErrorKind::TableNotFound(table) => report
                .with_message("Table not found")
                .with_label(label(format!("Table {table:?} not found"))),
            ErrorKind::WrongCommand(_) => report
                .with_message("Wrong command provided")
                .with_note("This error indicates an unsupported or malformed SQL command"),
            ErrorKind::Parser(message) => report
                .with_message("Parser error")
                .with_label(label(message.clone())),
        };

        report.finish().write(cache, writer).unwrap();
    }
}

impl Into<SqliteError> for ErrorKind {
    fn into(self) -> SqliteError {
        SqliteError::new(self, 0..0)
    }
}

pub type Result<T> = std::result::Result<T, SqliteError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value<'src> {
    Null,
    Integer(u64),
    Float(f64),
    Text(&'src str),
    Blob(&'src [u8]),
}

impl Eq for Value<'_> {}

impl PartialOrd for Value<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value<'_> {
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

#[derive(Debug)]
struct Serialized {
    pub varint: Varint,
    pub data: Vec<u8>,
}

impl<'src> Value<'src> {
    pub fn read(data: &'src [u8], varint: &Varint) -> Result<Self> {
        match varint.value {
            0 => Ok(Value::Null),
            n @ 1..=6 => {
                let length = [1, 2, 3, 4, 6, 8];
                let mut number = 0;
                for value in data.iter().take(length[n - 1]) {
                    number = (number << 8) + *value as u64;
                }
                Ok(Value::Integer(number))
            }
            8 => Ok(Value::Integer(0)),
            9 => Ok(Value::Integer(1)),
            n if n >= 12 && n.is_multiple_of(2) => {
                let length = (n - 12) / 2;
                Ok(Value::Blob(&data[..length]))
            }
            n if n >= 13 && !n.is_multiple_of(2) => {
                let length = (n - 13) / 2;
                let string = str::from_utf8(&data[..length]).unwrap();
                Ok(Value::Text(string))
            }
            n => Err(ErrorKind::UnhandledSerial(n).into()),
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
            Value::Integer(n) => Serialized {
                varint: Varint::new(6),
                data: u64::to_be_bytes(*n).to_vec(),
            },
            Value::Float(_) => {
                // TODO: Implement serialization for Float values
                panic!("Float serialization not yet implemented");
            }
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

#[derive(Debug, PartialEq, Clone, Default)]
pub struct Schema {
    pub names: Vec<String>,
    pub name: Option<usize>,
    pub columns: Vec<SchemaRow>,
    pub primary: Vec<usize>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SchemaRow {
    column: Spanned<Column>,
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

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Single(column) => write!(f, "{column}"),
            Column::Dotted { table, column } => write!(f, "{table}.{column}"),
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
    Indexed(Indexed<'db>),
    View(View<'db>),
    Join(Join<'db>),
    Where(Where<'db>),
    Groupby(GroupBy<'db>),
    Limit(Limit<'db>),
}

impl Debug for Table<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match f.alternate() {
            true => self.write_indented(f, 8, 0),
            false => self.write_normal(f),
        }
    }
}

impl<'db> Table<'db> {
    pub fn rows(&mut self) -> Rows<'_, 'db> {
        match self {
            Table::BTreePage(btreepage) => btreepage.rows(),
            Table::Indexed(indexed) => indexed.rows(),
            Table::View(view) => view.rows(),
            Table::Join(join) => join.rows(),
            Table::Where(r#where) => r#where.rows(),
            Table::Groupby(groupby) => groupby.rows(),
            Table::Limit(limit) => limit.rows(),
        }
    }

    pub fn count(&mut self) -> usize {
        match self {
            Table::BTreePage(btreepage) => btreepage.count(),
            Table::View(view) => view.inner.count(),
            table => {
                let mut rows = table.rows();
                let mut count = 0;
                while rows.next().is_some() {
                    count += 1;
                }
                count
            }
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Table::BTreePage(btreepage) => btreepage.schema(),
            Table::Indexed(indexed) => indexed.table.schema(),
            Table::View(view) => view.schema(),
            Table::Join(join) => join.schema(),
            Table::Where(r#where) => r#where.inner.schema(),
            Table::Groupby(groupby) => groupby.schema(),
            Table::Limit(limit) => limit.inner.schema(),
        }
    }

    fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        match self {
            Table::BTreePage(btreepage) => btreepage.write_indented(f, width, indent),
            Table::Indexed(indexed) => indexed.write_indented(f, width, indent),
            Table::View(view) => view.write_indented(f, width, indent),
            Table::Join(join) => join.write_indented(f, width, indent),
            Table::Where(r#where) => r#where.write_indented(f, width, indent),
            Table::Groupby(groupby) => groupby.write_indented(f, width, indent),
            Table::Limit(limit) => limit.write_indented(f, width, indent),
        }
    }

    fn write_normal(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let schema = self.schema();
        let name = schema.names.iter().max_by_key(|s| s.len()).unwrap();

        match self {
            Table::BTreePage(_) => writeln!(f, "select * from {name}"),
            Table::Indexed(indexed) => {
                let table = indexed.table.schema().names.join(", ");
                let index = indexed.index.schema().names.join(", ");

                writeln!(f, "select * from {table} using index {index}")
            }
            Table::View(view) => {
                let rows = view.fmt_rows().join(", ");

                write!(f, "(select {rows} from ")?;
                view.inner.write_normal(f)?;
                write!(f, ")")?;
                Ok(())
            }
            Table::Join(join) => {
                write!(f, "(")?;
                join.left.write_normal(f)?;
                write!(f, ") join (")?;
                join.right.write_normal(f)?;
                write!(f, ")")?;
                Ok(())
            }
            Table::Where(r#where) => {
                r#where.inner.write_normal(f)?;
                write!(f, " where ")?;
                write_stmt(&r#where.r#where, f)?;
                Ok(())
            }
            Table::Groupby(groupby) => {
                // ...
                write!(f, " group by ...")?;
                Ok(())
            }
            Table::Limit(limit) => {
                write!(f, "(")?;
                limit.inner.write_normal(f)?;
                write!(f, ") limit {}", limit.limit)?;
                Ok(())
            }
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
    Indexed(IndexedRows<'rows, 'db>),
    View(ViewRows<'rows, 'db>),
    Join(JoinRows<'rows, 'db>),
    Where(WhereRows<'rows, 'db>),
    Limit(LimitRows<'rows, 'db>),
}

impl Iterator for Rows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        match self {
            Rows::BTreePage(btreerows) => btreerows.current(),
            Rows::Indexed(indexed) => indexed.current(),
            Rows::View(view) => view.current(),
            Rows::Join(join) => join.current(),
            Rows::Where(r#where) => r#where.current(),
            Rows::Limit(limit) => limit.current(),
        }
    }

    fn advance(&mut self) {
        match self {
            Rows::BTreePage(btreerows) => btreerows.advance(),
            Rows::Indexed(indexed) => indexed.advance(),
            Rows::View(view) => view.advance(),
            Rows::Join(join) => join.advance(),
            Rows::Where(r#where) => r#where.advance(),
            Rows::Limit(limit) => limit.advance(),
        }
    }
}

pub enum Row<'page> {
    Cell(Cell<'page>),
    View(ViewRow<'page>),
    Join(JoinRow<'page>),
}

impl<'page> Row<'page> {
    pub fn get(&self, column: Column) -> Result<Value<'page>> {
        match self {
            Row::Cell(cell) => cell.get(column),
            Row::View(view) => view.get(column),
            Row::Join(join) => join.get(column),
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
        let file = File::open(path).map_err(|_| ErrorKind::FileNotFound(path.into()).into())?;

        let mut buf = vec![0; 100];
        (&file)
            .read_exact(&mut buf)
            .map_err(|_| ErrorKind::FileNotFound(path.into()).into())?;

        let header = Header::read(&buf);

        Ok(Self { file, header })
    }

    pub fn page_size(&self) -> usize {
        self.header.page_size
    }

    pub fn root(&self) -> Result<Table<'_>> {
        let schema = Schema {
            names: vec!["root".into()],
            name: Some(0),
            columns: vec![
                SchemaRow {
                    column: Spanned::empty("type".into()),
                    r#type: Type::Text,
                },
                SchemaRow {
                    column: Spanned::empty("name".into()),
                    r#type: Type::Text,
                },
                SchemaRow {
                    column: Spanned::empty("tbl_name".into()),
                    r#type: Type::Text,
                },
                SchemaRow {
                    column: Spanned::empty("rootpage".into()),
                    r#type: Type::Integer,
                },
                SchemaRow {
                    column: Spanned::empty("sql".into()),
                    r#type: Type::Text,
                },
            ],
            primary: vec![],
        };

        let btreepage = BTreePage::read(self, 1, schema)?;
        Ok(Table::BTreePage(btreepage))
    }

    pub fn show_tables(&self) -> Result<()> {
        let mut root = self.root()?;
        let mut rows = root.rows();

        while let Some(row) = rows.next() {
            let name = self.extract_text_value(&row, "name".into())?;
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
            let name = self.extract_text_value(&table, "name".into())?;
            if name == "sqlite_sequence" || name.starts_with("sqlite_autoindex") {
                continue;
            }

            println!("schema {name:?}");

            let sql = self.extract_text_value(&table, "sql".into())?;
            println!("{sql}");
        }

        Ok(())
    }

    fn page(&self, index: usize) -> Result<Page> {
        let offset = (index - 1) * self.header.page_size;

        (&self.file)
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|_| ErrorKind::PageRead { index, offset }.into())?;
        let mut data = vec![0; self.header.page_size];
        (&self.file)
            .read_exact(&mut data)
            .map_err(|_| ErrorKind::PageRead { index, offset }.into())?;

        let header_file_size = match index - 1 {
            0 => NonZeroUsize::new(100),
            _ => None,
        };

        Ok(Page {
            data,
            header_file_size,
        })
    }

    fn extract_text_value<'a>(&self, row: &Row<'a>, column: Column) -> Result<&'a str> {
        match row.get(column)? {
            Value::Text(s) => Ok(s),
            other => Err(ErrorKind::WrongValueType {
                expected: Type::Text,
                actual: other.r#type(),
            }
            .into()),
        }
    }

    fn extract_integer_value(&self, row: &Row, column: Column) -> Result<u64> {
        match row.get(column)? {
            Value::Integer(n) => Ok(n),
            other => Err(ErrorKind::WrongValueType {
                expected: Type::Integer,
                actual: other.r#type(),
            }
            .into()),
        }
    }

    fn find<P>(&self, predicate: P) -> Result<Option<(usize, Spanned<Query>)>>
    where
        P: Fn(&str, &Query) -> bool,
    {
        let mut root = self.root()?;
        let mut rows = root.rows();

        while let Some(row) = rows.next() {
            let tbl_name = self.extract_text_value(&row, "name".into())?;
            if tbl_name == "sqlite_sequence" {
                continue;
            }

            let rootpage = self.extract_integer_value(&row, "rootpage".into())?;

            let sql = self.extract_text_value(&row, "sql".into())?;

            let query = Query::parse(&sql.to_lowercase())?;

            if !predicate(tbl_name, &query) {
                continue;
            }

            return Ok(Some((rootpage as usize, query)));
        }

        Ok(None)
    }

    pub fn table(&self, name: &str) -> Result<BTreePage<'_>> {
        let Some((rootpage, query)) = self.find(|tbl_name, _| tbl_name == name)? else {
            return Err(ErrorKind::TableNotFound(name.to_string()).into());
        };

        if let Query::CreateTable(cts) = &*query {
            BTreePage::read(self, rootpage, cts.schema.clone())
        } else {
            Err(ErrorKind::WrongCommand(query).into())
        }
    }

    fn index(&self, table_name: &str, columns: &[Column]) -> Result<Option<BTreePage<'_>>> {
        let mut root = self.root()?;
        let mut rows = root.rows();

        let mut best: Option<(usize, u64, Spanned<CreateIndexStatement>)> = None;

        while let Some(row) = rows.next() {
            let tbl_name = self.extract_text_value(&row, "name".into())?;
            if tbl_name == "sqlite_sequence" {
                continue;
            }

            let sql = self.extract_text_value(&row, "sql".into())?;
            let rootpage = self.extract_integer_value(&row, "rootpage".into())?;

            let query = Query::parse(&sql.to_lowercase())?;
            let Query::CreateIndex(index) = *query.inner else {
                continue;
            };

            if index.table != table_name {
                continue;
            }

            let mut count = 0;
            for col in index.columns.iter() {
                for c in columns {
                    if col == c.name() {
                        count += 1;
                    }
                }
            }

            best = match best {
                None if count == 0 => None,
                Some((c, ..)) if c >= count => best,
                _ => Some((count, rootpage, index)),
            };
        }

        let Some((_, rootpage, index)) = best else {
            return Ok(None);
        };

        let table = self.table(table_name)?;

        let mut schema = vec![];
        let mut primary = vec![];

        for (idx, name) in index.columns.iter().enumerate() {
            let out = table
                .schema()
                .columns
                .iter()
                .find(|sr| sr.column.name() == name);

            if let Some(sr) = out {
                schema.push(sr.clone());
                primary.push(idx);
            } else {
                return Err(ErrorKind::ColumnNotFound(name.as_str().into()).into());
            }
        }

        schema.push(SchemaRow {
            column: Spanned::empty("index".into()),
            r#type: Type::Integer, // TODO: Check if this should be Type::Blob based on SQLite spec
        });

        let schema = Schema {
            names: vec![index.name.clone()],
            name: Some(0),
            columns: schema,
            primary,
        };

        let btreepage = BTreePage::read(self, rootpage as usize, schema)?;
        Ok(Some(btreepage))
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_builder(
        &self,
        from_clause: Spanned<From>,
        where_clause: Option<&Spanned<WhereStatement>>,
    ) -> Result<Table<'_>> {
        match *from_clause.inner {
            From::Table { table, alias } => {
                let mut btreepage = self.table(&table).map_err(|e| SqliteError {
                    span: table.span.clone(),
                    ..e
                })?;

                if let Some(alias) = alias {
                    btreepage.add_alias(*alias.inner);
                }

                if let Some(r#where) = where_clause {
                    let (columns, expressions): (Vec<Column>, Vec<Expression>) =
                        r#where.index().into_iter().unzip();

                    if let Some(index) = self.index(&table, &columns)? {
                        let indexed = Indexed {
                            table: btreepage,
                            index,
                            columns,
                            expressions,
                        };

                        return Ok(Table::Indexed(indexed));
                    }
                }

                Ok(Table::BTreePage(btreepage))
            }
            From::Subquery { query: _, alias: _ } => {
                // TODO: Implement subquery support
                panic!("Subquery support not yet implemented");
            }
            From::Join { left, right, on } => {
                let left = self.from_builder(left, None)?;

                let join = if let From::Table { table, alias } = &*right
                    && let Some(on) = &on
                    && let Expression::Column(column) = &*on.right
                    && let Expression::Column(left_col) = &*on.left
                    && let Some(index) = self.index(table, &[column.clone()])?
                {
                    let mut table = self.table(table)?;
                    if let Some(alias) = alias {
                        table.add_alias(alias.inner.as_str().into());
                    }

                    let indexed = Indexed {
                        table,
                        index,
                        columns: vec![column.clone()],
                        expressions: vec![],
                    };

                    let right = Table::Indexed(indexed);
                    Table::Join(Join::indexed(left, right, left_col.clone()))
                } else {
                    let right = self.from_builder(right, None)?;
                    Table::Join(Join::new(left, right))
                };

                match on {
                    Some(comparison) => {
                        let span = comparison.span.clone();
                        let r#where = Spanned {
                            inner: Box::new(WhereStatement::Comparison(comparison)),
                            span,
                        };
                        let r#where = Where::new(join, r#where);

                        Ok(Table::Where(r#where))
                    }
                    None => Ok(join),
                }
            }
        }
    }

    fn query_builder(&self, select: Spanned<SelectStatement>) -> Result<Table<'_>> {
        let SelectStatement {
            select_clause,
            from_clause,
            where_clause,
            groupby_clause,
            limit_clause,
        } = *select.inner;

        let mut table = self.from_builder(from_clause, where_clause.as_ref())?;

        if let Some(r#where) = where_clause {
            let r#where = Where::new(table, r#where);
            table = Table::Where(r#where);
        }

        // if select_clause
        //     .iter()
        //     .any(|s| matches!(s, Select::Function { .. }))
        //     || groupby_clause.is_some()
        // {
        // panic!("groupby work in progress");

        // let groupby = GroupBy::new(select_clause, groupby_clause, table)?;
        // table = Table::Groupby(groupby);
        // } else if *select_clause != vec![Select::Wildcard] {
        if let Some(first) = select_clause.inner.first()
            && *first.inner != Select::Wildcard
        {
            let view = View::new(select_clause, table)?;
            table = Table::View(view);
        }

        if let Some(limit_stmt) = limit_clause {
            let limit = Limit::new(table, limit_stmt.limit);
            table = Table::Limit(limit);
        }

        Ok(table)
    }

    pub fn execute(&self, query: Spanned<Query>) -> Result<Table<'_>> {
        match *query.inner {
            Query::Select(select_statement) => self.query_builder(select_statement),
            Query::CreateTable(create_table_statement) => {
                println!("{:?}", create_table_statement);
                panic!("CreateTable statement execution not yet implemented");
            }
            Query::CreateIndex(_create_index_statement) => {
                panic!("CreateIndex statement execution not yet implemented");
            }
            Query::Explain(select_statement) => {
                let table = self.query_builder(select_statement)?;
                println!("{table:#?}");

                // Returns empty table
                let schema = Schema {
                    names: vec![],
                    name: None,
                    columns: vec![],
                    primary: vec![],
                };
                let btree = BTreePageBuilder::new(schema).build()?;

                Ok(Table::BTreePage(btree))
            }
            Query::Info(select_statement) => {
                let _ = self.execute(Spanned {
                    inner: Box::new(Query::Explain(select_statement.clone())),
                    span: query.span.clone(),
                })?;

                let mut table = self.execute(Spanned {
                    inner: Box::new(Query::Select(select_statement)),
                    span: query.span,
                })?;

                let mut rows = table.rows();
                let mut count = 0;
                while rows.next().is_some() {
                    count += 1;
                }

                println!("# rows: {count}");

                Ok(table)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_spanned() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select a.id from apples as a join apples as b")?;

        let table = db.execute(query)?;
        let s = table.schema();

        assert_eq!(s.columns.len(), 1);
        let col = &s.columns[0].column;
        assert_eq!(col.span, 7..11);

        Ok(())
    }
}
