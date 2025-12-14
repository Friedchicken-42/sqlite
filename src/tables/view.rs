use crate::{
    Column, ErrorKind, Iterator, Result, Row, Rows, Schema, SchemaRow, SqliteError, Table, Type,
    Value,
    parser::{Select, Spanned},
};

pub struct View<'table> {
    pub inner: Box<Table<'table>>,
    schema: Schema,
    inner_columns: Vec<Spanned<Column>>,
}

pub fn create_schema(
    select: Vec<Spanned<Select>>,
    inner: &Table,
) -> Result<(Schema, Vec<Spanned<Column>>)> {
    let schema = inner.schema();

    let srs = select
        .iter()
        .map(|row| match row.inner.as_ref() {
            Select::Wildcard => Ok(schema
                .columns
                .iter()
                .map(|sr| {
                    let span = row.span.clone();
                    let mut sr = sr.clone();
                    let column = sr.column.with_span(span.clone());
                    sr.column = column.clone();

                    (sr, column)
                })
                .collect::<Vec<_>>()),
            Select::Column { name, alias, table } => {
                let sr = schema
                    .columns
                    .iter()
                    .find(|sr| sr.column.name() == **name)
                    .ok_or(SqliteError::new(
                        ErrorKind::ColumnNotFound(name.as_str().into()),
                        row.span.clone(),
                    ))?
                    .clone();

                let column = match table {
                    Some(table) => Column::Dotted {
                        table: table.to_string(),
                        column: name.to_string(),
                    },
                    None => Column::Single(name.to_string()),
                };

                let sr = match alias {
                    Some(alias) => SchemaRow {
                        column: Spanned::span(Column::Single(alias.to_string()), row.span.clone()),
                        ..sr
                    },
                    None => SchemaRow {
                        column: Spanned::span(column.clone(), row.span.clone()),
                        ..sr
                    },
                };

                let column = Spanned::span(column, row.span.clone());

                Ok(vec![(sr, column)])
            }
            Select::Function { name, .. } => {
                let r#type = match name.as_str() {
                    "count" => Type::Integer,
                    func => {
                        return Err(SqliteError::new(
                            ErrorKind::ColumnNotFound(func.into()),
                            name.span.clone(),
                        ));
                    }
                };

                let column = Column::Single(name.as_str().into());
                let span = name.span.clone();

                Ok(vec![(
                    SchemaRow {
                        column: Spanned::span(column, span.clone()),
                        r#type,
                    },
                    Spanned::span(Column::Single(name.as_str().into()), span.clone()),
                )])
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .fold((vec![], vec![]), |(mut s, mut n), (a, b)| {
            s.push(a);
            n.push(b);
            (s, n)
        });

    Ok((
        Schema {
            names: schema.names.clone(),
            name: None,
            columns: srs.0,
            primary: vec![],
        },
        srs.1,
    ))
}

impl<'table> View<'table> {
    pub fn new(select: Spanned<Vec<Spanned<Select>>>, inner: Table<'table>) -> Result<Self> {
        let (schema, inner_columns) = create_schema(*select.inner, &inner)?;

        Ok(Self {
            inner: Box::new(inner),
            schema,
            inner_columns,
        })
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::View(ViewRows {
            rows: Box::new(self.inner.rows()),
            schema: &self.schema,
            inner: &self.inner_columns,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn fmt_rows(&self) -> Vec<String> {
        self.schema
            .columns
            .iter()
            .map(|sr| sr.column.to_string())
            .collect::<Vec<_>>()
    }

    pub fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        let rows = self
            .fmt_rows()
            .into_iter()
            .enumerate()
            .map(|(i, column)| {
                let inner = self.inner_columns[i].to_string();
                format!("{column} ({inner})")
            })
            .collect::<Vec<_>>()
            .join(", ");

        writeln!(f, "View {{ {rows} }}")?;
        self.inner.write_indented_rec(f, prefix, true)
    }
}

pub struct ViewRows<'rows, 'table> {
    rows: Box<Rows<'rows, 'table>>,
    schema: &'rows Schema,
    inner: &'rows [Spanned<Column>],
}

impl Iterator for ViewRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        let current = self.rows.current()?;
        Some(Row::View(ViewRow {
            row: Box::new(current),
            schema: self.schema,
            inner: self.inner,
        }))
    }

    fn advance(&mut self) {
        self.rows.advance();
    }
}

pub struct ViewRow<'row> {
    row: Box<Row<'row>>,
    schema: &'row Schema,
    inner: &'row [Spanned<Column>],
}

impl<'row> ViewRow<'row> {
    pub fn get(&self, column: Column) -> Result<Value<'row>> {
        let columns = self
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, sr)| match (&column, &*sr.column) {
                (Column::Single(name1), Column::Single(name2)) => name1 == name2,
                (Column::Single(name1), Column::Dotted { column: name2, .. }) => name1 == name2,
                (Column::Dotted { .. }, Column::Single(_)) => false,
                (
                    Column::Dotted {
                        table: table1,
                        column: name1,
                    },
                    Column::Dotted {
                        table: table2,
                        column: name2,
                    },
                ) => table1 == table2 && name1 == name2,
            })
            .collect::<Vec<_>>();

        match &columns[..] {
            [] => Err(ErrorKind::ColumnNotFound(column).into()),
            [(i, _)] => self.row.get(*self.inner[*i].inner.clone()),
            lst => {
                let columns = lst
                    .iter()
                    .map(|(_, sr)| sr.column.clone())
                    .collect::<Vec<_>>();
                Err(ErrorKind::DuplicateColumn(columns).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Result, Sqlite, Table, parser::Query};

    use super::*;

    #[test]
    fn view() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select id from apples")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::View(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("id".into())?;
            assert_eq!(value, Value::Integer(i));

            assert!(row.get("name".into()).is_err());
        }

        Ok(())
    }

    #[test]
    fn view_alias() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select id as row_id from apples")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::View(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("row_id".into())?;
            assert_eq!(value, Value::Integer(i));

            assert!(row.get("id".into()).is_err());
        }

        Ok(())
    }
}
