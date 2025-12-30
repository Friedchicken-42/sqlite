use crate::{
    Access, Column, ErrorKind, Iterator, Result, Row, Rows, Schema, Table, Tabular, Value,
    parser::Spanned,
};

pub struct View<'table> {
    pub inner: Box<Table<'table>>,
    pub schema: Schema,
    pub inner_columns: Vec<Spanned<Column>>,
}

impl<'table> Tabular<'table> for View<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::View(ViewRows {
            rows: Box::new(self.inner.rows()),
            schema: &self.schema,
            inner: &self.inner_columns,
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
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

impl<'table> View<'table> {
    pub fn fmt_rows(&self) -> Vec<String> {
        self.schema
            .columns
            .iter()
            .map(|sr| sr.column.to_string())
            .collect::<Vec<_>>()
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

impl<'row> Access<'row> for ViewRow<'row> {
    fn get(&self, column: Column) -> Result<Value<'row>> {
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
