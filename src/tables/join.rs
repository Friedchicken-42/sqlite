use crate::{
    Access, Column, ErrorKind, Iterator, Result, Row, Rows, Schema, Table, Tabular, Value,
    parser::{Comparison, Spanned},
};

pub struct Join<'table> {
    pub left: Box<Table<'table>>,
    pub right: Box<Table<'table>>,
    pub on: Option<Spanned<Comparison>>,
    pub schema: Schema,
}

impl<'table> Tabular<'table> for Join<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::Join(JoinRows {
            left: Box::new(self.left.rows()),
            right: Box::new(self.right.rows()),
            on: self.on.as_ref(),
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        write!(f, "Join")?;
        if let Some(comparison) = &self.on {
            writeln!(f, " on {}", *comparison.inner)?;
        } else {
            writeln!(f, " full")?;
        }

        self.left.write_indented_rec(f, prefix, false)?;
        self.right.write_indented_rec(f, prefix, true)?;

        Ok(())
    }
}

pub struct JoinRows<'rows, 'table> {
    left: Box<Rows<'rows, 'table>>,
    right: Box<Rows<'rows, 'table>>,
    on: Option<&'rows Spanned<Comparison>>,
}

impl Iterator for JoinRows<'_, '_> {
    fn current(&self) -> Option<crate::Row<'_>> {
        if let Some(left) = self.left.current()
            && let Some(right) = self.right.current()
        {
            Some(Row::Join(JoinRow {
                left: Box::new(left),
                right: Box::new(right),
            }))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        loop {
            if self.left.current().is_none() && self.right.current().is_none() {
                self.left.advance();
                self.right.advance();
            } else {
                self.right.advance();

                if self.right.current().is_none() {
                    self.left.advance();

                    if self.left.current().is_none() {
                        return;
                    }

                    self.right.advance();
                }
            }

            if let Some(comparison) = self.on {
                if let Some(row) = self.current()
                    && comparison.matches(&row)
                {
                    return;
                }
            } else {
                return;
            }
        }
    }
}

pub struct JoinRow<'row> {
    left: Box<Row<'row>>,
    right: Box<Row<'row>>,
}

impl<'row> Access<'row> for JoinRow<'row> {
    fn get(&self, column: Column) -> Result<Value<'row>> {
        match (
            self.left.get(column.clone()),
            self.right.get(column.clone()),
        ) {
            (Err(_), Err(_)) => Err(ErrorKind::ColumnNotFound(column).into()),
            (Ok(_), Ok(_)) => Err(ErrorKind::DuplicateColumn(vec![]).into()),
            (Ok(v), Err(_)) | (Err(_), Ok(v)) => Ok(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Result, Sqlite, Table, parser::Query};

    use super::*;

    #[test]
    fn join() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select * from apples join oranges")?;
        let mut table = db.execute(query)?;

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            let value = row.get("apples.id".into())?;
            assert_eq!(value, Value::Integer(i / 6 + 1));

            let value = row.get("oranges.id".into())?;
            assert_eq!(value, Value::Integer(i % 6 + 1));
            i += 1;
        }

        Ok(())
    }

    #[test]
    fn join_indexed() -> Result<()> {
        let db = Sqlite::read("other.db")?;
        let query = Query::parse("select * from apples as a join apples as b on a.name = b.name")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::Where(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            let a = row.get("a.name".into())?;
            let b = row.get("b.name".into())?;
            assert_eq!(a, b);
            i += 1;
        }

        assert_eq!(i, 7);

        Ok(())
    }

    #[test]
    fn join_select_star_spans() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select * from apples as a join oranges as o")?;
        let table = db.execute(query)?;
        let schema = table.schema();

        println!("Schema columns count: {}", schema.columns.len());

        for (i, col) in schema.columns.iter().enumerate() {
            println!(
                "Column {}: {} with span {:?}",
                i, col.column.inner, col.column.span
            );
        }

        assert!(schema.columns.len() > 0);

        Ok(())
    }
}
