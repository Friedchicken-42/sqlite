use crate::{Iterator, Row, Rows, Table};

pub struct Limit<'table> {
    pub inner: Box<Table<'table>>,
    pub limit: usize,
}

impl<'table> Limit<'table> {
    pub fn new(inner: Table<'table>, limit: usize) -> Self {
        Self {
            inner: Box::new(inner),
            limit,
        }
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::Limit(LimitRows {
            rows: Box::new(self.inner.rows()),
            count: 0,
            limit: self.limit,
        })
    }

    pub fn count(&mut self) -> usize {
        self.inner.count()
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let spacer = "  ".repeat(indent);

        self.inner.write_indented(f, width, indent + 1)?;
        writeln!(f, "{:<width$} │ {}{}", "limit", spacer, self.limit)?;

        Ok(())
    }
}

pub struct LimitRows<'rows, 'table> {
    rows: Box<Rows<'rows, 'table>>,
    count: usize,
    limit: usize,
}

impl Iterator for LimitRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        match self.count > self.limit {
            true => None,
            false => self.rows.current(),
        }
    }

    fn advance(&mut self) {
        self.rows.advance();
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::{Result, Sqlite, Table, Value, parser::Query};

    use super::*;

    #[test]
    fn limit() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select * from apples limit 2")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::Limit(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("id".into())?;
            assert_eq!(value, Value::Integer(i));
        }

        assert_eq!(i, 2);

        Ok(())
    }
}
