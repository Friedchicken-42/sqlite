use crate::{Iterator, Row, Rows, Table, Tabular};

pub struct Limit<'table> {
    pub inner: Box<Table<'table>>,
    pub limit: usize,
    pub offset: usize,
}

impl<'table> Tabular<'table> for Limit<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        let mut rows = Rows::Limit(LimitRows {
            rows: Box::new(self.inner.rows()),
            count: 0,
            limit: self.limit,
        });

        for _ in 0..self.offset {
            rows.advance();
            if rows.current().is_none() {
                break;
            }
        }

        rows
    }

    fn schema(&self) -> &crate::Schema {
        self.inner.schema()
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        if self.limit != 0 {
            write!(f, "Limit {} ", self.limit)?;
        }
        if self.offset != 0 {
            write!(f, "Skip {}", self.offset)?;
        }
        writeln!(f)?;

        self.inner.write_indented_rec(f, prefix, true)
    }
}

impl<'table> Limit<'table> {
    pub fn new(inner: Table<'table>, limit: usize, offset: usize) -> Self {
        Self {
            inner: Box::new(inner),
            limit,
            offset,
        }
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
    use crate::{Access, Result, Sqlite, Table, Value, parser::Query};

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
