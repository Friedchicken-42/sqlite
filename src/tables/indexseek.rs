use crate::{
    Access, Iterator, Row, Rows, Table, Tabular, Value,
    tables::btreepage::{BTreePage, BTreeRows},
};

pub struct IndexSeek<'table> {
    pub table: BTreePage<'table>,
    pub index: Box<Table<'table>>,
}

impl<'table> Tabular<'table> for IndexSeek<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        let table = BTreeRows {
            btree: &mut self.table,
            indexes: vec![],
        };
        let index = Box::new(self.index.rows());

        Rows::IndexSeek(IndexSeekRows { table, index })
    }

    fn schema(&self) -> &crate::Schema {
        self.table.schema()
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        writeln!(f, "Index Seek")?;

        write!(f, "{prefix}├─ ")?;
        self.table.write_indented(f, prefix)?;
        self.index.write_indented_rec(f, prefix, true)?;

        Ok(())
    }
}

pub struct IndexSeekRows<'rows, 'table> {
    table: BTreeRows<'rows, 'table>,
    index: Box<Rows<'rows, 'table>>,
}

impl Iterator for IndexSeekRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        self.table.current()
    }

    fn advance(&mut self) {
        self.index.advance();

        let Some(row) = self.index.current() else {
            self.table.indexes.clear();
            return;
        };

        // let blob = match row.get("index".into()).unwrap() {
        //     Value::Blob(b) => b,
        //     other => panic!("expected Blob on \"index\", got {:?}", other.r#type()),
        // };
        // let rowid = blob.iter().fold(0, |acc, x| (acc << 8) + (*x as usize));

        let rowid = match row.get("index".into()).unwrap() {
            Value::Integer(b) => b as usize,
            other => panic!("expected Blob on \"index\", got {:?}", other.r#type()),
        };

        self.table.loop_until(|cell| {
            let row = cell.rowid().unwrap();
            row.cmp(&rowid)
        });
    }
}
