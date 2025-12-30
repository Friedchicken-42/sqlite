use std::cmp::Ordering;

use crate::{
    Access, Column, Iterator, Row, Rows, Tabular, Value,
    parser::Expression,
    tables::btreepage::{BTreePage, BTreeRows},
};

pub struct IndexSeek<'table> {
    pub table: BTreePage<'table>,
    pub index: BTreePage<'table>,
    pub columns: Vec<Column>,
    pub expressions: Vec<Expression>,
}

impl<'table> Tabular<'table> for IndexSeek<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        let table = BTreeRows {
            btree: &mut self.table,
            indexes: vec![],
        };

        let index = BTreeRows {
            btree: &mut self.index,
            indexes: vec![],
        };

        Rows::IndexSeek(IndexSeekRows {
            table,
            index,
            columns: &mut self.columns,
            expressions: &mut self.expressions,
        })
    }

    fn schema(&self) -> &crate::Schema {
        self.table.schema()
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(", ");

        writeln!(f, "Index Scan {{ {columns} }}")?;

        write!(f, "{prefix}├─")?;
        self.table.write_indented(f, "")?;
        write!(f, "{prefix}└─")?;
        self.index.write_indented(f, "")?;

        Ok(())
    }
}

pub struct IndexSeekRows<'rows, 'table> {
    table: BTreeRows<'rows, 'table>,
    index: BTreeRows<'rows, 'table>,
    pub columns: &'rows Vec<Column>,
    pub expressions: &'rows mut Vec<Expression>,
}

impl Iterator for IndexSeekRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        self.table.current()
    }

    fn advance(&mut self) {
        self.index.loop_until(|cell| {
            for (i, column) in self.columns.iter().enumerate() {
                let column = match column {
                    Column::Dotted { table, column }
                        if self.table.btree.schema().names.contains(table) =>
                    {
                        &Column::Single(column.to_string())
                    }
                    Column::Dotted { .. } => continue,
                    single => single,
                };

                let value = match self.expressions.get(i) {
                    Some(Expression::Literal(s)) => Value::Text(s.as_str()),
                    Some(Expression::Number(n)) => Value::Integer(*n),
                    _ => continue,
                };

                let Ok(v) = cell.get(column.clone()) else {
                    continue;
                };

                let cmp = v.cmp(&value);

                if cmp.is_ne() {
                    return cmp;
                }
            }
            Ordering::Equal
        });

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
