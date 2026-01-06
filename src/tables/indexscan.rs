use std::cmp::Ordering;

use crate::{
    Access, Column, Iterator, Row, Rows, Schema, Tabular, Value,
    parser::Expression,
    tables::btreepage::{BTreePage, BTreeRows},
};

pub struct IndexScan<'table> {
    pub table: BTreePage<'table>,
    pub columns: Vec<Column>,
    pub expressions: Vec<Expression>,
}

impl<'table> Tabular<'table> for IndexScan<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::IndexScan(IndexScanRows {
            table: BTreeRows {
                btree: &mut self.table,
                indexes: vec![],
            },
            columns: &self.columns,
            expressions: &mut self.expressions,
        })
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(", ");

        writeln!(f, "Index Scan {{ {columns} }}")?;
        write!(f, "{prefix}└─ ")?;
        self.table.write_indented(f, "")
    }

    fn schema(&self) -> &Schema {
        self.table.schema()
    }
}

pub struct IndexScanRows<'rows, 'table> {
    pub table: BTreeRows<'rows, 'table>,
    pub columns: &'rows [Column],
    pub expressions: &'rows mut [Expression],
}

impl Iterator for IndexScanRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        self.table.current()
    }

    fn advance(&mut self) {
        self.table.loop_until(|cell| {
            for (i, column) in self.columns.iter().enumerate() {
                let Ok(v) = cell.get(column.clone()) else {
                    continue;
                };

                let value = match self.expressions.get(i) {
                    Some(Expression::Literal(s)) => Value::Text(s.as_str()),
                    Some(Expression::Number(n)) => Value::Integer(*n),
                    _ => continue,
                };

                let cmp = v.cmp(&value);
                if cmp.is_ne() {
                    return cmp;
                }
            }

            Ordering::Equal
        });

        if self.table.current().is_none() {
            self.table.indexes.clear();
        }
    }
}
