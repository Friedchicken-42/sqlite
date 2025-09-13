use std::cmp::Ordering;

use crate::{
    Column, Iterator, Row, Rows, Value,
    parser::Expression,
    tables::btreepage::{BTreePage, BTreeRows},
};

pub struct Indexed<'table> {
    pub table: BTreePage<'table>,
    pub index: BTreePage<'table>,
    pub columns: Vec<Column>,
    pub expressions: Vec<Expression>,
}

impl<'table> Indexed<'table> {
    pub fn rows(&mut self) -> Rows<'_, 'table> {
        let table = BTreeRows {
            btree: &mut self.table,
            indexes: vec![],
        };

        let index = BTreeRows {
            btree: &mut self.index,
            indexes: vec![],
        };

        Rows::Indexed(IndexedRows {
            table,
            index,
            columns: &mut self.columns,
            expressions: &mut self.expressions,
        })
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let spacer = "  ".repeat(indent);
        let table = self.table.schema().names.join(", ");
        let index = self.index.schema().names.join(", ");
        let columns = self
            .columns
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(", ");

        writeln!(
            f,
            "{:<width$} │ {}{} with index {} ({})",
            "from", spacer, table, index, columns
        )
    }

    pub fn write_normal(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let table = self.table.schema().names.join(", ");
        let index = self.index.schema().names.join(", ");

        writeln!(f, "select * from {table} using index {index}")
    }
}

pub struct IndexedRows<'rows, 'table> {
    table: BTreeRows<'rows, 'table>,
    index: BTreeRows<'rows, 'table>,
    pub columns: &'rows Vec<Column>,
    pub expressions: &'rows mut Vec<Expression>,
}

impl Iterator for IndexedRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        self.table.current()
    }

    fn advance(&mut self) {
        self.index.loop_until(|cell| {
            for (i, column) in self.columns.iter().enumerate() {
                // TODO: if Dotted { table } check table in table.name
                let column: Column = column.name().into();

                let value = match self.expressions.get(i) {
                    Some(Expression::Literal(s)) => Value::Text(s.as_str()),
                    Some(Expression::Number(n)) => Value::Integer(*n),
                    _ => continue,
                };

                let Ok(v) = cell.get(column) else {
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
