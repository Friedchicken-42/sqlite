use std::cmp::Ordering;

use crate::{
    Access, Column, Iterator, Row, Rows, Schema, Serialized, Table, Tabular, Value,
    tables::{btreepage::Cell, join::JoinRow},
};

pub struct MergeJoin<'table> {
    pub left: Box<Table<'table>>,
    pub right: Box<Table<'table>>,
    pub left_col: Column,
    pub right_col: Column,
    pub schema: Schema,
}

impl<'table> Tabular<'table> for MergeJoin<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::MergeJoin(MergeJoinRows {
            left: Box::new(self.left.rows()),
            right: Box::new(self.right.rows()),
            left_col: &self.left_col,
            right_col: &self.right_col,
            buffer: vec![],
            index: 0,
            schema: &self.schema,
        })
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        writeln!(f, "MergeJoin {{ {} {} }}", self.left_col, self.right_col)?;
        self.left.write_indented_rec(f, prefix, false)?;
        self.right.write_indented_rec(f, prefix, true)?;

        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct MergeJoinRows<'rows, 'table> {
    left: Box<Rows<'rows, 'table>>,
    right: Box<Rows<'rows, 'table>>,
    left_col: &'rows Column,
    right_col: &'rows Column,
    buffer: Vec<Vec<Serialized>>,
    index: usize,
    schema: &'rows Schema,
}

impl Iterator for MergeJoinRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        if let Some(left) = self.left.current()
            && self.index < self.buffer.len()
        {
            // get left.current() + buffer[i]
            todo!()
        } else if let (Some(left), Some(right)) = (self.left.current(), self.right.current()) {
            Some(Row::Join(JoinRow {
                left: Box::new(left),
                right: Box::new(right),
            }))
        } else {
            None
        }
        // if let Some(left) = self.left.current()
        //     && let Some(right) = self.right.current()
        // {
        //     Some(Row::Join(JoinRow {
        //         left: Box::new(left),
        //         right: Box::new(right),
        //     }))
        // } else {
        //     None
        // }
    }

    fn advance(&mut self) {
        println!("advance");

        loop {
            if self.left.current().is_none() && self.right.current().is_none() {
                self.left.advance();
                self.right.advance();
            }

            let (Some(left_row), Some(right_row)) = (self.left.current(), self.right.current())
            else {
                return;
            };

            let (Ok(left_value), Ok(right_value)) = (
                left_row.get(self.left_col.clone()),
                right_row.get(self.right_col.clone()),
            ) else {
                panic!("expected both rows to contains the column");
            };
            println!("{left_value:?} - {right_value:?}");

            match left_value.cmp(&right_value) {
                Ordering::Less => {
                    self.left.advance();
                    continue;
                }
                Ordering::Equal => {}
                Ordering::Greater => {
                    self.right.advance();
                    continue;
                }
            }

            while let Some(right_row) = self.right.current()
                && let Ok(right_value) = right_row.get(self.right_col.clone())
                && left_value.cmp(&right_value).is_eq()
            {
                self.right.advance();
            }

            /*
            while left_value.cmp(&right_value).is_eq() {
                for sr in &self.schema.columns {
                    match right_row.get(*sr.column.inner.clone()) {
                        Ok(_) => {}
                        Err(_) => {}
                    };
                }

                let Some(rw) = self.right.next() else {
                    break;
                };

                let Ok(rv) = rw.get(self.right_col.clone()) else {
                    panic!("expected right to contains the column");
                };

                right_value = rv;
            }
            */
        }

        // 1) init
        // 2) store right
        // 3) cmp left & right
        //   <) inc left
        //   =) break
        //   >) inc right
        //
        // if equals:
        // 1) loop until not eq
        //    store right in buffer -> break
        // 2) index >= buffer.len() -> index = 0; inc left
        // 3) index < buffer.len() -> index += 1;

        /*
        loop {
            if self.left.current().is_none() && self.right.current().is_none() {
                self.left.advance();
                self.right.advance();
            } else if self.index < self.buffer.len() {
                // self.index += 1;
                return;
            } else {
                let (Some(left_row), Some(right_row)) = (self.left.current(), self.right.current())
                else {
                    return;
                };

                let (Ok(left_value), Ok(right_value)) = (
                    left_row.get(self.left_col.clone()),
                    right_row.get(self.right_col.clone()),
                ) else {
                    panic!("expected both rows to contains the column");
                };
                println!("{left_value:?} - {right_value:?}");

                while let Some(right_row) = self.right.next()
                    && let right_value = right_row
                        .get(self.right_col.clone())
                        .expect("value exist here")
                    && left_value.eq(&right_value)
                {}
                // let mut buf = vec![];
                // for sr in self.schema.columns.iter() {
                //     let column = (*sr.column).clone();

                //     let serialized = match right_row.get(column) {
                //         Ok(value) => value.serialize(),
                //         Err(_) => Value::Null.serialize(),
                //     };

                //     buf.push(serialized);
                // }

                // self.buffer.push(buf);

                // 1 < 2
                // 2 = 2
                // 3 > 2
                match left_value.cmp(&right_value) {
                    Ordering::Less => {
                        self.buffer.clear();
                        self.index = 0;

                        self.left.advance();
                    }
                    Ordering::Equal => break,
                    Ordering::Greater => {
                        // check
                        self.buffer.clear();
                        self.index = 0;

                        self.right.advance();
                    }
                }
            }
        }
        */
    }
}
