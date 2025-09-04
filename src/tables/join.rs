use crate::{Column, Iterator, Result, Row, Rows, Schema, SchemaRow, SqliteError, Table, Value};

pub struct Join<'table> {
    left: Box<Table<'table>>,
    right: Box<Table<'table>>,
    schema: Schema,
}

impl<'table> Join<'table> {
    pub fn new(left: Table<'table>, right: Table<'table>) -> Self {
        let mut names = vec![];
        let mut columns = vec![];

        for table in [&left, &right] {
            let schema = table.schema();
            let tbl_names = &schema.names;
            let tbl_name = tbl_names.iter().max_by_key(|s| s.len()).unwrap();

            names.extend(tbl_names.iter().cloned());

            for sr in &schema.columns {
                let column = match &sr.column {
                    Column::Single(column) => Column::Dotted {
                        table: tbl_name.clone(),
                        column: column.clone(),
                    },
                    dotted => dotted.clone(),
                };

                let sr = SchemaRow {
                    column,
                    r#type: sr.r#type,
                };

                columns.push(sr);
            }
        }

        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema: Schema {
                names,
                columns,
                primary: vec![],
            },
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::Join(JoinRows {
            rows: vec![self.left.rows(), self.right.rows()],
        })
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let spacer = "  ".repeat(indent);

        writeln!(f, "{:<width$} │ {}on ...", "join", spacer)?;
        self.left.write_indented(f, width, indent + 1)?;
        self.right.write_indented(f, width, indent + 1)?;

        Ok(())
    }

    pub fn write_normal(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(")?;
        self.left.write_normal(f)?;
        write!(f, ") join (")?;
        self.right.write_normal(f)?;
        write!(f, ")")?;

        Ok(())
    }
}

pub struct JoinRows<'rows, 'table> {
    rows: Vec<Rows<'rows, 'table>>,
}

impl Iterator for JoinRows<'_, '_> {
    fn current(&self) -> Option<crate::Row<'_>> {
        let row = self
            .rows
            .iter()
            .map(|r| r.current())
            .collect::<Option<Vec<_>>>()?;

        Some(Row::Join(JoinRow { row }))
    }

    fn advance(&mut self) {
        loop {
            let begin = self.rows.iter().all(|r| r.current().is_none());

            if begin {
                self.rows.iter_mut().for_each(|r| r.advance());
            } else {
                for row in self.rows.iter_mut().rev() {
                    row.advance();

                    if row.current().is_some() {
                        break;
                    }
                }

                if self.rows.iter().all(|r| r.current().is_none()) {
                    return;
                }

                for row in self.rows.iter_mut().rev() {
                    if row.current().is_none() {
                        row.advance();
                    }
                }
            }

            if self.current().is_some() {
                return;
            }
        }
    }
}

pub struct JoinRow<'row> {
    row: Vec<Row<'row>>,
}

impl<'row> JoinRow<'row> {
    pub fn get(&self, column: Column) -> Result<Value<'row>> {
        let values = self
            .row
            .iter()
            .flat_map(|r| r.get(column.clone()))
            .collect::<Vec<_>>();

        match values.len() {
            0 => Err(SqliteError::ColumnNotFound(column)),
            1 => Ok(values[0].clone()),
            _ => Err(SqliteError::WrongColumn(column)),
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

        assert!(matches!(&table, Table::Join(_)));

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
}
