use crate::{
    Column, ErrorKind, Iterator, Result, Row, Rows, Schema, SchemaRow, Table, Value,
    parser::{Expression, Spanned},
};

pub struct Join<'table> {
    pub left: Box<Table<'table>>,
    pub right: Box<Table<'table>>,
    schema: Schema,
    left_column: Option<Column>,
}

impl<'table> Join<'table> {
    pub fn new(left: Table<'table>, right: Table<'table>) -> Self {
        let (names, columns) = Self::create_schema(&left, &right);

        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema: Schema {
                names,
                columns,
                primary: vec![],
            },
            left_column: None,
        }
    }

    fn create_schema(left: &Table<'table>, right: &Table<'table>) -> (Vec<String>, Vec<SchemaRow>) {
        let mut names = vec![];
        let mut columns = vec![];

        for table in [left, right] {
            let schema = table.schema();
            let tbl_names = &schema.names;
            let tbl_name = tbl_names.iter().max_by_key(|s| s.len()).unwrap();

            names.extend(tbl_names.iter().cloned());

            for sr in &schema.columns {
                let column = match &*sr.column {
                    Column::Single(column) => Column::Dotted {
                        table: tbl_name.clone(),
                        column: column.clone(),
                    },
                    dotted => (*dotted).clone(),
                };

                let sr = SchemaRow {
                    column: Spanned::span(column, sr.column.span.clone()),
                    r#type: sr.r#type,
                };

                columns.push(sr);
            }
        }

        (names, columns)
    }

    pub fn indexed(left: Table<'table>, right: Table<'table>, column: Column) -> Self {
        Self {
            left_column: Some(column),
            ..Self::new(left, right)
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::Join(JoinRows {
            left: Box::new(self.left.rows()),
            right: Box::new(self.right.rows()),
            left_column: self.left_column.as_ref(),
        })
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let spacer = "  ".repeat(indent);

        write!(f, "{:<width$} |{}", "join", spacer)?;
        if let Some(left_column) = &self.left_column {
            writeln!(f, "on {}", left_column.name())?;
        } else {
            writeln!(f, "full")?;
        }

        self.left.write_indented(f, width, indent + 1)?;
        self.right.write_indented(f, width, indent + 1)?;

        Ok(())
    }
}

pub struct JoinRows<'rows, 'table> {
    left: Box<Rows<'rows, 'table>>,
    right: Box<Rows<'rows, 'table>>,
    left_column: Option<&'rows Column>,
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
        if self.left.current().is_none() && self.right.current().is_none() {
            self.left.advance();
            self.setup_right();
            self.right.advance();
            return;
        }

        self.right.advance();

        if self.right.current().is_none() {
            self.left.advance();

            if self.left.current().is_none() {
                return;
            }

            self.setup_right();
            self.right.advance();
        }
    }
}

impl JoinRows<'_, '_> {
    fn setup_right(&mut self) {
        let Rows::Indexed(indexed) = &mut *self.right else {
            return;
        };

        indexed.expressions.clear();

        let left = self.left.current().unwrap();
        let column = self.left_column.unwrap();
        let value = left.get(column.clone()).unwrap();

        let expr = match value {
            Value::Integer(n) => Expression::Number(n),
            Value::Text(t) => Expression::Literal(t.to_string()),
            _ => panic!("not supported as an expression"),
        };

        indexed.expressions.push(expr);
    }
}

pub struct JoinRow<'row> {
    left: Box<Row<'row>>,
    right: Box<Row<'row>>,
}

impl<'row> JoinRow<'row> {
    pub fn get(&self, column: Column) -> Result<Value<'row>> {
        match (
            self.left.get(column.clone()),
            self.right.get(column.clone()),
        ) {
            (Err(_), Err(_)) => Err(ErrorKind::ColumnNotFound(column).into()),
            (Ok(_), Ok(_)) => Err(ErrorKind::WrongColumn(column).into()),
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
}
