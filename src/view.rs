use std::fmt::Debug;

use crate::{
    Column, Iterator, Result, Row, Rows, Schema, SchemaRow, SqliteError, Table, Value,
    parser::Select,
};

pub struct View<'table> {
    inner: Box<Table<'table>>,
    schema: Schema,
    inner_columns: Vec<Column>,
}

impl Debug for View<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows = self
            .schema
            .columns
            .iter()
            .map(|sr| match &sr.column {
                Column::Single(c) => c.to_string(),
                Column::Dotted { table, column } => format!("{table}.{column}"),
            })
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "select {rows} from ({:?})", self.inner)
    }
}

fn create_schema(select: Vec<Select>, inner: &Table) -> Result<(Schema, Vec<Column>)> {
    let schema = inner.schema();

    let srs = select
        .iter()
        .map(|row| match row {
            Select::Wildcard => Ok(schema
                .columns
                .iter()
                .map(|sr| (sr.clone(), sr.column.clone()))
                .collect::<Vec<_>>()),
            Select::Column { name, alias, table } => {
                let sr = schema
                    .columns
                    .iter()
                    .find(|sr| sr.column.name() == name)
                    .ok_or(SqliteError::ColumnNotFound(name.as_str().into()))?
                    .clone();

                let column = match table {
                    Some(table) => Column::Dotted {
                        table: table.to_string(),
                        column: name.to_string(),
                    },
                    None => Column::Single(name.to_string()),
                };

                let sr = match alias {
                    Some(alias) => SchemaRow {
                        column: Column::Single(alias.to_string()),
                        ..sr
                    },
                    None => SchemaRow {
                        column: column.clone(),
                        ..sr
                    },
                };

                Ok(vec![(sr, column)])
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        // TODO: there must be a better way
        .fold((vec![], vec![]), |(mut s, mut n), (a, b)| {
            s.push(a);
            n.push(b);
            (s, n)
        });

    Ok((
        Schema {
            names: schema.names.clone(),
            columns: srs.0,
        },
        srs.1,
    ))
}

impl<'table> View<'table> {
    pub fn new(select: Vec<Select>, inner: Table<'table>) -> Self {
        let (schema, inner_columns) = create_schema(select, &inner).unwrap();

        Self {
            inner: Box::new(inner),
            schema,
            inner_columns,
        }
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::View(ViewRows {
            rows: Box::new(self.inner.rows()),
            schema: &self.schema,
            inner: &self.inner_columns,
        })
    }

    pub fn count(&mut self) -> usize {
        self.inner.count()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct ViewRows<'rows, 'table> {
    rows: Box<Rows<'rows, 'table>>,
    schema: &'rows Schema,
    inner: &'rows [Column],
}

impl Iterator for ViewRows<'_, '_> {
    fn current(&self) -> Option<Row<'_>> {
        let current = self.rows.current()?;
        Some(Row::View(ViewRow {
            row: Box::new(current),
            schema: self.schema,
            inner: self.inner,
        }))
    }

    fn advance(&mut self) {
        self.rows.advance();
    }
}

pub struct ViewRow<'row> {
    row: Box<Row<'row>>,
    schema: &'row Schema,
    inner: &'row [Column],
}

impl<'row> ViewRow<'row> {
    pub fn get(&self, column: Column) -> Result<Value<'row>> {
        let columns = self
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, sr)| match (&column, &sr.column) {
                (Column::Single(name1), Column::Single(name2)) => name1 == name2,
                (Column::Single(name1), Column::Dotted { column: name2, .. }) => name1 == name2,
                (Column::Dotted { .. }, Column::Single(_)) => false,
                (
                    Column::Dotted {
                        table: table1,
                        column: name1,
                    },
                    Column::Dotted {
                        table: table2,
                        column: name2,
                    },
                ) => table1 == table2 && name1 == name2,
            })
            .collect::<Vec<_>>();

        match &columns[..] {
            [] => Err(SqliteError::ColumnNotFound(column)),
            [(i, _)] => self.row.get(self.inner[*i].clone()),
            _ => Err(SqliteError::WrongColumn(column)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Result, Sqlite, Table, parser::Query};

    use super::*;

    #[test]
    fn view() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select id from apples")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::View(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("id".into())?;
            assert_eq!(value, Value::Integer(i));

            assert!(row.get("name".into()).is_err());
        }

        Ok(())
    }

    #[test]
    fn view_alias() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select id as row_id from apples")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::View(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("row_id".into())?;
            assert_eq!(value, Value::Integer(i));

            assert!(row.get("id".into()).is_err());
        }

        Ok(())
    }
}
