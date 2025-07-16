use crate::{
    Column, Iterator, Result, Row, Rows, Schema, SchemaRow, SqliteError, Table, Value,
    parser::Select,
};

pub struct View<'table> {
    inner: Box<Table<'table>>,
    schema: Schema,
    inner_columns: Vec<Column>,
}

fn create_schema(select: Vec<Select>, inner: &Table) -> Result<(Schema, Vec<Column>)> {
    let schema = inner.schema();

    let srs = select
        .iter()
        .map(|row| match row {
            Select::Wildcard => Ok(schema
                .0
                .iter()
                .map(|sr| (sr.clone(), sr.column.clone()))
                .collect::<Vec<_>>()),
            // Selec::Column {
            //     table: Some(table), ..
            // } if !inner.names.contains(table) => Err(SqliteError::TableNotFound(table.to_string())),
            Select::Column { name, alias, table } => {
                let sr = schema
                    .0
                    .iter()
                    .find(|sr| sr.column.name() == name)
                    .ok_or(SqliteError::ColumnNotFound(name.as_str().into()))?
                    .clone();

                let sr = match alias {
                    Some(alias) => SchemaRow {
                        column: Column::Single(alias.to_string()),
                        ..sr
                    },
                    None => sr,
                };

                // let column = Column::Single(name.to_string());
                let column = match table {
                    Some(table) => Column::Dotted {
                        table: table.to_string(),
                        column: name.to_string(),
                    },
                    None => Column::Single(name.to_string()),
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

    Ok((Schema::new(srs.0), srs.1))
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
        let index = self
            .schema
            .0
            .iter()
            .position(|sr| sr.column.name() == column.name());

        match index {
            None => Err(SqliteError::ColumnNotFound(column)),
            Some(i) => self.row.get(self.inner[i].clone()),
        }
    }
}
