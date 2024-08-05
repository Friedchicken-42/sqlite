use std::borrow::Cow;

use crate::{
    command::{FromTable, Select, SimpleColumn},
    schema::build_schema,
    Column, Row, Schema, Table, TableState, Value,
};

use anyhow::{bail, Result};

#[derive(Debug)]
pub struct Materialized<'a> {
    schema: Schema,
    data: Vec<Vec<Value<'a>>>,
    table: FromTable,
    state: TableState,
}

impl<'a> Materialized<'a> {
    pub fn new(input: Select, table: Box<dyn Table + '_>) -> Result<Self> {
        let tables = vec![&table];

        let (schema, columns) = build_schema(&input.select, &tables, &input.from.tables)?;

        let mut data: Vec<Vec<Value>> = vec![];
        let mut table = table;

        let table_name = input.from.tables[0].clone();

        let groupby = input.groupby.map_or(Ok(vec![]), |gb| {
            gb.columns
                .iter()
                .map(|column| match column {
                    SimpleColumn::String(s) => Ok(Column::String(s.to_string())),
                    SimpleColumn::Dotted { table, column } => Ok(Column::Dotted {
                        table: table.to_string(),
                        column: column.to_string(),
                    }),
                    SimpleColumn::Function { .. } => bail!("no function in group by clause"),
                })
                .collect::<Result<Vec<_>>>()
        })?;

        while let Some(row) = table.next() {
            let mut new_row = vec![None; schema.len()];
            let mut previous = None;

            for column in &groupby {
                let value = row.get(column)?;

                let value = match value {
                    Value::Null => Value::Null,
                    Value::Integer(i) => Value::Integer(i),
                    Value::Float(f) => Value::Float(f),
                    Value::Text(t) => Value::Text(Cow::Owned(t.to_string())),
                    Value::Blob(b) => Value::Blob(Cow::Owned(b.to_vec())),
                };

                if let Some(index) = schema
                    .iter()
                    .position(|sr| sr.column.name() == column.name())
                {
                    new_row[index] = Some(value);
                } else {
                    bail!("missing column {:?} in input", column.name());
                }

                for (i, _) in schema.iter().enumerate() {
                    if new_row[i].is_none() {
                        let column = &columns[i];
                        match column {
                            Column::Function(f) => {
                                previous = data.iter().position(|row| {
                                    for (i, value) in row.iter().enumerate() {
                                        if let Some(v) = &new_row[i] {
                                            if v != value {
                                                return false;
                                            }
                                        }
                                    }

                                    true
                                });

                                let new = match previous {
                                    Some(index) => {
                                        let old = &data[index];
                                        f.apply(&new_row, old, &columns, i)?
                                    }
                                    None => f.default(&new_row, &schema),
                                };

                                new_row[i] = Some(new);
                            }
                            c => bail!("column {:?} must be inside group by clause", c.name()),
                        }
                    }
                }
            }

            let row = new_row.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>();
            match previous {
                Some(index) => data[index] = row,
                None => data.push(row),
            }
        }

        Ok(Self {
            schema,
            data,
            table: table_name,
            state: TableState::Start,
        })
    }
}

impl<'a> Table for Materialized<'a> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        match self.state {
            TableState::Next(x) => Some(Box::new(MaterializedRow {
                schema: &self.schema,
                data: &self.data[x - 1],
                table: &self.table,
            })),
            TableState::Start | TableState::End => None,
        }
    }

    fn advance(&mut self) {
        self.state = match self.state {
            TableState::Start => TableState::Next(1),
            TableState::Next(x) if x != self.data.len() => TableState::Next(x + 1),
            _ => TableState::End,
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug)]
struct MaterializedRow<'a> {
    schema: &'a Schema,
    data: &'a [Value<'a>],
    table: &'a FromTable,
}

impl<'a> Row<'a> for MaterializedRow<'a> {
    fn get(&self, column: &Column) -> Result<Value<'a>> {
        let pos = self.schema.iter().position(|row| match column {
            Column::String(name) => row.column.name() == name,
            Column::Dotted { table, column } => {
                row.column.name() == column && self.table.alias() == table
            }
            Column::Function(_) => row.column == *column,
        });

        match pos {
            Some(index) => Ok(self.data[index].clone()),
            None => bail!("column {:?} not found", column.name()),
        }
    }

    fn all(&self) -> Result<Vec<Value<'a>>> {
        self.schema
            .iter()
            .map(|row| self.get(&row.column))
            .collect::<Result<Vec<_>>>()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{bail, Result};

    use crate::{command::Command, Sqlite, Table, Value};

    #[test]
    fn groupby_count() -> Result<()> {
        let db = Sqlite::read("other.db")?;

        let query = "select name, count(*) from apples group by name";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut out = select.execute(&db)?;

        while let Some(row) = out.next() {
            let name = row.get(&"name".into())?;
            let count = row.get(&"count(*)".into())?;
            if name == Value::Text("Fuji".into()) {
                assert_eq!(count, Value::Integer(2));
            } else {
                assert_eq!(count, Value::Integer(1));
            }
        }
        Ok(())
    }

    #[test]
    fn groupby_alias() -> Result<()> {
        let db = Sqlite::read("other.db")?;

        let query = "select a.name, count(*) as c from apples as a group by a.name";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut out = select.execute(&db)?;

        while let Some(row) = out.next() {
            assert!(row.get(&"name".into()).is_ok());
            assert!(row.get(&"c".into()).is_ok());
        }
        Ok(())
    }
}
