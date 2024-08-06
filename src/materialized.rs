use std::borrow::Cow;

use crate::{
    command::{FromTable, Select, SimpleColumn},
    schema::build_schema,
    Column, Function, FunctionParam, Row, Schema, Table, TableState, Value,
};

use anyhow::{bail, Result};

#[derive(Debug)]
pub struct Materialized<'a> {
    schema: Schema,
    data: Vec<Vec<Value<'a>>>,
    table: FromTable,
    state: TableState,
}

#[derive(Debug, Clone)]
enum Func {
    CountWild(u32),
    Count(u32),
}

#[derive(Debug, Clone)]
enum Item<'a> {
    Value(Value<'a>),
    Function(Func),
}

impl<'a> Item<'a> {
    fn value(self) -> Value<'a> {
        match self {
            Item::Value(v) => v,
            Item::Function(Func::CountWild(c)) => Value::Integer(c),
            Item::Function(Func::Count(c)) => Value::Integer(c),
        }
    }
}

fn apply<'a>(
    f: &Function,
    old_row: &Option<&[Item]>,
    row: &(dyn Row + '_),
    index: usize,
) -> Result<Item<'a>> {
    let func = match (old_row, f) {
        (None, Function::Count(FunctionParam::Wildcard)) => Func::CountWild(1),
        (Some(old), Function::Count(FunctionParam::Wildcard)) => {
            let Item::Function(Func::CountWild(v)) = old[index] else {
                bail!("expected function `count(*)`");
            };
            Func::CountWild(v + 1)
        }
        (_, Function::Count(FunctionParam::Column(column))) => {
            let inc = match row.get(column)? {
                Value::Null => 0,
                _ => 1,
            };

            match old_row {
                None => Func::Count(inc),
                Some(old) => {
                    let Item::Function(Func::Count(v)) = old[index] else {
                        bail!("expected function `count`");
                    };
                    Func::Count(v + inc)
                }
            }
        }
    };

    Ok(Item::Function(func))
}

fn row_position<'a>(data: &'a [Vec<Item<'a>>], new: &'a [Option<Item<'a>>]) -> Option<usize> {
    data.iter().position(|row| {
        for (i, value) in row.iter().enumerate() {
            match (&new[i], value) {
                (Some(Item::Value(a)), Item::Value(b)) if a != b => return false,
                _ => {}
            }
        }

        true
    })
}

impl<'a> Materialized<'a> {
    pub fn new(input: Select, mut table: Box<dyn Table + '_>) -> Result<Self> {
        let (schema, columns) = build_schema(&input.select, &[&table], &input.from.tables)?;

        let mut data: Vec<Vec<Item>> = vec![];

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
                    new_row[index] = Some(Item::Value(value));
                } else {
                    bail!("missing column {:?} in input", column.name());
                }
            }

            for (i, _) in schema.iter().enumerate() {
                if new_row[i].is_none() {
                    let column = &columns[i];
                    match column {
                        Column::Function(f) => {
                            previous = row_position(&data, &new_row);
                            let old_row = previous.map(|index| data[index].as_ref());

                            let new = apply(f, &old_row, &row, i)?;

                            new_row[i] = Some(new);
                        }
                        c if !groupby.is_empty() => {
                            bail!("column {:?} must be inside group by clause", c.name())
                        }
                        _ => {
                            let value = row.get(column)?;

                            let value = match value {
                                Value::Null => Value::Null,
                                Value::Integer(i) => Value::Integer(i),
                                Value::Float(f) => Value::Float(f),
                                Value::Text(t) => Value::Text(Cow::Owned(t.to_string())),
                                Value::Blob(b) => Value::Blob(Cow::Owned(b.to_vec())),
                            };

                            new_row[i] = Some(Item::Value(value));
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

        let data = data
            .into_iter()
            .map(|row| row.into_iter().map(|item| item.value()).collect())
            .collect::<Vec<_>>();

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

    use super::Materialized;

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

    #[test]
    fn materialized() -> Result<()> {
        let db = Sqlite::read("other.db")?;

        let query = "select name from apples";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let table = select.execute(&db)?;

        let query = "select name from apples";
        let Command::Select(input) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut out = Materialized::new(input, table)?;

        while let Some(row) = out.next() {
            assert!(row.get(&"name".into()).is_ok());
        }
        Ok(())
    }

    #[test]
    fn extra_column() -> Result<()> {
        let db = Sqlite::read("other.db")?;

        let query = "select name, count(*), id from apples group by name";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        assert!(select.execute(&db).is_err());
        Ok(())
    }

    #[test]
    fn count_column() -> Result<()> {
        let db = Sqlite::read("other.db")?;

        let query = "select name, count(id) from apples group by name";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        assert!(select.execute(&db).is_ok());
        Ok(())
    }
}
