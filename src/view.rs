use anyhow::{anyhow, bail, Result};

use crate::{
    command::{FromTable, InputColumn, SelectClause, SimpleColumn},
    Column, Row, Schema, Sqlite, Table, Value,
};

#[derive(Debug)]
pub struct ViewRow<'a> {
    schema: &'a Schema,
    rows: Vec<Box<dyn Row<'a> + 'a>>,
    tables: &'a [String],
}

impl<'a> Row<'a> for ViewRow<'a> {
    fn get(&self, column: &Column) -> Result<Value<'a>> {
        match column {
            Column::String(_) => {
                for row in &self.rows {
                    let value = row.get(column);
                    if value.is_ok() {
                        return value;
                    }
                }
            }
            dotted @ Column::Dotted { table, .. } => {
                let pos = self.tables.iter().position(|name| name == table);
                if let Some(index) = pos {
                    return self.rows[index].get(dotted);
                }
            }
        }

        bail!("missing column: {column:?}")
    }

    fn all(&self) -> Result<Vec<Value<'a>>> {
        self.schema
            .0
            .iter()
            .map(|(name, _)| self.get(name))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug)]
pub struct View<'a> {
    names: Vec<String>,
    tables: Vec<Box<dyn Table + 'a>>,
    schema: Schema,
    db: &'a Sqlite,
}

impl<'a> View<'a> {
    pub fn new(select: SelectClause, from: Vec<FromTable>, db: &'a Sqlite) -> Result<Self> {
        let names = from
            .iter()
            .map(|table| match table {
                FromTable::Simple(n) => n,
                FromTable::Alias(n, _) => n,
            })
            .map(|name| name.to_string())
            .collect::<Vec<_>>();

        let mut tables = names
            .iter()
            .map(|name| db.search(name, vec![]))
            .collect::<Result<Vec<_>>>()?;

        tables.iter_mut().enumerate().for_each(|(i, table)| {
            if i < names.len() - 1 {
                table.advance();
            }
        });

        let schema = select
            .columns
            .iter()
            .map(|column| match column {
                InputColumn::Wildcard => Ok(tables
                    .iter()
                    .map(|table| match table.name() {
                        Some(name) => Ok((name, &table.schema().0)),
                        None => Err(anyhow!("ASED")),
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flat_map(|(name, vec)| {
                        vec.iter().map(|(col, r#type)| {
                            (
                                Column::Dotted {
                                    table: name.to_string(),
                                    column: col.name().to_string(),
                                },
                                *r#type,
                            )
                        })
                    })
                    .collect::<Vec<_>>()),
                InputColumn::Alias(_, _) => todo!(),
                InputColumn::Simple(SimpleColumn::String(column)) => {
                    let vec = tables
                        .iter()
                        .map(|table| table.schema())
                        .flat_map(|schema| &schema.0)
                        .filter(|(col, _)| col.name() == *column)
                        .collect::<Vec<_>>();

                    match vec[..] {
                        [] => Err(anyhow!("missing column {column:?}")),
                        [pair] => Ok(vec![pair.clone()]),
                        _ => Err(anyhow!("multiple columns {column:?}")),
                    }
                }
                InputColumn::Simple(SimpleColumn::Dotted { table, column }) => {
                    let index = names
                        .iter()
                        .position(|name| *name == *table)
                        .ok_or(anyhow!("Missing table {table:?}"))?;

                    let (_, r#type) = tables[index]
                        .schema()
                        .0
                        .iter()
                        .find(|(col, _)| col.name() == *column)
                        .ok_or(anyhow!("Missing column {column:?}"))?;

                    Ok(vec![(
                        Column::Dotted {
                            table: table.to_string(),
                            column: column.to_string(),
                        },
                        *r#type,
                    )])
                }
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Self {
            names,
            tables,
            schema: Schema(schema),
            db,
        })
    }
}

impl<'a> Table for View<'a> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        if self.tables.is_empty() {
            return None;
        }

        let rows = self
            .tables
            .iter()
            .map(|table| table.current())
            .collect::<Option<Vec<_>>>()?;

        Some(Box::new(ViewRow {
            rows,
            schema: &self.schema,
            tables: &self.names,
        }))
    }

    fn advance(&mut self) {
        loop {
            if self.tables.is_empty() {
                break;
            }

            self.tables.last_mut().unwrap().advance();

            if self.tables.last().unwrap().current().is_none() {
                self.tables.pop();
            } else if self.tables.len() < self.names.len() {
                let name = &self.names[self.tables.len()];
                let new_table = self.db.search(name, vec![]).unwrap();
                // new_table.advance();

                self.tables.push(new_table);
            } else {
                break;
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn name(&self) -> Option<&str> {
        None
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{bail, Result};

    use crate::{
        command::{FromTable, InputColumn, SelectClause, SimpleColumn},
        display::DisplayMode,
        Column, Sqlite, Table, Type,
    };

    use super::View;

    #[test]
    fn view_get() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![
                InputColumn::Simple(SimpleColumn::String("color".into())),
                InputColumn::Simple(SimpleColumn::String("description".into())),
            ],
        };

        let from = vec![
            FromTable::Simple("apples".into()),
            FromTable::Simple("oranges".into()),
        ];

        let mut view = View::new(select, from, &db)?;

        assert_eq!(
            view.schema().0,
            [
                ("color".into(), Type::Text),
                ("description".into(), Type::Text),
            ]
        );

        while let Some(row) = view.next() {
            row.get(&"color".into())?;
            row.get(&"description".into())?;
        }

        Ok(())
    }

    #[test]
    fn view_all() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![
                InputColumn::Wildcard,
                InputColumn::Simple(SimpleColumn::String("color".into())),
            ],
        };

        let from = vec![
            FromTable::Simple("apples".into()),
            FromTable::Simple("oranges".into()),
        ];

        let view = View::new(select, from, &db)?;

        assert_eq!(
            view.schema().0,
            [
                ("apples.id".into(), Type::Integer),
                ("apples.name".into(), Type::Text),
                ("apples.color".into(), Type::Text),
                ("oranges.id".into(), Type::Integer),
                ("oranges.name".into(), Type::Text),
                ("oranges.description".into(), Type::Text),
                (Column::String("color".into()), Type::Text),
            ]
        );

        use std::io::Cursor;
        let mut f = Cursor::new(vec![]);
        db.display(&mut f, view, DisplayMode::Table)?;
        println!("{}", std::str::from_utf8(f.get_ref())?);

        Ok(())
    }

    #[test]
    fn view_single() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![InputColumn::Wildcard],
        };

        let from = vec![FromTable::Simple("apples".into())];

        let mut view = View::new(select, from, &db)?;

        let mut base = db.search("apples", vec![])?;

        loop {
            match (view.next(), base.next()) {
                (Some(a), Some(b)) => {
                    assert_eq!(a.get(&"id".into())?, b.get(&"id".into())?);
                    assert_eq!(a.get(&"name".into())?, b.get(&"name".into())?);
                    assert_eq!(a.get(&"color".into())?, b.get(&"color".into())?);
                }
                (None, None) => break,
                _ => bail!("view differ from table"),
            }
        }

        Ok(())
    }
}
