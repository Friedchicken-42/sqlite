use anyhow::{anyhow, bail, Result};

use crate::{
    command::{Column, FromTable, SelectClause, SimpleColumn},
    Row, Schema, Sqlite, Table, Value,
};

#[derive(Debug)]
pub struct ViewRow<'a> {
    schema: &'a Schema,
    rows: Vec<Box<dyn Row<'a> + 'a>>,
}

impl<'a> Row<'a> for ViewRow<'a> {
    fn get(&self, column: &str) -> Result<Value<'a>> {
        for row in &self.rows {
            let value = row.get(column);
            if value.is_ok() {
                return value;
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
    fn new(select: SelectClause, from: Vec<FromTable>, db: &'a Sqlite) -> Result<Self> {
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
                Column::Simple(SimpleColumn::String(column)) => {
                    let vec = tables
                        .iter()
                        .map(|table| table.schema())
                        .flat_map(|schema| schema.0.clone())
                        .filter(|(name, _)| *name == *column)
                        .collect::<Vec<_>>();

                    match vec.len() {
                        0 => Err(anyhow!("missing column {column:?}")),
                        1 => Ok(vec),
                        _ => Err(anyhow!("multiple columns {column:?}")),
                    }
                }
                Column::Alias(_, _) => todo!(),
                Column::Simple(SimpleColumn::Dotted(_)) => todo!(),
                Column::Wildcard => Ok(tables
                    .iter()
                    .flat_map(|t| t.schema().0.clone())
                    .collect::<Vec<_>>()),
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
                let mut new_table = self.db.search(name, vec![]).unwrap();
                new_table.advance();

                self.tables.push(new_table);
            } else {
                break;
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{bail, Result};

    use crate::{
        command::{Column, FromTable, SelectClause, SimpleColumn},
        display::DisplayMode,
        Sqlite, Table, Type,
    };

    use super::View;

    #[test]
    fn view_get() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![
                Column::Simple(SimpleColumn::String("color".into())),
                Column::Simple(SimpleColumn::String("description".into())),
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
            row.get("color")?;
            row.get("description")?;
        }

        Ok(())
    }

    #[test]
    fn view_all() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![
                // Column::Simple(SimpleColumn::String("color".into())),
                // Column::Simple(SimpleColumn::String("description".into())),
                Column::Wildcard,
                Column::Simple(SimpleColumn::String("color".into())),
            ],
        };

        let from = vec![
            FromTable::Simple("apples".into()),
            FromTable::Simple("oranges".into()),
        ];

        let view = View::new(select, from, &db)?;

        use std::io::Cursor;
        let mut f = Cursor::new(vec![]);
        db.display(&mut f, view, DisplayMode::Table)?;
        println!("{}", std::str::from_utf8(f.get_ref())?);

        // while let Some(row) = view.next() {
        //     let values = row.all();
        //     println!("{values:?}");
        // }

        Ok(())
    }

    #[test]
    fn view_single() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![Column::Wildcard],
        };

        let from = vec![FromTable::Simple("apples".into())];

        let mut view = View::new(select, from, &db)?;

        let mut base = db.search("apples", vec![])?;

        loop {
            match (view.next(), base.next()) {
                (Some(a), Some(b)) => {
                    assert_eq!(a.get("id")?, b.get("id")?);
                    assert_eq!(a.get("name")?, b.get("name")?);
                    assert_eq!(a.get("color")?, b.get("color")?);
                }
                (None, None) => break,
                _ => bail!("view differ from table"),
            }
        }

        Ok(())
    }
}
