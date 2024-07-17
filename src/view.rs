use anyhow::{anyhow, bail, Result};

use crate::{
    command::{FromClause, FromTable, InputColumn, SelectClause, SimpleColumn, WhereClause},
    Column, Row, Schema, Sqlite, Table, Type, Value,
};

#[derive(Debug)]
pub struct ViewRow<'a> {
    schema: &'a Schema,
    rows: Vec<Box<dyn Row<'a> + 'a>>,
    input: &'a [FromTable],
    output: &'a [String],
}

impl<'a> Row<'a> for ViewRow<'a> {
    fn get(&self, column: &Column) -> Result<Value<'a>> {
        let pos = self
            .schema
            .0
            .iter()
            .position(|(col, _)| column.name() == col.name());

        let Some(index) = pos else {
            bail!("missing column: {column:?}")
        };

        // TODO: fix extra cloning
        let name = self.output[index].clone();

        let column = match column {
            Column::String(_) => Column::String(name),
            Column::Dotted { table, .. } => Column::Dotted {
                table: table.to_string(),
                column: name,
            },
        };

        match &column {
            Column::String(_) => {
                for row in &self.rows {
                    let value = row.get(&column);
                    if value.is_ok() {
                        return value;
                    }
                }
            }
            dotted @ Column::Dotted { table, .. } => {
                let pos = self.input.iter().position(|tbl| tbl.alias() == table);
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

fn simple_column_pair(
    column: &SimpleColumn,
    tables: &[Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<(Column, Type)> {
    match column {
        SimpleColumn::String(name) => {
            let pairs = tables
                .iter()
                .flat_map(|table| &table.schema().0)
                .filter(|(col, _)| col.name() == name)
                .collect::<Vec<_>>();

            match pairs[..] {
                [] => bail!("column {name:?} not found"),
                [pair] => Ok(pair.clone()),
                _ => bail!(""),
            }
        }
        SimpleColumn::Dotted { table, column } => {
            let index = input
                .iter()
                .position(|tbl| *tbl.alias() == *table)
                .ok_or(anyhow!("Missing table {table:?}"))?;

            let (_, r#type) = tables[index]
                .schema()
                .0
                .iter()
                .find(|(col, _)| col.name() == *column)
                .ok_or(anyhow!("Missing column {column:?}"))?;

            Ok((
                Column::Dotted {
                    table: table.to_string(),
                    column: column.to_string(),
                },
                *r#type,
            ))
        }
    }
}

fn build_schema(
    select: SelectClause,
    tables: &[Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<(Schema, Vec<String>)> {
    let (schema, output): (Vec<(Column, Type)>, Vec<String>) = select
        .columns
        .iter()
        .map(|column| match column {
            InputColumn::Wildcard => Ok(input
                .iter()
                .zip(tables.iter())
                .flat_map(|(from, table)| {
                    let schema = table.schema();
                    schema.0.iter().map(|(col, r#type)| {
                        (
                            Column::Dotted {
                                table: from.alias().to_string(),
                                column: col.name().to_string(),
                            },
                            *r#type,
                            col.name().to_string(),
                        )
                    })
                })
                .collect::<Vec<_>>()),
            InputColumn::Simple(column) => {
                let (column, r#type) = simple_column_pair(column, tables, input)?;
                let name = column.name().to_string();
                Ok(vec![(column, r#type, name)])
            }
            InputColumn::Alias(column, alias) => {
                let (column, r#type) = simple_column_pair(column, tables, input)?;
                let name = column.name().to_string();

                Ok(vec![(
                    match column {
                        Column::String(_) => Column::String(alias.to_string()),
                        Column::Dotted { table, column } => Column::Dotted {
                            table,
                            column: alias.to_string(),
                        },
                    },
                    r#type,
                    name,
                )])
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|item| ((item.0, item.1), item.2))
        .unzip();

    Ok((Schema(schema), output))
}

#[derive(Debug)]
pub struct View<'a> {
    input: Vec<FromTable>,
    output: Vec<String>,
    tables: Vec<Box<dyn Table + 'a>>,
    r#where: Option<WhereClause<'a>>,
    schema: Schema,
    db: &'a Sqlite,
}

impl<'a> View<'a> {
    pub fn new(
        select: SelectClause,
        from: FromClause,
        db: &'a Sqlite,
        r#where: Option<WhereClause<'a>>,
    ) -> Result<Self> {
        let input = from.tables.clone();

        let mut tables = input
            .iter()
            .map(|table| db.search(table.name(), vec![]))
            .collect::<Result<Vec<_>>>()?;

        tables.iter_mut().enumerate().for_each(|(i, table)| {
            if i < input.len() - 1 {
                table.advance();
            }
        });

        let (schema, output) = build_schema(select, &tables, &input)?;

        Ok(Self {
            input,
            output,
            tables,
            r#where,
            schema,
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
            input: &self.input,
            output: &self.output,
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
            } else if self.tables.len() < self.input.len() {
                let tbl = &self.input[self.tables.len()];
                let new_table = self.db.search(tbl.name(), vec![]).unwrap();

                self.tables.push(new_table);
            } else if let Some(r#where) = &self.r#where {
                if let Some(row) = self.current() {
                    if r#where.matches(&row) {
                        break;
                    }
                }
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
        command::{FromClause, FromTable, InputColumn, SelectClause, SimpleColumn},
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

        let from = FromClause {
            tables: vec![
                FromTable::Simple("apples".into()),
                FromTable::Simple("oranges".into()),
            ],
            conditions: vec![],
        };

        let mut view = View::new(select, from, &db, None)?;

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

        let from = FromClause {
            tables: vec![
                FromTable::Simple("apples".into()),
                FromTable::Simple("oranges".into()),
            ],
            conditions: vec![],
        };

        let view = View::new(select, from, &db, None)?;

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

        let from = FromClause {
            tables: vec![FromTable::Simple("apples".into())],
            conditions: vec![],
        };

        let mut view = View::new(select, from, &db, None)?;

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

    #[test]
    fn view_alias_table() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![InputColumn::Simple(SimpleColumn::Dotted {
                table: "a".to_string(),
                column: "id".to_string(),
            })],
        };

        let from = FromClause {
            tables: vec![FromTable::Alias("apples".into(), "a".into())],
            conditions: vec![],
        };

        let mut view = View::new(select, from, &db, None)?;

        assert_eq!(view.schema().0, [("a.id".into(), Type::Integer)]);

        while let Some(row) = view.next() {
            assert!(row.get(&"id".into()).is_ok());
            assert!(row.get(&"a.id".into()).is_ok());
            assert!(row.get(&"apples.id".into()).is_err());
        }

        Ok(())
    }

    #[test]
    fn view_alias_column() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let select = SelectClause {
            columns: vec![InputColumn::Alias(
                SimpleColumn::String("id".to_string()),
                "test".to_string(),
            )],
        };

        let from = FromClause {
            tables: vec![FromTable::Simple("apples".to_string())],
            conditions: vec![],
        };

        let mut view = View::new(select, from, &db, None)?;

        assert_eq!(view.schema().0, [("test".into(), Type::Integer)]);

        while let Some(row) = view.next() {
            assert!(row.get(&"test".into()).is_ok());
            assert!(row.get(&"id".into()).is_err());
        }
        Ok(())
    }
}
