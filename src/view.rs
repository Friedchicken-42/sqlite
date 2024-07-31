use std::num::NonZeroUsize;

use anyhow::{anyhow, bail, Result};

use crate::{
    command::{FromTable, InputColumn, Select, SelectClause, SimpleColumn, WhereClause},
    Column, Key, Row, Schema, SchemaRow, Sqlite, Table, TableState, Value,
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
            .iter()
            .position(|row| column.name() == row.column.name());

        let Some(index) = pos else {
            bail!("missing column: {column:?}")
        };

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
            .iter()
            .map(|row| self.get(&row.column))
            .collect::<Result<Vec<_>>>()
    }
}

fn simple_column_schema(
    column: &SimpleColumn,
    tables: &[Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<SchemaRow> {
    match column {
        SimpleColumn::String(name) => {
            let rows = tables
                .iter()
                .flat_map(|table| table.schema())
                .filter(|row| row.column.name() == name)
                .collect::<Vec<_>>();

            match rows[..] {
                [] => bail!("column {name:?} not found"),
                [row] => Ok(row.clone()),
                _ => bail!(""),
            }
        }
        SimpleColumn::Dotted { table, column } => {
            let index = input
                .iter()
                .position(|tbl| *tbl.alias() == *table)
                .ok_or(anyhow!("Missing table {table:?}"))?;

            let schema = tables[index]
                .schema()
                .iter()
                .find(|row| row.column.name() == *column)
                .ok_or(anyhow!("Missing column {column:?}"))?;

            Ok(SchemaRow {
                column: Column::Dotted {
                    table: table.to_string(),
                    column: column.to_string(),
                },
                ..schema.clone()
            })
        }
        SimpleColumn::Function { .. } => bail!("cannot compute a function inside of a view"),
    }
}

fn build_schema(
    select: SelectClause,
    tables: &[Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<(Schema, Vec<String>)> {
    let (schema, output): (Schema, Vec<String>) = select
        .columns
        .iter()
        .map(|column| match column {
            InputColumn::Wildcard => Ok(input
                .iter()
                .zip(tables.iter())
                .flat_map(|(from, table)| {
                    let schema = table.schema();
                    schema.iter().map(|row| {
                        (
                            SchemaRow {
                                column: Column::Dotted {
                                    table: from.alias().to_string(),
                                    column: row.column.name().to_string(),
                                },
                                r#type: row.r#type,
                                key: Key::Other,
                            },
                            row.column.name().to_string(),
                        )
                    })
                })
                .collect::<Vec<_>>()),
            InputColumn::Simple(column) => {
                let schema = simple_column_schema(column, tables, input)?;
                let name = schema.column.name().to_string();
                Ok(vec![(schema, name)])
            }
            InputColumn::Alias(column, alias) => {
                let schema = simple_column_schema(column, tables, input)?;
                let name = schema.column.name().to_string();

                Ok(vec![(
                    SchemaRow {
                        column: match schema.column {
                            Column::String(_) => Column::String(alias.to_string()),
                            Column::Dotted { table, .. } => Column::Dotted {
                                table,
                                column: alias.to_string(),
                            },
                        },
                        ..schema
                    },
                    name,
                )])
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        // .map(|item| ((item.0, item.1, item.2), item.3))
        .unzip();

    Ok((schema, output))
}

#[derive(Debug)]
pub struct View<'a> {
    input: Vec<FromTable>,
    output: Vec<String>,
    tables: Vec<Box<dyn Table + 'a>>,
    r#where: Option<WhereClause<'a>>,
    schema: Schema,
    limit: Option<NonZeroUsize>,
    state: TableState,
    db: &'a Sqlite,
}

impl<'a> View<'a> {
    pub fn new(select: Select<'a>, db: &'a Sqlite) -> Result<Self> {
        let Select {
            select,
            from,
            r#where,
            limit,
            groupby,
        } = select;

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

        let limit = limit.map(|clause| clause.limit);

        Ok(Self {
            input,
            output,
            tables,
            r#where,
            schema,
            limit,
            state: TableState::Next(1),
            db,
        })
    }
}

impl<'a> Table for View<'a> {
    fn current(&self) -> Option<Box<dyn Row<'_> + '_>> {
        if self.state == TableState::End {
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
        self.state = match (self.limit, &self.state) {
            (Some(limit), TableState::Next(index)) if *index > limit.get() => TableState::End,
            (_, TableState::Next(index)) => TableState::Next(index + 1),
            (_, TableState::Start) => TableState::Next(1),
            (_, TableState::End) => TableState::End,
        };

        loop {
            if self.tables.is_empty() {
                self.state = TableState::End;
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
        command::Command, display::DisplayMode, Column, Key, SchemaRow, Sqlite, Table, Type,
    };

    use super::View;

    #[test]
    fn get() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select color, description from apples join oranges";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut view = View::new(select, &db)?;

        assert_eq!(
            view.schema(),
            &[
                SchemaRow {
                    column: "color".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
                SchemaRow {
                    column: "description".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
            ]
        );

        while let Some(row) = view.next() {
            row.get(&"color".into())?;
            row.get(&"description".into())?;
        }

        Ok(())
    }

    #[test]
    fn wildcard() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select * from apples";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut view = View::new(select, &db)?;

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
    fn all_columns() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select *, color from apples join oranges";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let view = View::new(select, &db)?;

        assert_eq!(
            view.schema(),
            &[
                SchemaRow {
                    column: "apples.id".into(),
                    r#type: Type::Integer,
                    key: Key::Other
                },
                SchemaRow {
                    column: "apples.name".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
                SchemaRow {
                    column: "apples.color".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
                SchemaRow {
                    column: "oranges.id".into(),
                    r#type: Type::Integer,
                    key: Key::Other
                },
                SchemaRow {
                    column: "oranges.name".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
                SchemaRow {
                    column: "oranges.description".into(),
                    r#type: Type::Text,
                    key: Key::Other
                },
                SchemaRow {
                    column: Column::String("color".into()),
                    r#type: Type::Text,
                    key: Key::Other
                },
            ]
        );

        use std::io::Cursor;
        let mut f = Cursor::new(vec![]);
        db.display(&mut f, view, DisplayMode::Table)?;
        println!("{}", std::str::from_utf8(f.get_ref())?);

        Ok(())
    }

    #[test]
    fn limit() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select id from apples limit 2";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut view = View::new(select, &db)?;

        let mut count = 0;
        while let Some(row) = view.next() {
            assert!(row.get(&"id".into()).is_ok());
            count += 1;
        }

        assert_eq!(count, 2);

        Ok(())
    }

    #[test]
    fn alias_table() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select a.id from apples as a";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut view = View::new(select, &db)?;

        assert_eq!(
            view.schema(),
            &[SchemaRow {
                column: "a.id".into(),
                r#type: Type::Integer,
                key: Key::Other
            }]
        );

        while let Some(row) = view.next() {
            assert!(row.get(&"id".into()).is_ok());
            assert!(row.get(&"a.id".into()).is_ok());
            assert!(row.get(&"apples.id".into()).is_err());
        }

        Ok(())
    }

    #[test]
    fn alias_column() -> Result<()> {
        let db = Sqlite::read("sample.db")?;

        let query = "select id as test from apples";
        let Command::Select(select) = Command::parse(query)? else {
            bail!("command must be `select`")
        };

        let mut view = View::new(select, &db)?;

        assert_eq!(
            view.schema(),
            &[SchemaRow {
                column: "test".into(),
                r#type: Type::Integer,
                key: Key::Other
            }]
        );

        while let Some(row) = view.next() {
            assert!(row.get(&"test".into()).is_ok());
            assert!(row.get(&"id".into()).is_err());
        }
        Ok(())
    }
}
