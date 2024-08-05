use crate::{
    command::{FromTable, InputColumn, SelectClause, SimpleColumn},
    Column, Function, FunctionParam, Table, Type,
};
use anyhow::{anyhow, bail, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRow {
    pub column: Column,
    pub r#type: Type,
}

pub type Schema = Vec<SchemaRow>;

pub fn build_schema(
    select: &SelectClause,
    tables: &[&Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<(Schema, Vec<Column>)> {
    Ok(select
        .columns
        .iter()
        .map(|column| build_schema_column(column, tables, input))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .unzip())
}

fn build_schema_column(
    column: &InputColumn,
    tables: &[&Box<dyn Table + '_>],
    input: &[FromTable],
) -> Result<Vec<(SchemaRow, Column)>> {
    match column {
        InputColumn::Wildcard => Ok(input
            .iter()
            .zip(tables.iter())
            .flat_map(|(from, table)| {
                table.schema().iter().map(|row| {
                    (
                        SchemaRow {
                            column: Column::Dotted {
                                table: from.alias().to_string(),
                                column: row.column.name().to_string(),
                            },
                            r#type: row.r#type,
                        },
                        row.column.clone(),
                    )
                })
            })
            .collect::<Vec<_>>()),
        InputColumn::Simple(column) => {
            let schema = build_schema_simple_column(column, tables, input)?;
            let column = schema.column.clone();
            Ok(vec![(schema, column)])
        }
        InputColumn::Alias(column, alias) => {
            let schema = build_schema_simple_column(column, tables, input)?;
            let column = schema.column.clone();
            let schema = SchemaRow {
                column: match schema.column {
                    Column::Dotted { table, .. } => Column::Dotted {
                        table,
                        column: alias.to_string(),
                    },
                    _ => Column::String(alias.to_string()),
                },
                r#type: schema.r#type,
            };

            Ok(vec![(schema, column)])
        }
    }
}

fn build_schema_simple_column(
    column: &SimpleColumn,
    tables: &[&Box<dyn Table + '_>],
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

            let column = Column::Dotted {
                table: table.to_string(),
                column: schema.column.name().to_string(),
            };

            Ok(SchemaRow {
                column,
                r#type: schema.r#type,
            })
        }
        SimpleColumn::Function { name, param } => {
            let inner = match **param {
                InputColumn::Wildcard => FunctionParam::Wildcard,
                _ => {
                    let inner = build_schema_column(param, tables, input)?;
                    let (sr, _) = &inner[0];
                    FunctionParam::Column(Box::new(sr.column.clone()))
                }
            };

            let (function, r#type) = match name.as_str() {
                "count" => (Function::Count(inner), Type::Integer),
                f => bail!("missing function {f:?}"),
            };

            let column = Column::Function(function);

            Ok(SchemaRow { column, r#type })
        }
    }
}
