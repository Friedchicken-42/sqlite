use std::{borrow::Cow, io::Write};

use anyhow::Result;

use crate::{Row, Schema, Table, Value};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DisplayMode {
    List,
    Table,
    Box,
}

struct DisplayOptions {
    column_sizes: Option<Vec<usize>>,
    separators: [char; 11],
    full_column: bool,
}

impl DisplayOptions {
    fn r#box(self) -> Self {
        Self {
            separators: ['│', '─', '┌', '┬', '┐', '├', '┼', '┤', '└', '┴', '┘'],
            ..self
        }
    }
}

impl Default for DisplayOptions {
    fn default() -> Self {
        Self {
            column_sizes: None,
            separators: ['|', '-', '+', '+', '+', '+', '+', '+', '+', '+', '+'],
            full_column: true,
        }
    }
}

fn display_spacer_start(f: &mut impl Write, opts: &DisplayOptions) -> Result<()> {
    display_spacer(f, opts, 2)
}
fn display_spacer_middle(f: &mut impl Write, opts: &DisplayOptions) -> Result<()> {
    display_spacer(f, opts, 5)
}
fn display_spacer_end(f: &mut impl Write, opts: &DisplayOptions) -> Result<()> {
    display_spacer(f, opts, 8)
}

fn display_spacer(f: &mut impl Write, opts: &DisplayOptions, offset: usize) -> Result<()> {
    if let Some(ref sizes) = opts.column_sizes {
        for (i, size) in sizes.iter().enumerate() {
            if i == 0 {
                write!(f, "{}", opts.separators[offset])?;
            } else {
                write!(f, "{}", opts.separators[offset + 1])?;
            }

            for _ in 0..*size + 2 {
                write!(f, "{}", opts.separators[1])?;
            }
        }
    }

    writeln!(f, "{}", opts.separators[offset + 2])?;

    Ok(())
}

fn display_schema(f: &mut impl Write, schema: &Schema, opts: &DisplayOptions) -> Result<()> {
    write!(f, "{}", opts.separators[0])?;

    for (i, row) in schema.iter().enumerate() {
        let width = match &opts.column_sizes {
            Some(arr) => arr[i],
            None => 0,
        };

        let col = match opts.full_column {
            true => &row.column.full(),
            false => row.column.name(),
        };

        match width {
            0 => write!(f, "{}{}", col, opts.separators[0]),
            _ => write!(f, " {:^width$} {}", col, opts.separators[0]),
        }?;
    }

    writeln!(f)?;

    Ok(())
}

fn display_value(
    f: &mut impl Write,
    value: &Value,
    index: usize,
    opts: &DisplayOptions,
) -> Result<()> {
    if index == 0 {
        write!(f, "{}", opts.separators[0])?;
    }

    let width = match &opts.column_sizes {
        Some(arr) => arr[index],
        None => 0,
    };

    let value = value.to_string();

    if width == 0 {
        write!(f, "{value}{}", opts.separators[0])?;
    } else {
        write!(f, " {value:<width$} {}", opts.separators[0])?;
    }
    Ok(())
}

fn display_row<'a>(f: &mut impl Write, row: impl Row<'a>, opts: &DisplayOptions) -> Result<()> {
    for (i, value) in row.all()?.iter().enumerate() {
        display_value(f, value, i, opts)?;
    }

    writeln!(f)?;

    Ok(())
}

fn display_list(f: &mut impl Write, mut table: impl Table, opts: DisplayOptions) -> Result<()> {
    let schema = table.schema();

    display_schema(f, schema, &opts)?;

    while let Some(row) = table.next() {
        display_row(f, row, &opts)?;
    }
    Ok(())
}

fn display_table(f: &mut impl Write, mut table: impl Table, opts: DisplayOptions) -> Result<()> {
    const BACKUP_SIZE: usize = 20;
    let mut backup: Vec<Vec<Value>> = Vec::with_capacity(BACKUP_SIZE);

    for _ in 0..BACKUP_SIZE {
        let Some(row) = table.next() else {
            break;
        };

        let all = row.all()?;
        let mut vec = vec![];

        for value in all {
            let value = match value {
                Value::Null => Value::Null,
                Value::Integer(i) => Value::Integer(i),
                Value::Float(f) => Value::Float(f),
                Value::Text(t) => Value::Text(Cow::Owned(t.to_string())),
                Value::Blob(b) => Value::Blob(Cow::Owned(b.to_vec())),
            };

            vec.push(value.clone())
        }

        backup.push(vec);
    }

    let schema = table.schema();

    let mut sizes = schema
        .iter()
        .map(|row| match opts.full_column {
            true => row.column.full().len(),
            false => row.column.name().len(),
        })
        .collect::<Vec<_>>();

    for values in &backup {
        for (i, value) in values.iter().enumerate() {
            sizes[i] = sizes[i].max(value.to_string().len());
        }
    }

    let opts = DisplayOptions {
        column_sizes: Some(sizes),
        ..opts
    };

    display_spacer_start(f, &opts)?;

    display_schema(f, schema, &opts)?;

    display_spacer_middle(f, &opts)?;

    for row in backup.iter() {
        for (i, value) in row.iter().enumerate() {
            display_value(f, value, i, &opts)?;
        }

        writeln!(f)?;
    }

    while let Some(row) = table.next() {
        display_row(f, row, &opts)?;
    }

    display_spacer_end(f, &opts)?;

    Ok(())
}

pub fn display(f: &mut impl Write, table: impl Table, mode: DisplayMode) -> Result<()> {
    let options = DisplayOptions::default();

    match mode {
        DisplayMode::List => display_list(f, table, options)?,
        DisplayMode::Table => display_table(f, table, options)?,
        DisplayMode::Box => display_table(f, table, options.r#box())?,
    };

    f.flush()?;

    Ok(())
}
