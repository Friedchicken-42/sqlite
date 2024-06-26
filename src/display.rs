use std::io::Write;

use anyhow::Result;

use crate::{Rows, Table};

#[derive(Debug)]
pub enum DisplayMode {
    List,
    Table,
}

pub fn display_list(mut table: Table<'_>, f: &mut impl Write) -> Result<()> {
    let schema = table.schema();

    for (i, (name, _)) in schema.0.iter().enumerate() {
        if i != 0 {
            write!(f, "|")?;
        }

        write!(f, "{name}")?;
    }

    writeln!(f)?;

    while let Some(row) = table.next() {
        for (i, value) in row.all()?.iter().enumerate() {
            if i != 0 {
                write!(f, "|")?;
            }

            write!(f, "{value}")?;
        }

        writeln!(f)?;
    }
    Ok(())
}

pub fn display_table(mut table: Table<'_>, f: &mut impl Write) -> Result<()> {
    let backup_size = 10;
    let mut backup: Vec<Vec<Value>> = Vec::with_capacity(backup_size);

    for _ in 0..backup_size {
        let Some(row) = self.next() else {
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

    let schema = self.schema();

    let mut sizes = schema
        .0
        .iter()
        .map(|(name, _)| name.len())
        .collect::<Vec<_>>();

    for values in &backup {
        for (i, value) in values.iter().enumerate() {
            sizes[i] = sizes[i].max(value.to_string().len());
        }
    }

    print!("+");
    for size in &sizes {
        for _ in 0..*size + 2 {
            print!("-");
        }
        print!("+");
    }
    println!();

    print!("|");
    for (i, (name, _)) in schema.0.iter().enumerate() {
        let width = sizes[i];
        print!(" {name:^width$} |");
    }
    println!();

    print!("+");
    for size in &sizes {
        for _ in 0..*size + 2 {
            print!("-");
        }
        print!("+");
    }
    println!();

    for row in backup.iter() {
        print!("|");
        for (i, value) in row.iter().enumerate() {
            let width = sizes[i];
            let value = value.to_string();
            print!(" {value:<width$} |");
        }
        println!();
    }

    while let Some(row) = self.next() {
        print!("|");
        for (i, value) in row.all()?.iter().enumerate() {
            let width = sizes[i];
            let value = value.to_string();
            print!(" {value:<width$} |");
        }
        println!();
    }

    print!("+");
    for size in &sizes {
        for _ in 0..*size + 2 {
            print!("-");
        }
        print!("+");
    }
    println!();
    Ok(())
}
