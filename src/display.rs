use crate::{Iterator, Row, Schema, Serialized, Table, Value};

enum DisplayMode {
    List,
    Table,
}

pub struct DisplayOptions {
    mode: DisplayMode,
    column_sizes: Option<Vec<usize>>,
    separators: [char; 11],
    full_column: bool,
}

impl Default for DisplayOptions {
    fn default() -> Self {
        Self {
            mode: DisplayMode::Table,
            column_sizes: None,
            separators: ['|', '-', '+', '+', '+', '+', '+', '+', '+', '+', '+'],
            full_column: true,
        }
    }
}

impl DisplayOptions {
    pub fn list() -> Self {
        Self {
            mode: DisplayMode::List,
            ..Self::default()
        }
    }

    pub fn table() -> Self {
        Self::default()
    }

    pub fn r#box() -> Self {
        Self {
            separators: ['│', '─', '┌', '┬', '┐', '├', '┼', '┤', '└', '┴', '┘'],
            ..Self::default()
        }
    }
}

fn display_spacer(opts: &DisplayOptions, offset: usize) {
    if let Some(ref sizes) = opts.column_sizes {
        for (i, size) in sizes.iter().enumerate() {
            if i == 0 {
                print!("{}", opts.separators[offset]);
            } else {
                print!("{}", opts.separators[offset + 1]);
            }

            for _ in 0..*size + 2 {
                print!("{}", opts.separators[1]);
            }
        }
    }

    println!("{}", opts.separators[offset + 2]);
}

fn display_spacer_top(opts: &DisplayOptions) {
    display_spacer(opts, 2);
}

fn display_spacer_middle(opts: &DisplayOptions) {
    display_spacer(opts, 5);
}

fn display_spacer_bottom(opts: &DisplayOptions) {
    display_spacer(opts, 8);
}

fn display_schema(schema: &Schema, opts: &DisplayOptions) {
    print!("{}", opts.separators[0]);

    for (i, row) in schema.columns.iter().enumerate() {
        let width = match &opts.column_sizes {
            Some(arr) => arr[i],
            None => 0,
        };

        let col = row.column.name();

        match width {
            0 => print!("{}{}", col, opts.separators[0]),
            _ => print!(" {:^width$} {}", col, opts.separators[0]),
        };
    }

    println!();
}

fn display_value(value: Value, index: usize, opts: &DisplayOptions) {
    if index == 0 {
        print!("{}", opts.separators[0]);
    }

    let width = match &opts.column_sizes {
        Some(arr) => arr[index],
        None => 0,
    };

    let value = value.to_string();

    if width == 0 {
        print!("{value}{}", opts.separators[0]);
    } else {
        print!(" {value:<width$} {}", opts.separators[0]);
    }
}

fn display_row(row: Row, schema: &Schema, opts: &DisplayOptions) {
    for (i, sr) in schema.columns.iter().enumerate() {
        let value = row.get(sr.column.clone()).unwrap();
        display_value(value, i, opts);
    }

    println!()
}

fn display_table(table: &mut Table<'_>, opts: DisplayOptions) {
    const __BACKUP: bool = true;

    const BACKUP_SIZE: usize = 20;
    let mut backup: Vec<Vec<Serialized>> = Vec::with_capacity(BACKUP_SIZE);
    let mut ended = false;

    let schema = table.schema().clone();

    let mut sizes = schema
        .columns
        .iter()
        .map(|sr| sr.column.name().len())
        .collect::<Vec<_>>();

    let mut rows = table.rows();

    let opts = if __BACKUP {
        for _ in 0..BACKUP_SIZE {
            let Some(row) = rows.next() else {
                ended = true;
                break;
            };

            let mut vec = vec![];

            for sr in schema.columns.iter() {
                let value = row.get(sr.column.clone()).unwrap();
                let serialized = value.serialize();
                vec.push(serialized);
            }

            backup.push(vec);
        }

        for values in &backup {
            for (i, serialized) in values.iter().enumerate() {
                let value = Value::read(&serialized.data, &serialized.varint).unwrap();
                sizes[i] = sizes[i].max(value.to_string().len());
            }
        }

        DisplayOptions {
            column_sizes: Some(sizes),
            ..opts
        }
    } else {
        DisplayOptions {
            column_sizes: Some(sizes),
            ..opts
        }
    };

    display_spacer_top(&opts);
    display_schema(table.schema(), &opts);
    display_spacer_middle(&opts);

    let mut rows = table.rows();

    if __BACKUP {
        for _ in 0..BACKUP_SIZE {
            rows.next();
        }

        for row in backup.into_iter() {
            for (i, serialized) in row.iter().enumerate() {
                let value = Value::read(&serialized.data, &serialized.varint).unwrap();
                display_value(value, i, &opts);
            }

            println!();
        }

        if !ended {
            while let Some(row) = rows.next() {
                display_row(row, &schema, &opts);
            }
        }
    } else {
        while let Some(row) = rows.next() {
            display_row(row, &schema, &opts);
        }
    }

    display_spacer_bottom(&opts);
}

fn display_list(table: &mut Table<'_>, opts: DisplayOptions) {
    let schema = table.schema().clone();

    display_schema(&schema, &opts);

    let mut rows = table.rows();
    while let Some(row) = rows.next() {
        display_row(row, &schema, &opts);
    }
}

pub trait Printable {
    fn display(&mut self, options: DisplayOptions);
}

impl Printable for Table<'_> {
    fn display(&mut self, options: DisplayOptions) {
        match options.mode {
            DisplayMode::List => display_list(self, options),
            DisplayMode::Table => display_table(self, options),
        }
    }
}
