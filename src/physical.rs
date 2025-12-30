use std::{collections::HashSet, fmt::Display, hash::Hash};

use crate::{
    Column, ErrorKind, Result, Schema, SchemaRow, Sqlite, SqliteError, Table, Type,
    parser::{
        Comparison, Expression, From, Operator, Select, SelectStatement, Spanned, WhereStatement,
    },
    tables::{
        btreepage::{Page, PageType, Storage},
        indexseek::IndexSeek,
        join::Join,
        limit::Limit,
        view::View,
        r#where::Where,
    },
};

#[derive(Clone, Debug)]
pub enum SortCol {
    Rowid,
    Column(Column),
}

#[derive(Clone, Debug)]
pub struct Metadata {
    sort: Vec<SortCol>,
    records: usize,
    pages: usize,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            sort: vec![SortCol::Rowid],
            records: 0,
            pages: 0,
        }
    }
}

impl Metadata {
    fn heuristic(page: &Page, schema: &Schema) -> Result<Self> {
        let pages = match page.r#type()? {
            PageType::TableLeaf | PageType::IndexLeaf => 1,
            PageType::TableInterior | PageType::IndexInterior => page.pointers()?.count() + 1,
        };

        let records = match page.r#type()? {
            PageType::TableLeaf | PageType::IndexLeaf => page.pointers()?.count(),
            PageType::TableInterior | PageType::IndexInterior => {
                let mut size = 0;
                for sr in &schema.columns {
                    size += match sr.r#type {
                        Type::Null => 0,
                        Type::Integer | Type::Float => 4,
                        Type::Text | Type::Blob => 20,
                    };
                }

                let records = page.data.len().saturating_sub(20) / (size);
                page.pointers()?.count() * records
            }
        };

        let sort = match page.r#type()? {
            PageType::TableLeaf | PageType::TableInterior => vec![SortCol::Rowid],
            PageType::IndexLeaf | PageType::IndexInterior => schema
                .columns
                .iter()
                .filter(|sr| sr.column.name() != "index")
                .map(|sr| SortCol::Column(*sr.column.inner.clone()))
                .collect(),
        };

        Ok(Self {
            sort,
            records,
            pages,
        })
    }
}

#[derive(Clone)]
pub enum Physical {
    Project {
        table: Spanned<Physical>,
        schema: Schema,
        inner_columns: Vec<Spanned<Column>>,
    },
    Filter {
        table: Spanned<Physical>,
        condition: Spanned<WhereStatement>,
    },
    TableScan {
        table: Spanned<String>,
        alias: Option<Spanned<String>>,
        schema: Schema,
        metadata: Metadata,
    },
    IndexFilter {
        table: Spanned<String>,
        index: String,
        columns: Vec<Column>,
        expressions: Vec<Expression>,
        schema: Schema,
        metadata: Metadata,
    },
    NestedLoop {
        left: Spanned<Physical>,
        right: Spanned<Physical>,
        schema: Schema,
        on: Option<Spanned<Comparison>>,
    },
    IndexNestedLoop {
        left: Spanned<Physical>,
        right: Spanned<Physical>,
        schema: Schema,
        on: Spanned<Comparison>,
    },
    Limit {
        table: Spanned<Physical>,
        limit: usize,
        offset: usize,
    },
}

impl Hash for Physical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
    }
}

impl PartialEq for Physical {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Project {
                    table: l_table,
                    schema: l_schema,
                    ..
                },
                Self::Project {
                    table: r_table,
                    schema: r_schema,
                    ..
                },
            ) => *l_table == *r_table && l_schema == r_schema,
            (
                Self::Filter {
                    table: l_table,
                    condition: l_condition,
                },
                Self::Filter {
                    table: r_table,
                    condition: r_condition,
                },
            ) => *l_table == *r_table && *l_condition == *r_condition,
            (
                Self::TableScan {
                    table: l_table,
                    alias: l_alias,
                    ..
                },
                Self::TableScan {
                    table: r_table,
                    alias: r_alias,
                    ..
                },
            ) => *l_table == *r_table && *l_alias == *r_alias,
            (
                Self::IndexFilter {
                    table: l_table,
                    index: l_index,
                    columns: l_columns,
                    expressions: l_expressions,
                    ..
                },
                Self::IndexFilter {
                    table: r_table,
                    index: r_index,
                    columns: r_columns,
                    expressions: r_expressions,
                    ..
                },
            ) => {
                *l_table == *r_table
                    && l_index == r_index
                    && l_columns == r_columns
                    && l_expressions == r_expressions
            }
            (
                Self::NestedLoop {
                    left: l_left,
                    right: l_right,
                    on: l_on,
                    schema: l_schema,
                },
                Self::NestedLoop {
                    left: r_left,
                    right: r_right,
                    on: r_on,
                    schema: r_schema,
                },
            ) => *l_left == *r_left && *l_right == *r_right && l_on == r_on && l_schema == r_schema,
            (
                Self::IndexNestedLoop {
                    left: l_left,
                    right: l_right,
                    on: l_on,
                    schema: l_schema,
                },
                Self::IndexNestedLoop {
                    left: r_left,
                    right: r_right,
                    on: r_on,
                    schema: r_schema,
                },
            ) => *l_left == *r_left && *l_right == *r_right && l_on == r_on && l_schema == r_schema,
            (
                Self::Limit {
                    table: l_table,
                    limit: l_limit,
                    offset: l_offset,
                },
                Self::Limit {
                    table: r_table,
                    limit: r_limit,
                    offset: r_offset,
                },
            ) => *l_table == *r_table && l_limit == r_limit && l_offset == r_offset,
            _ => false,
        }
    }
}

impl Eq for Physical {}

impl Display for Physical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_indented(f, "")
    }
}

impl Physical {
    fn metadata(&self) -> Metadata {
        match self {
            Physical::Project { table, .. } => table.metadata(),
            Physical::Filter { table, .. } => table.metadata(),
            Physical::TableScan { metadata, .. } => metadata.clone(),
            Physical::IndexFilter { metadata, .. } => metadata.clone(),
            Physical::NestedLoop { left, right, .. }
            | Physical::IndexNestedLoop { left, right, .. } => {
                let left = left.metadata();
                let right = right.metadata();

                Metadata {
                    sort: vec![],
                    records: left.records * right.records,
                    pages: left.pages + left.records * right.pages,
                }
            }
            Physical::Limit {
                table,
                limit,
                offset,
            } => {
                let m = table.metadata();
                let t = limit + offset;

                Metadata {
                    records: if t < m.records { t } else { m.records },
                    ..m
                }
            }
        }
    }

    fn schema(&self) -> Schema {
        match self {
            Physical::Project { schema, .. } => schema.clone(),
            Physical::Filter { table, .. } => table.schema(),
            Physical::TableScan { schema, .. } => schema.clone(),
            Physical::IndexFilter { schema, .. } => schema.clone(),
            Physical::NestedLoop { schema, .. } => schema.clone(),
            Physical::IndexNestedLoop { schema, .. } => schema.clone(),
            Physical::Limit { table, .. } => table.schema(),
        }
    }

    fn write_indented_rec(
        &self,
        f: &mut std::fmt::Formatter,
        prefix: &str,
        is_last: bool,
    ) -> std::fmt::Result {
        let connector = if is_last { "└" } else { "├" };
        let branch_prefix = if is_last { "   " } else { "│  " };

        write!(f, "{prefix}{connector}─ ")?;

        let new_prefix = format!("{}{}", prefix, branch_prefix);

        self.write_indented(f, &new_prefix)
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        match self {
            Physical::Project { table, schema, .. } => {
                let rows = schema
                    .columns
                    .iter()
                    .map(|s| s.column.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                writeln!(f, "Project {{ {rows} }}")?;
                table.write_indented_rec(f, prefix, true)
            }
            Physical::Filter { table, condition } => {
                writeln!(f, "Filter {{ {} }}", *condition.inner)?;
                table.write_indented_rec(f, prefix, true)
            }
            Physical::TableScan { table, alias, .. } => {
                write!(f, "TableScan {{ ")?;
                if let Some(alias) = alias {
                    write!(f, "{}, ", *alias.inner)?;
                }
                writeln!(f, "{} }}", *table.inner)
            }
            Physical::IndexFilter {
                table,
                index,
                columns,
                ..
            } => {
                let cols = columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                writeln!(
                    f,
                    "IndexFilter {{ {}, {}, ({}) }}",
                    *table.inner, index, cols
                )
            }
            Physical::NestedLoop {
                left, right, on, ..
            } => {
                write!(f, "NestedLoop")?;

                if let Some(comp) = on {
                    write!(f, " {{ {} }}", *comp.inner)?;
                }
                writeln!(f)?;
                left.write_indented_rec(f, prefix, false)?;
                right.write_indented_rec(f, prefix, true)
            }
            Physical::IndexNestedLoop {
                left, right, on, ..
            } => {
                writeln!(f, "IndexNestedLoop {{ {} }}", *on.inner)?;
                left.write_indented_rec(f, prefix, false)?;
                right.write_indented_rec(f, prefix, true)
            }
            Physical::Limit {
                table,
                limit,
                offset,
            } => {
                writeln!(f, "Limit {{ {limit} }} Skip {{ {offset} }}")?;
                table.write_indented_rec(f, prefix, true)
            }
        }
    }
}

fn create_schema(
    schema: &Schema,
    select: &[Spanned<Select>],
) -> Result<(Schema, Vec<Spanned<Column>>)> {
    let srs = select
        .iter()
        .map(|row| match &**row {
            Select::Wildcard => {
                let srs = schema
                    .columns
                    .iter()
                    .map(|sr| {
                        let span = row.span.clone();
                        let column = sr.column.clone().with_span(span);
                        let sr = SchemaRow {
                            column: column.clone(),
                            ..*sr
                        };
                        (sr, column)
                    })
                    .collect::<Vec<_>>();

                Ok(srs)
            }
            Select::Column { name, table, alias } => {
                let sr = schema
                    .columns
                    .iter()
                    .find(|sr| sr.column.name() == **name)
                    .ok_or(SqliteError::new(
                        ErrorKind::ColumnNotFound(name.as_str().into()),
                        row.span.clone(),
                    ))?
                    .clone();

                let column = match table {
                    Some(table) => Column::Dotted {
                        table: table.to_string(),
                        column: name.to_string(),
                    },
                    None => Column::Single(name.to_string()),
                };

                let outer = match alias {
                    Some(alias) => Column::Single(alias.to_string()),
                    None => column.clone(),
                };

                let column = Spanned::span(column, row.span.clone());

                let sr = SchemaRow {
                    column: Spanned::span(outer, row.span.clone()),
                    ..sr
                };

                Ok(vec![(sr, column)])
            }
            Select::Function { .. } => panic!("functions not yet supported"),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .fold((vec![], vec![]), |(mut s, mut n), (a, b)| {
            s.push(a);
            n.push(b);
            (s, n)
        });

    Ok((
        Schema {
            names: schema.names.clone(),
            name: schema.name,
            columns: srs.0,
            primary: vec![],
        },
        srs.1,
    ))
}

fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut columns = vec![];
    for schema in [left, right] {
        for sr in &schema.columns {
            let column = match schema.current_name() {
                Some(table) => Column::Dotted {
                    table: table.to_string(),
                    column: sr.column.name().into(),
                },
                None => *sr.column.inner.clone(),
            };

            let sr = SchemaRow {
                column: Spanned::span(column, sr.column.span.clone()),
                r#type: sr.r#type,
            };

            columns.push(sr);
        }
    }

    let names = [left.names.clone(), right.names.clone()].concat();

    Schema {
        names,
        name: None,
        columns,
        primary: vec![],
    }
}

impl Sqlite {
    pub fn physical_from_builder(&self, from: Spanned<From>) -> Result<Spanned<Physical>> {
        match *from.inner {
            From::Table { table, alias } => {
                let btree = self.table(&table)?;
                let metadata = match btree.storage {
                    Storage::Memory { pages, .. } => {
                        let page = pages.first().expect("should have at least one page");
                        Metadata::heuristic(page, &btree.schema)?
                    }
                    Storage::File { root, .. } => {
                        let page = self.page(root)?;
                        Metadata::heuristic(&page, &btree.schema)?
                    }
                };

                let mut schema = btree.schema.clone();
                if let Some(alias) = &alias {
                    schema.names.push(alias.to_string());
                    schema.name = Some(schema.names.len() - 1);
                }

                let scan = Physical::TableScan {
                    table,
                    alias,
                    metadata,
                    schema,
                };
                Ok(Spanned::span(scan, from.span))
            }
            From::Subquery { .. } => panic!("subquery yet not supported"),
            From::Join { left, right, on } => {
                let left = self.physical_from_builder(left)?;
                let right = self.physical_from_builder(right)?;
                let schema = merge_schemas(&left.schema(), &right.schema());
                let join = Physical::NestedLoop {
                    left,
                    right,
                    on,
                    schema,
                };

                Ok(Spanned::span(join, from.span))
            }
        }
    }

    pub fn physical_builder(&self, select: Spanned<SelectStatement>) -> Result<Spanned<Physical>> {
        let SelectStatement {
            select_clause,
            from_clause,
            where_clause,
            groupby_clause,
            limit_clause,
        } = *select.inner;

        let mut table = self.physical_from_builder(from_clause)?;

        if let Some(r#where) = where_clause {
            let span = r#where.span.clone();
            let filter = Physical::Filter {
                table,
                condition: r#where,
            };

            table = Spanned::span(filter, span);
        }

        if let Some(first) = select_clause.first()
            && *first.inner != Select::Wildcard
        {
            let span = table.span.clone();
            let (schema, inner_columns) = create_schema(&table.schema(), &select_clause)?;
            let project = Physical::Project {
                table,
                schema,
                inner_columns,
            };

            table = Spanned::span(project, span);
        }

        if let Some(limit_stmt) = limit_clause {
            let limit = Physical::Limit {
                table,
                limit: limit_stmt.limit,
                offset: 0,
            };

            table = Spanned::span(limit, limit_stmt.span);
        }

        Ok(table)
    }

    pub fn optimize(&self, physical: Spanned<Physical>) -> Spanned<Physical> {
        let span = physical.span.clone();
        let mut all = HashSet::from([*physical.inner]);

        let rules = [
            switch_loop,
            tablescan_to_indexfilter,
            join_indexfilter,
            // project_after_join,
        ];

        loop {
            let mut new = vec![];

            for rule in rules {
                for p in &all {
                    for out in apply(self, p, rule) {
                        new.push(out);
                    }
                }
            }

            let orig = all.len();
            for n in new {
                all.insert(n);
            }

            if orig == all.len() {
                break;
            }
        }

        println!("---\n");
        for p in &all {
            println!("{p}{:?}\n", p.metadata());
        }
        println!("---");

        let best = all
            .into_iter()
            .max_by_key(|p| p.metadata().pages)
            .expect("a rule deleted the first node");

        println!("query cost: {:?}", best.metadata());
        Spanned::span(best, span)
    }

    pub fn table_builder(&self, physical: Spanned<Physical>) -> Result<Table<'_>> {
        match *physical.inner {
            Physical::Project {
                table,
                schema,
                inner_columns,
            } => {
                let inner = self.table_builder(table)?;
                let view = View {
                    inner: Box::new(inner),
                    schema,
                    inner_columns,
                };
                Ok(Table::View(view))
            }
            Physical::Filter { table, condition } => {
                let inner = self.table_builder(table)?;
                let r#where = Where::new(inner, condition);
                Ok(Table::Where(r#where))
            }
            Physical::TableScan { table, alias, .. } => {
                let mut btreepage = self.table(&table).map_err(|e| e.span(table.span.clone()))?;

                if let Some(alias) = alias {
                    btreepage.add_alias(*alias.inner);
                }

                Ok(Table::BTreePage(btreepage))
            }
            Physical::IndexFilter {
                table,
                index,
                columns,
                expressions,
                ..
            } => {
                let span = table.span.clone();
                let table = self.table(&table).map_err(|e| e.span(span.clone()))?;
                let index = self.table(&index).map_err(|e| e.span(span.clone()))?;

                let indexseek = IndexSeek {
                    table,
                    index,
                    columns,
                    expressions,
                };

                Ok(Table::IndexSeek(indexseek))
            }
            Physical::NestedLoop {
                left,
                right,
                on,
                schema,
            } => {
                let left = self.table_builder(left)?;
                let right = self.table_builder(right)?;
                let join = Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                    schema,
                };

                Ok(Table::Join(join))
            }
            Physical::IndexNestedLoop {
                left,
                right,
                on,
                schema,
            } => todo!(),
            Physical::Limit {
                table,
                limit,
                offset,
            } => {
                let inner = self.table_builder(table)?;
                let limit = Limit::new(inner, limit, offset);
                Ok(Table::Limit(limit))
            }
        }
    }
}

type Rule = fn(&Sqlite, &Physical) -> Vec<Physical>;

fn apply(db: &Sqlite, physical: &Physical, rule: Rule) -> Vec<Physical> {
    // TODO: too many `collect`
    let inners: Vec<Physical> = match physical {
        Physical::Project {
            table,
            schema,
            inner_columns,
        } => apply(db, table, rule)
            .into_iter()
            .map(|inner| Physical::Project {
                table: Spanned::span(inner, table.span.clone()),
                schema: schema.clone(),
                inner_columns: inner_columns.clone(),
            })
            .collect(),
        Physical::Filter { table, condition } => apply(db, table, rule)
            .into_iter()
            .map(|inner| Physical::Filter {
                table: Spanned::span(inner, table.span.clone()),
                condition: condition.clone(),
            })
            .collect(),
        p @ Physical::TableScan { .. } => vec![p.clone()],
        p @ Physical::IndexFilter { .. } => vec![p.clone()],
        Physical::NestedLoop {
            left,
            right,
            on,
            schema,
        } => {
            let l = apply(db, left, rule)
                .into_iter()
                .map(|inner| Physical::NestedLoop {
                    left: Spanned::span(inner, left.span.clone()),
                    right: right.clone(),
                    on: on.clone(),
                    schema: schema.clone(),
                });

            let r = apply(db, right, rule)
                .into_iter()
                .map(|inner| Physical::NestedLoop {
                    left: left.clone(),
                    right: Spanned::span(inner, right.span.clone()),
                    on: on.clone(),
                    schema: schema.clone(),
                });

            l.chain(r).collect()
        }
        Physical::IndexNestedLoop {
            left,
            right,
            on,
            schema,
        } => {
            let l = apply(db, left, rule)
                .into_iter()
                .map(|inner| Physical::IndexNestedLoop {
                    left: Spanned::span(inner, left.span.clone()),
                    right: right.clone(),
                    on: on.clone(),
                    schema: schema.clone(),
                });

            let r = apply(db, right, rule)
                .into_iter()
                .map(|inner| Physical::IndexNestedLoop {
                    left: left.clone(),
                    right: Spanned::span(inner, right.span.clone()),
                    on: on.clone(),
                    schema: schema.clone(),
                });

            l.chain(r).collect()
        }
        Physical::Limit {
            table,
            limit,
            offset,
        } => apply(db, table, rule)
            .into_iter()
            .map(|inner| Physical::Limit {
                table: Spanned::span(inner, table.span.clone()),
                limit: *limit,
                offset: *offset,
            })
            .collect(),
    };

    rule(db, physical).into_iter().chain(inners).collect()
}

fn switch_loop(_: &Sqlite, physical: &Physical) -> Vec<Physical> {
    if let Physical::NestedLoop {
        left,
        right,
        on,
        schema,
    } = physical
    {
        vec![Physical::NestedLoop {
            left: right.clone(),
            right: left.clone(),
            on: on.clone(),
            schema: schema.clone(),
        }]
    } else {
        vec![]
    }
}

fn tablescan_to_indexfilter(db: &Sqlite, physical: &Physical) -> Vec<Physical> {
    if let Physical::Filter {
        table: tf,
        condition,
    } = physical
        && let WhereStatement::Comparison(comp) = &**condition
        && let Comparison { left, op, right } = &**comp
        && let Expression::Column(col) = &**left
        && **op == Operator::Equal
        && let Physical::TableScan {
            table,
            alias, // TODO: check this alias
            metadata,
            ..
        } = &**tf
    {
        db.indexes(table, &[col.name().to_string()])
            .unwrap_or_default()
            .into_iter()
            .map(|idx| {
                let index = idx
                    .schema
                    .current_name()
                    .expect("index should have a name")
                    .to_string();

                Physical::IndexFilter {
                    table: table.clone(),
                    index,
                    columns: vec![col.clone()],
                    expressions: vec![(**right).clone()],
                    schema: idx.schema.clone(),
                    metadata: Metadata {
                        sort: vec![SortCol::Column(col.clone())],
                        records: metadata.records,
                        pages: metadata.pages.div_ceil(10).max(1),
                    },
                }
            })
            .collect()
    } else {
        vec![]
    }
}

fn join_indexfilter(db: &Sqlite, physical: &Physical) -> Vec<Physical> {
    if let Physical::NestedLoop {
        left,
        right,
        on,
        schema,
    } = physical
        && let Some(comp) = on
        && let Expression::Column(_) = &*comp.left
        && *comp.op == Operator::Equal
        && let Expression::Column(_) = &*comp.right
    {
        let r = Physical::Filter {
            table: right.clone(),
            condition: Spanned::span(WhereStatement::Comparison(comp.clone()), comp.span.clone()),
        };

        tablescan_to_indexfilter(db, &r)
            .into_iter()
            .map(|p| Physical::IndexNestedLoop {
                left: left.clone(),
                right: Spanned::span(p, right.span.clone()),
                on: comp.clone(),
                schema: schema.clone(),
            })
            .collect()
    } else {
        vec![]
    }
}

/*
fn project_after_loop(_: &Sqlite, physical: &Physical) -> Vec<Physical> {
    if let Physical::Project { table, select } = physical
        && let Physical::NestedLoop { left, right, on } = &**table
    {
        todo!()
    } else {
        vec![]
    }
}
*/

/*
fn indexonly(db: &Sqlite, physical: &Physical) -> Vec<Physical> {
    let all_in_project = |select: &[Spanned<Select>], columns: &[Column]| -> bool {
        select.iter().all(|s| match &**s {
            Select::Column { name, .. } => {
                columns.iter().find(|c| c.name() == name.as_str()).is_some()
            }
            _ => false,
        })
    };

    if let Physical::Project { table, select } = physical
        && let Physical::IndexFilter {
            table,
            index,
            columns,
            expressions,
            metadata,
        } = &**table
        && all_in_project(select, columns)
    {
        Physical::IndexOnlyScan {}
    } else {
        vec![]
    }
}
*/
