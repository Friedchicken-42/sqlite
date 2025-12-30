use crate::{
    Result, Sqlite, Table,
    parser::{Comparison, From, Select, SelectStatement, Spanned, WhereStatement},
    tables::{limit::Limit, view::View, r#where::Where},
};

pub enum Physical {
    Project {
        table: Spanned<Physical>,
        // TODO: replace this `Select` with something else
        // should not contains functions
        select: Spanned<Vec<Spanned<Select>>>,
    },
    Filter {
        table: Spanned<Physical>,
        condition: Spanned<WhereStatement>,
    },
    TableScan {
        table: Spanned<String>,
        alias: Option<Spanned<String>>,
    },
    NestedLoop {
        left: Spanned<Physical>,
        right: Spanned<Physical>,
        on: Option<Spanned<Comparison>>,
    },
    Limit {
        table: Spanned<Physical>,
        limit: usize,
        offset: usize,
    },
}

impl Sqlite {
    pub fn physical_from_builder(&self, from: Spanned<From>) -> Result<Spanned<Physical>> {
        match *from.inner {
            From::Table { table, alias } => {
                let scan = Physical::TableScan { table, alias };
                Ok(Spanned::span(scan, from.span))
            }
            From::Subquery { query, alias } => panic!("subquery yet not supported"),
            From::Join { left, right, on } => {
                let left = self.physical_from_builder(left)?;
                let right = self.physical_from_builder(right)?;
                let join = Physical::NestedLoop { left, right, on };

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
            let span = select_clause.span.clone();
            let project = Physical::Project {
                table,
                select: select_clause,
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

    pub fn table_builder(&self, physical: Spanned<Physical>) -> Result<Table<'_>> {
        match *physical.inner {
            Physical::Project { table, select } => {
                let inner = self.table_builder(table)?;
                let view = View::new(select, inner)?;
                Ok(Table::View(view))
            }
            Physical::Filter { table, condition } => {
                let inner = self.table_builder(table)?;
                let r#where = Where::new(inner, condition);
                Ok(Table::Where(r#where))
            }
            Physical::TableScan { table, alias } => {
                let mut btreepage = self.table(&table).map_err(|e| e.span(table.span.clone()))?;

                if let Some(alias) = alias {
                    btreepage.add_alias(*alias.inner);
                }

                Ok(Table::BTreePage(btreepage))
            }
            Physical::NestedLoop { left, right, on } => todo!(),
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
