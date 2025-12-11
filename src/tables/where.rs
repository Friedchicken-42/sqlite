use crate::{
    Iterator, Row, Rows, Table, Value,
    parser::{Comparison, Expression, Operator, Spanned, WhereStatement},
};

impl WhereStatement {
    fn matches(&self, row: &Row) -> bool {
        match self {
            WhereStatement::Comparison(comparison) => comparison.matches(row),
            WhereStatement::And(left, right) => left.matches(row) && right.matches(row),
            WhereStatement::Or(left, right) => left.matches(row) || right.matches(row),
        }
    }
}

impl Comparison {
    fn matches(&self, row: &Row) -> bool {
        let Comparison { left, op, right } = self;

        fn to_value<'a>(expr: &'a Expression, row: &'a Row) -> Value<'a> {
            // TODO: check this spanned
            match expr {
                Expression::Column(column) => row.get(column.clone()).unwrap(),
                Expression::Literal(s) => Value::Text(s),
                Expression::Number(n) => Value::Integer(*n),
            }
        }

        let left = to_value(left, row);
        let right = to_value(right, row);

        match *op.inner {
            Operator::Less => left.lt(&right),
            Operator::LessEqual => left.le(&right),
            Operator::Equal => left.eq(&right),
            Operator::NotEqual => left.ne(&right),
            Operator::Greater => left.gt(&right),
            Operator::GreaterEqual => left.ge(&right),
        }
    }
}

pub struct Where<'db> {
    pub inner: Box<Table<'db>>,
    pub r#where: Spanned<WhereStatement>,
}

fn write_expr(expr: &Expression, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match expr {
        Expression::Column(column) => write!(f, "{column}"),
        Expression::Literal(s) => write!(f, "{s:?}"),
        Expression::Number(n) => write!(f, "{n}"),
    }
}

pub fn write_stmt(stmt: &WhereStatement, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match stmt {
        WhereStatement::Comparison(comparison) => {
            let Comparison { left, op, right } = &**comparison;
            write_expr(left, f)?;

            let op = match &*op.inner {
                Operator::Less => "<",
                Operator::LessEqual => "<=",
                Operator::Equal => "=",
                Operator::NotEqual => "!=",
                Operator::Greater => ">",
                Operator::GreaterEqual => ">=",
            };
            write!(f, " {op} ")?;

            write_expr(right, f)?;

            Ok(())
        }
        WhereStatement::And(a, b) => {
            write_stmt(a, f)?;
            write!(f, " and ")?;
            write_stmt(b, f)?;

            Ok(())
        }
        WhereStatement::Or(a, b) => {
            write_stmt(a, f)?;
            write!(f, " or ")?;
            write_stmt(b, f)?;

            Ok(())
        }
    }
}

impl<'table> Where<'table> {
    pub fn new(inner: Table<'table>, r#where: Spanned<WhereStatement>) -> Self {
        Self {
            inner: Box::new(inner),
            r#where,
        }
    }

    pub fn rows(&mut self) -> Rows<'_, 'table> {
        Rows::Where(WhereRows {
            rows: Box::new(self.inner.rows()),
            r#where: &self.r#where,
        })
    }

    pub fn write_indented(
        &self,
        f: &mut std::fmt::Formatter,
        width: usize,
        indent: usize,
    ) -> std::fmt::Result {
        let spacer = "  ".repeat(indent);

        self.inner.write_indented(f, width, indent + 1)?;
        write!(f, "{:<width$} │ {}", "where", spacer)?;
        write_stmt(&self.r#where, f)?;
        writeln!(f)?;

        Ok(())
    }
}

pub struct WhereRows<'rows, 'table> {
    rows: Box<Rows<'rows, 'table>>,
    r#where: &'rows WhereStatement,
}

impl Iterator for WhereRows<'_, '_> {
    fn current(&self) -> Option<crate::Row<'_>> {
        self.rows.current()
    }

    fn advance(&mut self) {
        loop {
            self.rows.advance();

            let Some(row) = self.current() else {
                return;
            };

            if self.r#where.matches(&row) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Result, Sqlite, Table, parser::Query};

    use super::*;

    #[test]
    fn r#where() -> Result<()> {
        let db = Sqlite::read("sample.db")?;
        let query = Query::parse("select * from apples where id < 3")?;
        let mut table = db.execute(query)?;

        assert!(matches!(&table, Table::Where(_)));

        let mut i = 0;
        let mut rows = table.rows();
        while let Some(row) = rows.next() {
            i += 1;

            let value = row.get("id".into())?;
            assert_eq!(value, Value::Integer(i));
        }

        assert!(i == 2);

        Ok(())
    }
}
