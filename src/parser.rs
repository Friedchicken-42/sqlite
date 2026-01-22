use std::{fmt::Display, ops::Range};

use chumsky::{
    Parser,
    container::Seq,
    extra::ParserExtra,
    input::MapExtra,
    pratt::{infix, left},
    prelude::*,
};

use crate::{Column, ErrorKind, Schema, SchemaRow, SqliteError, Type};

trait SqlParser<'src, O> = Parser<'src, &'src str, Spanned<O>, extra::Err<Rich<'src, char>>>;

#[derive(Debug, Clone)]
pub struct Spanned<T> {
    pub inner: Box<T>,
    pub span: Range<usize>,
}

impl<T> Spanned<T> {
    fn new<'src, 'b, I: Input<'src, Span = SimpleSpan<usize>>, E: ParserExtra<'src, I>>(
        inner: T,
        ext: &mut MapExtra<'src, '_, I, E>,
    ) -> Spanned<T> {
        Spanned {
            inner: Box::new(inner),
            span: ext.span().into_range(),
        }
    }

    pub fn empty(inner: T) -> Spanned<T> {
        Spanned {
            inner: Box::new(inner),
            span: 0..0,
        }
    }

    pub fn span(inner: T, span: Range<usize>) -> Spanned<T> {
        Spanned {
            inner: Box::new(inner),
            span,
        }
    }

    pub fn with_span(self, span: Range<usize>) -> Spanned<T> {
        Spanned { span, ..self }
    }
}

impl<T: PartialEq> PartialEq for Spanned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T> std::ops::Deref for Spanned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Query {
    Select(Spanned<SelectStatement>),
    CreateTable(Spanned<CreateTableStatement>),
    CreateIndex(Spanned<CreateIndexStatement>),
    Explain(Spanned<SelectStatement>),
    Info(Spanned<SelectStatement>), // Could use Query
    Test(Spanned<SelectStatement>),
}

impl Query {
    pub fn parse(query: &str) -> crate::Result<Spanned<Query>> {
        parser().parse(query).into_result().map_err(|errs| {
            let error = &errs[0];

            SqliteError::new(
                ErrorKind::Parser(error.to_string()),
                error.span().into_range(),
            )
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub select_clause: Spanned<Vec<Spanned<Select>>>,
    pub from_clause: Spanned<From>,
    pub where_clause: Option<Spanned<WhereStatement>>,
    pub groupby_clause: Option<Spanned<Groupby>>,
    pub limit_clause: Option<Spanned<Limit>>,
}

fn ident_parser<'src>() -> impl SqlParser<'src, String> + Clone {
    text::unicode::ident()
        .map(|s: &str| s.into())
        .map_with(Spanned::new)
}

#[derive(Debug, PartialEq, Clone)]
pub enum Select {
    Wildcard,
    Column {
        name: Spanned<String>,
        table: Option<Spanned<String>>,
        alias: Option<Spanned<String>>,
    },
    Function {
        name: Spanned<String>,
        args: Spanned<Vec<Select>>,
    },
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Select::Wildcard => write!(f, "*"),
            Select::Column { name, table, .. } => {
                if let Some(table) = table {
                    write!(f, "{}.", *table.inner)?;
                }
                write!(f, "{}", *name.inner)
            }
            Select::Function { name, .. } => write!(f, "{}(...)", *name.inner),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum From {
    Table {
        table: Spanned<String>,
        alias: Option<Spanned<String>>,
    },
    Subquery {
        query: Spanned<SelectStatement>,
        alias: Option<Spanned<String>>,
    },
    Join {
        left: Spanned<From>,
        // join: Type
        right: Spanned<From>,
        on: Option<Spanned<Comparison>>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereStatement {
    Comparison(Spanned<Comparison>),
    And(Spanned<Self>, Spanned<Self>),
    Or(Spanned<Self>, Spanned<Self>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Groupby {
    columns: Vec<Column>,
}

impl Display for WhereStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WhereStatement::Comparison(comp) => write!(f, "{}", *comp.inner),
            WhereStatement::And(a, b) => write!(f, "({} and {})", *a.inner, *b.inner),
            WhereStatement::Or(a, b) => write!(f, "({} or {})", *a.inner, *b.inner),
        }
    }
}

impl WhereStatement {
    pub fn index(&self) -> Vec<(Column, Expression)> {
        match self {
            WhereStatement::Comparison(comparison) => comparison.index(),
            WhereStatement::And(left, right) => [left.index(), right.index()].concat(),
            WhereStatement::Or(..) => vec![],
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Comparison {
    pub left: Spanned<Expression>,
    pub op: Spanned<Operator>,
    pub right: Spanned<Expression>,
}

impl Display for Comparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", *self.left, *self.op, *self.right)
    }
}

impl Comparison {
    fn index(&self) -> Vec<(Column, Expression)> {
        match (&*self.left, &*self.op) {
            (Expression::Column(column), Operator::Equal) => {
                vec![(column.clone(), *self.right.inner.clone())]
            }
            _ => vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Less,
    LessEqual,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
}
impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Less => write!(f, "<"),
            Operator::LessEqual => write!(f, "<="),
            Operator::Equal => write!(f, "="),
            Operator::NotEqual => write!(f, "<>"),
            Operator::Greater => write!(f, ">"),
            Operator::GreaterEqual => write!(f, ">="),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Column(Column),
    Literal(String),
    Number(u64),
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Column(column) => write!(f, "{column}"),
            Expression::Literal(s) => write!(f, "{s:?}"),
            Expression::Number(n) => write!(f, "{n}"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Limit {
    pub limit: usize,
}

fn comparison_parser<'src>() -> impl SqlParser<'src, Comparison> + Clone {
    let ident = text::ident().padded();

    let expression = text::int(10)
        .padded()
        .map(|s: &str| Expression::Number(s.parse().unwrap()))
        .or(ident
            .repeated()
            .to_slice()
            .delimited_by(just("\'"), just("\'"))
            .padded()
            .map(|s: &str| Expression::Literal(s.to_string())))
        .or(ident
            .then(just(".").ignore_then(ident).or_not())
            .to_slice()
            .padded()
            .map(|string: &str| Expression::Column(string.trim().into())))
        .map_with(Spanned::new);

    let operator = choice((
        just("<>").to(Operator::NotEqual),
        just("<=").to(Operator::LessEqual),
        just("<").to(Operator::Less),
        just(">=").to(Operator::GreaterEqual),
        just(">").to(Operator::Greater),
        just("=").to(Operator::Equal),
    ))
    .map_with(Spanned::new);

    expression
        .then(operator.padded())
        .then(expression)
        .map(
            |((left, op), right): (
                (Spanned<Expression>, Spanned<Operator>),
                Spanned<Expression>,
            )| Comparison { left, op, right },
        )
        .map_with(Spanned::new)
}

fn where_parser<'src>() -> impl SqlParser<'src, WhereStatement> + Clone {
    comparison_parser()
        .map(WhereStatement::Comparison)
        .map_with(Spanned::new)
        .pratt((
            infix(left(2), just("and").padded(), |l, _, r, e| {
                Spanned::new(WhereStatement::And(l, r), e)
            }),
            infix(left(1), just("or").padded(), |l, _, r, e| {
                Spanned::new(WhereStatement::Or(l, r), e)
            }),
        ))
}

fn gropuby_parser<'src>() -> impl SqlParser<'src, Groupby> + Clone {
    let ident = text::ident().padded();

    ident
        .then(just(".").ignore_then(ident).or_not())
        .to_slice()
        .padded()
        .map(|column: &str| column.into())
        .separated_by(just(","))
        .at_least(1)
        .collect::<Vec<_>>()
        .map(|columns: Vec<Column>| Groupby { columns })
        .map_with(Spanned::new)
}

fn selectstmt_parser<'src>() -> impl SqlParser<'src, SelectStatement> {
    recursive(|select_stmt| {
        let wildcard = just("*").padded().map(|_| Select::Wildcard);

        let column = ident_parser()
            .then(just(".").ignore_then(ident_parser()).or_not())
            .then(just("as").padded().ignore_then(ident_parser()).or_not())
            .map(
                |((first, second), alias): (
                    (Spanned<String>, Option<Spanned<String>>),
                    Option<Spanned<String>>,
                )| {
                    let (table, name) = match second {
                        None => (None, first),
                        Some(t) => (Some(first), t),
                    };

                    Select::Column { table, name, alias }
                },
            );

        let function = ident_parser()
            .then(
                wildcard
                    .or(column.clone())
                    .separated_by(just(",").padded())
                    .at_least(1)
                    .collect::<Vec<_>>()
                    .delimited_by(just("("), just(")"))
                    .map_with(Spanned::new),
            )
            .map(
                |(name, args): (Spanned<String>, Spanned<Vec<Select>>)| Select::Function {
                    name,
                    args,
                },
            );

        let columns = wildcard
            .or(function)
            .or(column)
            .map_with(Spanned::new)
            .separated_by(just(",").padded())
            .at_least(1)
            .collect::<Vec<_>>();

        let select = just("select")
            .padded()
            .ignore_then(columns)
            .map_with(Spanned::new);

        let subquery = select_stmt
            .delimited_by(just("("), just(")"))
            .then(just("as").padded().ignore_then(ident_parser()).or_not())
            .map(
                |(query, alias): (Spanned<SelectStatement>, Option<Spanned<String>>)| {
                    From::Subquery { query, alias }
                },
            )
            .map_with(Spanned::new);

        let table = ident_parser()
            .then(just("as").padded().ignore_then(ident_parser()).or_not())
            .map(
                |(table, alias): (Spanned<String>, Option<Spanned<String>>)| From::Table {
                    table,
                    alias,
                },
            )
            .map_with(Spanned::new);

        let table_subquery = table.or(subquery);

        let join = table_subquery
            .clone()
            .then_ignore(just("join").padded())
            .then(table_subquery.clone())
            .then(
                just("on")
                    .padded()
                    .ignore_then(comparison_parser())
                    .or_not(),
            )
            .map(
                |((left, right), on): (
                    (Spanned<From>, Spanned<From>),
                    Option<Spanned<Comparison>>,
                )| { From::Join { left, right, on } },
            )
            .map_with(Spanned::new);

        let from = just("from").padded().ignore_then(join.or(table_subquery));

        let r#where = just("where").padded().ignore_then(where_parser()).or_not();

        let groupby = just("group by")
            .padded()
            .ignore_then(gropuby_parser())
            .or_not();

        let limit = just("limit")
            .padded()
            .ignore_then(text::int(10))
            .map(|n: &str| Limit {
                limit: n.parse().unwrap(),
            })
            .map_with(Spanned::new)
            .or_not();

        select
            .then(from)
            .then(r#where)
            .then(groupby)
            .then(limit)
            .map(
            |((((select_clause, from_clause), where_clause), groupby_clause), limit_clause): (
                (
                    (
                        (Spanned<Vec<Spanned<Select>>>, Spanned<From>),
                        Option<Spanned<WhereStatement>>,
                    ),
                    Option<Spanned<Groupby>>,
                ),
                Option<Spanned<Limit>>,
            )| {
                SelectStatement {
                    select_clause,
                    from_clause,
                    where_clause,
                    groupby_clause,
                    limit_clause,
                }
            },
        )
        .map_with(Spanned::new)
    })
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub schema: Schema,
    pub extras: Vec<Extra>,
}

#[derive(Debug, PartialEq, Clone)]
enum Key {
    Primary,
    AutoIncrement,
    NotNull,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Extra {
    WithoutRowid,
}

fn createtablestmt_parser<'src>() -> impl SqlParser<'src, CreateTableStatement> {
    let ident = text::ident().or(just("_")).padded();

    let string = ident
        .repeated()
        .to_slice()
        .delimited_by(just('"'), just('"'))
        .padded();

    let r#type = choice((
        just("integer").or(just("int")).to(Type::Integer),
        just("text").to(Type::Text),
    ));

    let key = choice((
        just("primary key").to(Key::Primary),
        just("autoincrement").to(Key::AutoIncrement),
        just("not null").to(Key::NotNull),
    ))
    .padded()
    .repeated()
    .collect::<Vec<_>>();

    let column = string
        .or(ident)
        .map_with(Spanned::new)
        .then(r#type.padded())
        .then(key);

    let columns = column
        .separated_by(just(","))
        .at_least(1)
        .collect::<Vec<_>>();

    let table = string.or(ident);

    let extra = choice((just("without rowid").to(Extra::WithoutRowid),))
        .padded()
        .repeated()
        .collect::<Vec<_>>();

    just("create table")
        .padded()
        .ignore_then(table)
        .then(columns.delimited_by(just("("), just(")")))
        .then(extra)
        .map(
            |((table, columns), extras): (
                (&str, Vec<((Spanned<&str>, Type), Vec<Key>)>),
                Vec<Extra>,
            )| {
                let names = vec![table.to_string()];

                let (pairs, keys): (Vec<(Spanned<&str>, Type)>, Vec<Vec<Key>>) =
                    columns.into_iter().unzip();

                let columns = pairs
                    .into_iter()
                    .map(|(c, t)| SchemaRow {
                        column: Spanned::span((*c).into(), c.span),
                        r#type: t,
                    })
                    .collect::<_>();

                let primary = keys
                    .into_iter()
                    .enumerate()
                    .filter(|(_, k)| k.contains(&Key::Primary))
                    .map(|(idx, _)| idx)
                    .collect();

                CreateTableStatement {
                    schema: Schema {
                        columns,
                        names,
                        name: Some(0),
                        primary,
                    },
                    extras,
                }
            },
        )
        .map_with(Spanned::new)
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

fn createindexstmt_parser<'src>() -> impl SqlParser<'src, CreateIndexStatement> {
    let ident = text::ident().padded();

    let columns = ident
        .separated_by(just(","))
        .at_least(1)
        .collect::<Vec<_>>();

    just("create index")
        .padded()
        .ignore_then(ident)
        .then_ignore(just("on").padded())
        .then(ident)
        .then(columns.delimited_by(just("("), just(")")))
        .map(
            |((name, table), columns): ((&str, &str), Vec<&str>)| CreateIndexStatement {
                name: name.to_string(),
                table: table.to_string(),
                columns: columns.into_iter().map(String::from).collect::<_>(),
            },
        )
        .map_with(Spanned::new)
}

fn explain_parser<'src>() -> impl SqlParser<'src, SelectStatement> {
    just("explain").padded().ignore_then(selectstmt_parser())
}

fn info_parser<'src>() -> impl SqlParser<'src, SelectStatement> {
    just("info").padded().ignore_then(selectstmt_parser())
}

fn test_parser<'src>() -> impl SqlParser<'src, SelectStatement> {
    just("test").padded().ignore_then(selectstmt_parser())
}

fn parser<'src>() -> impl SqlParser<'src, Query> {
    choice((
        selectstmt_parser().map(Query::Select),
        createtablestmt_parser().map(Query::CreateTable),
        createindexstmt_parser().map(Query::CreateIndex),
        explain_parser().map(Query::Explain),
        info_parser().map(Query::Info),
        test_parser().map(Query::Test),
    ))
    .then_ignore(just(";").or_not())
    .then_ignore(end())
    .map_with(Spanned::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariadne::Source;

    fn check(query: &str, expected: Query) {
        match Query::parse(query) {
            Ok(parsed) => {
                if *parsed != expected {
                    panic!(
                        "{}\n=== Parsed ===\n{:#?}\n=== Expected ===\n{:#?}",
                        query, *parsed, expected
                    );
                }
            }
            Err(err) => {
                err.write(("query", Source::from(query)), std::io::stdout());
            }
        }
    }

    #[test]
    fn test_simple() {
        check(
            "select id from users;",
            Query::Select(Spanned::empty(SelectStatement {
                select_clause: Spanned::empty(vec![Spanned::empty(Select::Column {
                    name: Spanned::empty("id".into()),
                    table: None,
                    alias: None,
                })]),
                from_clause: Spanned::empty(From::Table {
                    table: Spanned::empty("users".into()),
                    alias: None,
                }),
                where_clause: None,
                groupby_clause: None,
                limit_clause: None,
            })),
        );
    }

    #[test]
    fn test_complex() {
        fn comparison(column: Column, op: Operator, right: u64) -> Spanned<WhereStatement> {
            Spanned::empty(WhereStatement::Comparison(Spanned::empty(Comparison {
                left: Spanned::empty(Expression::Column(column)),
                op: Spanned::empty(op),
                right: Spanned::empty(Expression::Number(right)),
            })))
        }

        check(
            "select u.id from users as u where u.id > 1 and u.id < 10 or u.id <> 3;",
            Query::Select(Spanned::empty(SelectStatement {
                select_clause: Spanned::empty(vec![Spanned::empty(Select::Column {
                    name: Spanned::empty("id".into()),
                    table: Some(Spanned::empty("u".into())),
                    alias: None,
                })]),
                from_clause: Spanned::empty(From::Table {
                    table: Spanned::empty("users".into()),
                    alias: Some(Spanned::empty("u".into())),
                }),
                where_clause: Some(Spanned::empty(WhereStatement::Or(
                    Spanned::empty(WhereStatement::And(
                        comparison("u.id".into(), Operator::Greater, 1),
                        comparison("u.id".into(), Operator::Less, 10),
                    )),
                    comparison("u.id".into(), Operator::NotEqual, 3),
                ))),
                groupby_clause: None,
                limit_clause: None,
            })),
        );
    }

    #[test]
    fn test_create_table() {
        check(
            "create table A(id int primary key, x int, y int);",
            Query::CreateTable(Spanned::empty(CreateTableStatement {
                schema: Schema {
                    names: vec!["A".into()],
                    name: Some(0),
                    columns: vec![
                        SchemaRow {
                            column: Spanned::empty("id".into()),
                            r#type: Type::Integer,
                        },
                        SchemaRow {
                            column: Spanned::empty("x".into()),
                            r#type: Type::Integer,
                        },
                        SchemaRow {
                            column: Spanned::empty("y".into()),
                            r#type: Type::Integer,
                        },
                    ],
                    primary: vec![0],
                },
                extras: vec![],
            })),
        );
    }

    #[test]
    fn test_create_index() {
        check(
            "create index idx on A(x, y);",
            Query::CreateIndex(Spanned::empty(CreateIndexStatement {
                name: "idx".into(),
                table: "A".into(),
                columns: vec!["x".into(), "y".into()],
            })),
        );
    }

    #[test]
    fn test_all() {
        let queries = [
            "select id from users;",
            "select id, name from users;",
            "select * from users;",
            "select id as ID from users;",
            "select id from users as u;",
            "select users.id from users;",
            "select u.id from users as u;",
            "select product from items where price = 100;",
            "select * from employees where department = 'Sales';",
            "select name, email from customers where customer_id > 123;",
            "select id from products where category = 'Electronics';",
            "select id from (select id from stuff);",
            "select a.id, b.id from apples as a join oranges as o;",
            "select a.id, b.id from apples as a join oranges as o on a.id = o.id;",
            "select * from (select * from apples) join (select * from oranges) on a = b;",
            "select * from x where x.id >= 1 and x.id <= 10 or id <> id;",
        ];

        for query in queries {
            if let Err(err) = Query::parse(query) {
                err.write(("query", Source::from(query)), std::io::stdout());
            }
        }
    }
}
