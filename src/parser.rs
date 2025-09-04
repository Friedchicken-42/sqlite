use chumsky::{
    container::Seq,
    pratt::{infix, left},
    prelude::*,
};

use crate::{Column, Schema, SchemaRow, SqliteError, Type};

#[derive(Debug, PartialEq)]
pub enum Query {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    Explain(SelectStatement),
}

impl Query {
    pub fn parse(query: &str) -> crate::Result<Self> {
        parser().parse(query).into_result().map_err(|errs| {
            let err = &errs[0];

            SqliteError::Parser {
                query: query.to_string(),
                span: err.span().into_range(),
                message: err.to_string(),
            }
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub select_clause: Vec<Select>,
    pub from_clause: From,
    pub where_clause: Option<WhereStatement>,
    pub limit_clause: Option<Limit>,
}

#[derive(Debug, PartialEq)]
pub enum Select {
    Wildcard,
    Column {
        name: String,
        table: Option<String>,
        alias: Option<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum From {
    Table {
        table: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<SelectStatement>,
        alias: Option<String>,
    },
    Join {
        left: Box<From>,
        // join: Type
        right: Box<From>,
        on: Option<Comparison>,
    },
}

#[derive(Debug, PartialEq)]
pub enum WhereStatement {
    Comparison(Comparison),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
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

#[derive(Debug, PartialEq)]
pub struct Comparison {
    pub left: Expression,
    pub op: Operator,
    pub right: Expression,
}

impl Comparison {
    fn index(&self) -> Vec<(Column, Expression)> {
        match (&self.left, &self.op) {
            (Expression::Column(column), Operator::Equal) => {
                vec![(column.clone(), self.right.clone())]
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

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Column(Column),
    Literal(String),
    Number(u64),
}

#[derive(Debug, PartialEq)]
pub struct Limit {
    pub limit: usize,
}

fn comparison_parser<'src>()
-> impl Parser<'src, &'src str, Comparison, extra::Err<Rich<'src, char>>> + Clone {
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
            .map(|string: &str| Expression::Column(string.trim().into())));

    let operator = choice((
        just("<>").to(Operator::NotEqual),
        just("<=").to(Operator::LessEqual),
        just("<").to(Operator::Less),
        just(">=").to(Operator::GreaterEqual),
        just(">").to(Operator::Greater),
        just("=").to(Operator::Equal),
    ));

    expression.then(operator.padded()).then(expression).map(
        |((left, op), right): ((Expression, Operator), Expression)| Comparison { left, op, right },
    )
}

fn where_parser<'src>()
-> impl Parser<'src, &'src str, WhereStatement, extra::Err<Rich<'src, char>>> + Clone {
    comparison_parser().map(WhereStatement::Comparison).pratt((
        infix(left(2), just("and").padded(), |l, _, r, _| {
            WhereStatement::And(Box::new(l), Box::new(r))
        }),
        infix(left(1), just("or").padded(), |l, _, r, _| {
            WhereStatement::Or(Box::new(l), Box::new(r))
        }),
    ))
}

fn selectstmt_parser<'src>()
-> impl Parser<'src, &'src str, SelectStatement, extra::Err<Rich<'src, char>>> {
    recursive(|select_stmt| {
        let ident = text::ident().padded();

        let wildcard = just("*").padded().map(|_| Select::Wildcard);

        let column = ident
            .then(just(".").ignore_then(ident).or_not())
            .then(just("as").padded().ignore_then(ident).or_not())
            .map(
                |((first, second), alias): ((&str, Option<&str>), Option<&str>)| {
                    let (table, name) = match second {
                        None => (None, first.to_string()),
                        Some(t) => (Some(first.to_string()), t.to_string()),
                    };

                    let alias = alias.map(|s| s.to_string());

                    Select::Column { table, name, alias }
                },
            );

        let columns = wildcard
            .or(column)
            .separated_by(just(",").padded())
            .at_least(1)
            .collect::<Vec<_>>();

        let subquery = select_stmt
            .delimited_by(just("("), just(")"))
            .then(just("as").ignore_then(ident).or_not())
            .map(
                |(query, alias): (SelectStatement, Option<&str>)| From::Subquery {
                    query: Box::new(query),
                    alias: alias.map(|s| s.to_string()),
                },
            );

        let table = ident
            .then(just("as").padded().ignore_then(ident).or_not())
            .map(|(table, alias): (&str, Option<&str>)| From::Table {
                table: table.to_string(),
                alias: alias.map(|s| s.to_string()),
            });

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
                |((q1, q2), on): ((From, From), Option<Comparison>)| From::Join {
                    left: Box::new(q1),
                    right: Box::new(q2),
                    on,
                },
            );

        let from = just("from").padded().ignore_then(join.or(table_subquery));

        let r#where = just("where").padded().ignore_then(where_parser()).or_not();

        let limit = just("limit")
            .padded()
            .ignore_then(text::int(10))
            .map(|n: &str| Limit {
                limit: n.parse().unwrap(),
            })
            .or_not();

        just("select")
            .padded()
            .ignore_then(columns)
            .then(from)
            .then(r#where)
            .then(limit)
            .map(
                |(((select_clause, from_clause), where_clause), limit_clause): (
                    ((Vec<Select>, From), Option<WhereStatement>),
                    Option<Limit>,
                )| {
                    SelectStatement {
                        select_clause,
                        from_clause,
                        where_clause,
                        limit_clause,
                    }
                },
            )
    })
}

#[derive(Debug, PartialEq)]
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

fn createtablestmt_parser<'src>()
-> impl Parser<'src, &'src str, CreateTableStatement, extra::Err<Rich<'src, char>>> {
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

    let column = string.or(ident).then(r#type.padded()).then(key);

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
            |((table, columns), extras): ((&str, Vec<((&str, Type), Vec<Key>)>), Vec<Extra>)| {
                let names = vec![table.to_string()];

                let (pairs, keys): (Vec<(&str, Type)>, Vec<Vec<Key>>) = columns.into_iter().unzip();

                let columns = pairs
                    .into_iter()
                    .map(|(c, t)| SchemaRow {
                        column: c.into(),
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
                        primary,
                    },
                    extras,
                }
            },
        )
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

fn createindexstmt_parser<'src>()
-> impl Parser<'src, &'src str, CreateIndexStatement, extra::Err<Rich<'src, char>>> {
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
}

fn explain_parser<'src>()
-> impl Parser<'src, &'src str, SelectStatement, extra::Err<Rich<'src, char>>> {
    just("explain").padded().ignore_then(selectstmt_parser())
}

fn parser<'src>() -> impl Parser<'src, &'src str, Query, extra::Err<Rich<'src, char>>> {
    selectstmt_parser()
        .map(Query::Select)
        .or(createtablestmt_parser().map(Query::CreateTable))
        .or(createindexstmt_parser().map(Query::CreateIndex))
        .or(explain_parser().map(Query::Explain))
        .then_ignore(just(";").or_not())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ariadne::{Color, Label, Report, ReportKind, Source};
    use std::ops::Range;

    fn parse(query: &str) -> Result<Query, Report<'_, (&str, Range<usize>)>> {
        match parser().parse(query).into_result() {
            Err(errs) => {
                let err = &errs[0];

                Err(
                    Report::build(ReportKind::Error, ("query", err.span().into_range()))
                        .with_config(
                            ariadne::Config::new().with_index_type(ariadne::IndexType::Char),
                        )
                        .with_message(err.to_string())
                        .with_label(
                            Label::new(("query", err.span().into_range()))
                                .with_message(err.to_string())
                                .with_color(Color::Red),
                        )
                        .finish(),
                )
            }
            Ok(parsed) => Ok(parsed),
        }
    }

    fn display_report(query: &str, report: Report<'_, (&str, Range<usize>)>) {
        let mut buffer = Vec::new();

        report
            .write(("query", Source::from(query)), &mut buffer)
            .unwrap();

        panic!("{}", std::str::from_utf8(&buffer).unwrap());
    }

    fn check(query: &str, expected: Query) {
        match parse(query) {
            Ok(parsed) => {
                if parsed != expected {
                    panic!("{query}\n=== Parsed ===\n{parsed:#?}\n=== Expected ===\n{expected:#?}");
                }
            }
            Err(report) => display_report(query, report),
        }
    }

    #[test]
    fn test_simple() {
        check(
            "select id from users;",
            Query::Select(SelectStatement {
                select_clause: vec![Select::Column {
                    name: "id".into(),
                    table: None,
                    alias: None,
                }],
                from_clause: From::Table {
                    table: "users".into(),
                    alias: None,
                },
                where_clause: None,
                limit_clause: None,
            }),
        );
    }

    #[test]
    fn test_complex() {
        fn comparison(column: Column, op: Operator, right: u64) -> WhereStatement {
            WhereStatement::Comparison(Comparison {
                left: Expression::Column(column),
                op,
                right: Expression::Number(right),
            })
        }

        check(
            "select u.id from users as u where u.id > 1 and u.id < 10 or u.id <> 3;",
            Query::Select(SelectStatement {
                select_clause: vec![Select::Column {
                    name: "id".into(),
                    table: Some("u".into()),
                    alias: None,
                }],
                from_clause: From::Table {
                    table: "users".into(),
                    alias: Some("u".into()),
                },
                where_clause: Some(WhereStatement::Or(
                    Box::new(WhereStatement::And(
                        Box::new(comparison("u.id".into(), Operator::Greater, 1)),
                        Box::new(comparison("u.id".into(), Operator::Less, 10)),
                    )),
                    Box::new(comparison("u.id".into(), Operator::NotEqual, 3)),
                )),
                limit_clause: None,
            }),
        );
    }

    #[test]
    fn test_create_table() {
        check(
            "create table A(id int primary key, x int, y int);",
            Query::CreateTable(CreateTableStatement {
                schema: Schema {
                    names: vec!["A".into()],
                    columns: vec![
                        SchemaRow {
                            column: "id".into(),
                            r#type: Type::Integer,
                        },
                        SchemaRow {
                            column: "x".into(),
                            r#type: Type::Integer,
                        },
                        SchemaRow {
                            column: "y".into(),
                            r#type: Type::Integer,
                        },
                    ],
                    primary: vec![0],
                },
                extras: vec![],
            }),
        );
    }

    #[test]
    fn test_create_index() {
        check(
            "create index idx on A(x, y);",
            Query::CreateIndex(CreateIndexStatement {
                name: "idx".into(),
                table: "A".into(),
                columns: vec!["x".into(), "y".into()],
            }),
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
            if let Err(report) = parse(query) {
                display_report(query, report);
            }
        }
    }
}
