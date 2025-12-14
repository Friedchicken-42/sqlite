use crate::{
    Result, Rows, Schema, Table,
    parser::{Groupby, Select, Spanned},
    tables::{
        btreepage::{BTreePage, BTreePageBuilder},
        view::create_schema,
    },
};

pub struct GroupBy<'db> {
    table: BTreePage<'db>,
}

impl<'db> GroupBy<'db> {
    pub fn new(
        select: Vec<Spanned<Select>>,
        groupby: Option<Groupby>,
        inner: Table,
    ) -> Result<Self> {
        let (schema, columns) = create_schema(select, &inner)?;

        let mut table = BTreePageBuilder::new(schema).build()?;

        Ok(Self { table })
    }

    pub fn schema(&self) -> &Schema {
        self.table.schema()
    }

    pub fn rows(&mut self) -> Rows<'_, 'db> {
        self.table.rows()
    }

    pub fn write_indented(&self, f: &mut std::fmt::Formatter, _prefix: &str) -> std::fmt::Result {
        writeln!(f, "todo")
    }
}
