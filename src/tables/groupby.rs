use crate::{
    Result, Rows, Schema, Table, Tabular,
    parser::{Groupby, Select, Spanned},
    tables::{
        btreepage::{BTreePage, BTreePageBuilder},
        view::create_schema,
    },
};

pub struct GroupBy<'db> {
    table: BTreePage<'db>,
}

impl<'table> Tabular<'table> for GroupBy<'table> {
    fn rows(&mut self) -> Rows<'_, 'table> {
        todo!()
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        todo!()
    }

    fn schema(&self) -> &Schema {
        todo!()
    }
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
}
