use crate::{Tabular, tables::btreepage::BTreePage};

pub struct IndexScan<'table>(BTreePage<'table>);

impl<'table> Tabular<'table> for IndexScan<'table> {
    fn rows(&mut self) -> crate::Rows<'_, 'table> {
        self.0.rows()
    }

    fn write_indented(&self, f: &mut std::fmt::Formatter, prefix: &str) -> std::fmt::Result {
        write!(f, "{prefix}└─")?;
        self.0.write_indented(f, "")
    }

    fn schema(&self) -> &crate::Schema {
        self.0.schema()
    }
}
