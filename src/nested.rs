//! Nested `lineitem` shape from apache/datafusion-comet#7044.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, RecordBatch, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::json::writer::{EncoderOptions, make_encoder};
use arrow_schema::extension::Json;

// Comet keeps its join keys and its partition column `l_shipdate` as plain columns.
// `l_linenumber` also stays because CDC upserts on the primary key.
const LINEITEM_TOP_LEVEL: &[&str] = &[
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_shipdate",
];
const LINEITEM_DATA: &str = "lineitem_data";

pub fn is_json(field: &Field) -> bool {
    field.try_extension_type::<Json>().is_ok()
}

/// Moves the other columns into one JSON column. Each value is an array with
/// one object, so the flattened table equals the flat table.
pub struct NestedShape {
    schema: SchemaRef,
    top_level: Vec<usize>,
    nested: Vec<usize>,
    nested_field: FieldRef,
}

impl NestedShape {
    pub fn new(table: &str, flat: &Schema) -> Option<Self> {
        if table != "lineitem" {
            return None;
        }
        let (top_level, nested): (Vec<usize>, Vec<usize>) = (0..flat.fields().len())
            .partition(|&i| LINEITEM_TOP_LEVEL.contains(&flat.field(i).name().as_str()));
        let nested_fields: Fields = nested.iter().map(|&i| flat.fields()[i].clone()).collect();
        let nested_field = Arc::new(Field::new(
            LINEITEM_DATA,
            DataType::Struct(nested_fields),
            false,
        ));

        let mut fields: Vec<FieldRef> = top_level
            .iter()
            .map(|&i| flat.fields()[i].clone())
            .collect();
        fields.push(Arc::new(
            Field::new(LINEITEM_DATA, DataType::Utf8, false).with_extension_type(Json::default()),
        ));

        Some(Self {
            schema: Arc::new(Schema::new(fields)),
            top_level,
            nested,
            nested_field,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let DataType::Struct(fields) = self.nested_field.data_type() else {
            unreachable!("nested field is a struct");
        };
        let children: Vec<ArrayRef> = self
            .nested
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        let rows = StructArray::try_new(fields.clone(), children, None)?;

        let options = EncoderOptions::default();
        let mut encoder = make_encoder(&self.nested_field, &rows, &options)?;
        let mut json = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 256);
        let mut buf = Vec::with_capacity(256);
        for row in 0..batch.num_rows() {
            buf.clear();
            buf.push(b'[');
            encoder.encode(row, &mut buf);
            buf.push(b']');
            json.append_value(
                std::str::from_utf8(&buf).context("JSON encoder wrote invalid UTF-8")?,
            );
        }

        let mut columns: Vec<ArrayRef> = self
            .top_level
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        columns.push(Arc::new(json.finish()));
        Ok(RecordBatch::try_new(self.schema(), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tpch::TpchTable;
    use arrow::array::AsArray;
    use arrow::json::ReaderBuilder;

    #[test]
    fn flattening_returns_the_flat_rows() {
        let mut flat = TpchTable::new("lineitem", 0.001)
            .unwrap()
            .with_batch_size(256);
        let shape = NestedShape::new("lineitem", &flat.schema()).unwrap();
        let batch = flat.next().unwrap();
        let nested = shape.apply(&batch).unwrap();
        assert!(is_json(
            nested.schema().field_with_name(LINEITEM_DATA).unwrap()
        ));

        let item = Field::new_list_field(shape.nested_field.data_type().clone(), false);
        let wrapper = Schema::new(vec![Field::new("d", DataType::List(Arc::new(item)), false)]);
        let lines: String = nested
            .column_by_name(LINEITEM_DATA)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|value| format!("{{\"d\":{}}}\n", value.unwrap()))
            .collect();
        let decoded = ReaderBuilder::new(Arc::new(wrapper))
            .with_batch_size(batch.num_rows())
            .build(lines.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let list = decoded.column(0).as_list::<i32>();
        assert!(list.offsets().lengths().all(|len| len == 1));

        let rows = list.values().as_struct();
        assert_eq!(
            nested.num_columns() - 1 + rows.num_columns(),
            batch.num_columns()
        );
        for field in batch.schema().fields() {
            let name = field.name();
            let actual = rows
                .column_by_name(name)
                .or_else(|| nested.column_by_name(name))
                .unwrap();
            assert_eq!(
                actual.to_data(),
                batch.column_by_name(name).unwrap().to_data(),
                "{name}"
            );
        }
    }
}
