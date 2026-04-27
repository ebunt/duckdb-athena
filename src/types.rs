use libduckdb_sys::{duckdb_data_chunk, duckdb_data_chunk_get_vector, idx_t};
use quack_rs::{types::TypeId, vector::VectorWriter};

use crate::error::{Error, Result};

#[derive(Debug, PartialEq)]
enum TypedValue<'a> {
    Boolean(bool),
    BigInt(i64),
    Integer(i32),
    TinyInt(i8),
    SmallInt(i16),
    Float(f32),
    Double(f64),
    Varchar(&'a str),
}

// Maps Athena data types to DuckDB types.
// Only returns non-Varchar types when populate_column can write them correctly.
// Supported types are listed here: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
pub fn map_type(col_type: String) -> Result<TypeId> {
    let type_id = match col_type.as_str() {
        "boolean" => TypeId::Boolean,
        "tinyint" => TypeId::TinyInt,
        "smallint" => TypeId::SmallInt,
        "int" | "integer" => TypeId::Integer,
        "bigint" => TypeId::BigInt,
        "double" => TypeId::Double,
        "float" => TypeId::Float,
        // Decimal, date, and timestamp are returned as strings by Athena.
        // Register as Varchar to avoid writing string data into fixed-width vectors.
        "decimal" | "date" | "timestamp" => TypeId::Varchar,
        "string" | "varchar" | "char" => TypeId::Varchar,
        _ => {
            return Err(Error::DuckDB(format!("Unsupported data type: {col_type}")));
        }
    };

    Ok(type_id)
}

fn parse_value(value: Option<&str>, col_type: TypeId) -> Option<TypedValue<'_>> {
    let value = value?;
    match col_type {
        TypeId::Boolean => {
            if value.eq_ignore_ascii_case("true") {
                Some(TypedValue::Boolean(true))
            } else if value.eq_ignore_ascii_case("false") {
                Some(TypedValue::Boolean(false))
            } else {
                None
            }
        }
        TypeId::BigInt => value.parse::<i64>().ok().map(TypedValue::BigInt),
        TypeId::Integer => value.parse::<i32>().ok().map(TypedValue::Integer),
        TypeId::TinyInt => value.parse::<i8>().ok().map(TypedValue::TinyInt),
        TypeId::SmallInt => value.parse::<i16>().ok().map(TypedValue::SmallInt),
        TypeId::Float => value.parse::<f32>().ok().map(TypedValue::Float),
        TypeId::Double => value.parse::<f64>().ok().map(TypedValue::Double),
        _ => Some(TypedValue::Varchar(value)),
    }
}

pub unsafe fn populate_column(
    value: Option<&str>,
    col_type: TypeId,
    output: duckdb_data_chunk,
    row_idx: usize,
    col_idx: usize,
) {
    unsafe {
        let vector = duckdb_data_chunk_get_vector(output, col_idx as idx_t);
        let mut writer = VectorWriter::new(vector);

        match parse_value(value, col_type) {
            Some(TypedValue::Boolean(v)) => writer.write_bool(row_idx, v),
            Some(TypedValue::BigInt(v)) => writer.write_i64(row_idx, v),
            Some(TypedValue::Integer(v)) => writer.write_i32(row_idx, v),
            Some(TypedValue::TinyInt(v)) => writer.write_i8(row_idx, v),
            Some(TypedValue::SmallInt(v)) => writer.write_i16(row_idx, v),
            Some(TypedValue::Float(v)) => writer.write_f32(row_idx, v),
            Some(TypedValue::Double(v)) => writer.write_f64(row_idx, v),
            Some(TypedValue::Varchar(v)) => writer.write_varchar(row_idx, v),
            None => writer.set_null(row_idx),
        }
    }
}

#[cfg(test)]
mod tests {
    use quack_rs::types::TypeId;

    use super::{parse_value, TypedValue};

    #[test]
    fn parse_value_preserves_missing_as_null() {
        assert_eq!(parse_value(None, TypeId::Varchar), None);
        assert_eq!(parse_value(None, TypeId::Integer), None);
    }

    #[test]
    fn parse_value_rejects_invalid_typed_values_as_null() {
        assert_eq!(parse_value(Some("not-an-int"), TypeId::Integer), None);
        assert_eq!(parse_value(Some("yes"), TypeId::Boolean), None);
    }

    #[test]
    fn parse_value_parses_supported_typed_values() {
        assert_eq!(
            parse_value(Some("true"), TypeId::Boolean),
            Some(TypedValue::Boolean(true))
        );
        assert_eq!(
            parse_value(Some("42"), TypeId::Integer),
            Some(TypedValue::Integer(42))
        );
        assert_eq!(
            parse_value(Some("3.5"), TypeId::Double),
            Some(TypedValue::Double(3.5))
        );
    }

    #[test]
    fn parse_value_preserves_empty_strings_for_varchar() {
        assert_eq!(
            parse_value(Some(""), TypeId::Varchar),
            Some(TypedValue::Varchar(""))
        );
    }
}
