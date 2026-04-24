use libduckdb_sys::{
    duckdb_data_chunk, duckdb_data_chunk_get_vector, duckdb_vector_assign_string_element_len, idx_t,
};
use quack_rs::{types::TypeId, vector::VectorWriter};

use crate::error::{Error, Result};

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

pub unsafe fn populate_column(
    value: &str,
    col_type: TypeId,
    output: duckdb_data_chunk,
    row_idx: usize,
    col_idx: usize,
) {
    unsafe {
        let vector = duckdb_data_chunk_get_vector(output, col_idx as idx_t);
        match col_type {
            TypeId::Boolean => {
                let v = value.eq_ignore_ascii_case("true");
                let mut writer = VectorWriter::new(vector);
                writer.write_bool(row_idx, v);
            }
            TypeId::BigInt => {
                if let Ok(v) = value.parse::<i64>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_i64(row_idx, v);
                }
            }
            TypeId::Integer => {
                if let Ok(v) = value.parse::<i32>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_i32(row_idx, v);
                }
            }
            TypeId::TinyInt => {
                if let Ok(v) = value.parse::<i8>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_i8(row_idx, v);
                }
            }
            TypeId::SmallInt => {
                if let Ok(v) = value.parse::<i16>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_i16(row_idx, v);
                }
            }
            TypeId::Float => {
                if let Ok(v) = value.parse::<f32>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_f32(row_idx, v);
                }
            }
            TypeId::Double => {
                if let Ok(v) = value.parse::<f64>() {
                    let mut writer = VectorWriter::new(vector);
                    writer.write_f64(row_idx, v);
                }
            }
            _ => {
                // Varchar and any other type: write as string.
                // SAFETY: only reached for types registered as Varchar with DuckDB.
                let bytes = value.as_bytes();
                duckdb_vector_assign_string_element_len(
                    vector,
                    row_idx as idx_t,
                    bytes.as_ptr().cast(),
                    bytes.len() as idx_t,
                );
            }
        }
    }
}
