//! The C API v1 adapter: `athena_scan` registered through quack-rs, built on
//! the stable `C_STRUCT` ABI so one binary loads across DuckDB 1.4 to 2.0.
//! Everything that is not about DuckDB's handles lives in `scan.rs`.

use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size,
    duckdb_function_info, duckdb_function_set_error, duckdb_init_info, idx_t,
};
use quack_rs::{
    table::{BindInfo, FfiBindData, FfiInitData, InitInfo, TableFunctionBuilder},
    types::{LogicalType, TypeId},
    vector::VectorWriter,
};
use std::{ffi::CString, sync::Mutex};

use crate::results::ResultRow;
use crate::scan::{bind_scan, result_row_cell, submit_scan, RowSource, ScanArgs, ScanBindData};
use crate::types::{parse_cell, Cell, ColType};

struct ScanInitData {
    /// `athena_scan` registers no `local_init` and never raises `max_threads`
    /// above DuckDB's default of 1, so `read_athena` runs single-threaded; the
    /// `Mutex` exists only to satisfy `FfiInitData::set`'s `Send + Sync` bound,
    /// never to coordinate real concurrent access.
    source: Mutex<RowSource>,
    /// Resolved type of each projected column, in the order DuckDB projected them
    /// (which is also Athena's SELECT-list order), so writes are positional.
    col_types: Vec<ColType>,
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena(info: duckdb_function_info, output: duckdb_data_chunk) {
    unsafe {
        let Some(state) = FfiInitData::<ScanInitData>::get_mut(info) else {
            duckdb_data_chunk_set_size(output, 0);
            return;
        };
        let capacity = libduckdb_sys::duckdb_vector_size() as usize;
        let source = state.source.get_mut().expect("single-threaded scan");
        let written = source
            .next_rows(capacity)
            .and_then(|rows| rows_to_duckdb_data_chunk(&rows, &state.col_types, output));
        if let Err(e) = written {
            let msg = CString::new(e).unwrap_or_default();
            duckdb_function_set_error(info, msg.as_ptr());
            duckdb_data_chunk_set_size(output, 0);
        }
    }
}

pub fn rows_to_duckdb_data_chunk(
    rows: &[ResultRow],
    col_types: &[ColType],
    chunk: duckdb_data_chunk,
) -> Result<(), String> {
    let result_size = rows.len();
    let chunk_col_count =
        unsafe { libduckdb_sys::duckdb_data_chunk_get_column_count(chunk) } as usize;

    // DuckDB data chunk vectors hold at most STANDARD_VECTOR_SIZE rows. The
    // row source hands over at most the capacity it was asked for, but fail
    // loud rather than write out of bounds if that ever changes.
    let capacity = unsafe { libduckdb_sys::duckdb_vector_size() } as usize;
    if result_size > capacity {
        return Err(format!(
            "Athena result page has {result_size} rows, exceeding DuckDB's vector capacity of {capacity}"
        ));
    }

    // Write every chunk column for every row. Bounded by both the resolved
    // column types and the DuckDB chunk column counts so we never write past the
    // chunk boundary if they diverge (e.g. unsupported column types skipped).
    let col_count = col_types.len().min(chunk_col_count);
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, &col_type) in col_types.iter().take(col_count).enumerate() {
            unsafe {
                populate_column(
                    result_row_cell(row, col_idx),
                    col_type,
                    chunk,
                    row_idx,
                    col_idx,
                )
            };
        }
    }

    unsafe { duckdb_data_chunk_set_size(chunk, result_size as idx_t) };

    Ok(())
}

/// Writes one Athena cell into the DuckDB output vector.
///
/// `value` is `None` when Athena returned SQL NULL (the datum has no
/// `varCharValue`). A value that fails to parse is also written as NULL
/// rather than silently left as a valid zero; see [`parse_cell`].
///
/// # Safety
/// `output` must be a writable chunk whose column `col_idx` has the DuckDB
/// type registered at bind for `col_type`, and `row_idx` must be within its
/// capacity.
unsafe fn populate_column(
    value: Option<&str>,
    col_type: ColType,
    output: duckdb_data_chunk,
    row_idx: usize,
    col_idx: usize,
) {
    unsafe {
        let vector = duckdb_data_chunk_get_vector(output, col_idx as idx_t);
        let mut writer = VectorWriter::new(vector);

        let Some(cell) = parse_cell(value, col_type) else {
            writer.set_null(row_idx);
            return;
        };
        match cell {
            Cell::Bool(v) => writer.write_bool(row_idx, v),
            Cell::I8(v) => writer.write_i8(row_idx, v),
            Cell::I16(v) => writer.write_i16(row_idx, v),
            Cell::I32(v) => writer.write_i32(row_idx, v),
            Cell::I64(v) => writer.write_i64(row_idx, v),
            Cell::F32(v) => writer.write_f32(row_idx, v),
            Cell::F64(v) => writer.write_f64(row_idx, v),
            Cell::Date(days) => writer.write_date(row_idx, days),
            Cell::Timestamp(micros) => writer.write_timestamp(row_idx, micros),
            // The physical width DuckDB allocates for a DECIMAL(width, _).
            // Must match the width registered at bind, or the write lands at
            // the wrong stride.
            Cell::Decimal { unscaled, width } => match width {
                1..=4 => writer.write_i16(row_idx, unscaled as i16),
                5..=9 => writer.write_i32(row_idx, unscaled as i32),
                10..=18 => writer.write_i64(row_idx, unscaled as i64),
                _ => writer.write_i128(row_idx, unscaled),
            },
            Cell::Text(v) => writer.write_varchar(row_idx, v),
        }
    }
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena_bind(bind_info: duckdb_bind_info) {
    unsafe {
        let bi = BindInfo::new(bind_info);
        if bi.parameter_count() < 1 {
            bi.set_error("athena_scan requires at least 1 parameter: tablename");
            return;
        }

        let args = match read_args(&bi) {
            Ok(args) => args,
            Err(e) => {
                bi.set_error(&e);
                return;
            }
        };
        let bind_data = match bind_scan(args) {
            Ok(bind_data) => bind_data,
            Err(e) => {
                bi.set_error(&e);
                return;
            }
        };

        // Register each output column with the type resolved from Glue: native
        // DECIMAL(width, scale) when applicable, else the plain TypeId.
        for (name, col_type) in bind_data.columns.iter().zip(&bind_data.col_types) {
            match *col_type {
                ColType::Decimal { width, scale } => {
                    bi.add_result_column_with_type(name, &LogicalType::decimal(width, scale));
                }
                // JSON text is a VARCHAR column to DuckDB; only the Athena-side
                // SELECT differs.
                ColType::Json => {
                    bi.add_result_column(name, TypeId::Varchar);
                }
                ColType::Simple(type_id) => {
                    bi.add_result_column(name, type_id);
                }
            }
        }
        if let Some(rows) = bind_data.cardinality {
            bi.set_cardinality(rows, false);
        }

        FfiBindData::<ScanBindData>::set(bind_info, bind_data);
    }
}

/// Reads the positional and named parameters off DuckDB's bind handle. Only
/// type errors are reported here; what the values mean is `bind_scan`'s job.
///
/// # Safety
/// `bi` must wrap the live bind handle of an `athena_scan` call with at least
/// one positional parameter.
unsafe fn read_args(bi: &BindInfo) -> Result<ScanArgs, String> {
    unsafe {
        // None when omitted/null, Some(value) otherwise.
        let named_str = |name: &str| -> Result<Option<String>, String> {
            let val = bi.get_named_parameter_value(name);
            if val.is_null() {
                Ok(None)
            } else {
                val.as_str().map(Some).map_err(|e| e.to_string())
            }
        };
        let named_i32 = |name: &str| -> Result<Option<i32>, String> {
            let val = bi.get_named_parameter_value(name);
            if val.is_null() {
                Ok(None)
            } else {
                val.as_i32()
                    .map(Some)
                    .ok_or_else(|| format!("{name} must fit in a 32-bit integer"))
            }
        };

        Ok(ScanArgs {
            tablename: bi
                .get_parameter_value(0)
                .as_str()
                .map_err(|e| e.to_string())?,
            output_location: named_str("output_location")?,
            workgroup: named_str("workgroup")?,
            maxrows: named_i32("maxrows")?,
            timeout_seconds: named_i32("timeout_seconds")?,
            result_reuse_minutes: named_i32("result_reuse_minutes")?,
            database: named_str("database")?,
            region: named_str("region")?,
            profile: named_str("profile")?,
            predicate: named_str("predicate")?,
        })
    }
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena_init(info: duckdb_init_info) {
    unsafe {
        let bind_data = match FfiBindData::<ScanBindData>::get_from_init(info) {
            Some(d) => d,
            None => return,
        };

        // Projection pushdown: DuckDB tells us which output columns it actually
        // needs, in order.
        let init = InitInfo::new(info);
        let projected: Vec<usize> = (0..init.projected_column_count())
            .map(|i| {
                init.projected_column_index(i)
                    .expect("index within projected_column_count() is always valid")
            })
            .collect();
        // Resolved types of exactly the projected columns, in projection order,
        // so the scan writes each Athena result column into its matching chunk
        // vector with the physical layout registered at bind.
        let projected_types: Vec<ColType> = projected
            .iter()
            .filter_map(|&i| bind_data.col_types.get(i).copied())
            .collect();

        // The v1 API has no progress callback and no filter pushdown, so the
        // whole wait happens here, with a heartbeat on stderr.
        let source = submit_scan(bind_data, &projected, None).and_then(|scan| scan.wait(true));
        match source {
            Ok(source) => FfiInitData::<ScanInitData>::set(
                info,
                ScanInitData {
                    source: Mutex::new(source),
                    col_types: projected_types,
                },
            ),
            Err(e) => {
                let msg = CString::new(e).unwrap_or_default();
                libduckdb_sys::duckdb_init_set_error(info, msg.as_ptr());
            }
        }
    }
}

pub fn build_table_function_def() -> TableFunctionBuilder {
    TableFunctionBuilder::new("athena_scan")
        .param(TypeId::Varchar)
        .named_param("output_location", TypeId::Varchar)
        .named_param("workgroup", TypeId::Varchar)
        .named_param("maxrows", TypeId::Integer)
        .named_param("database", TypeId::Varchar)
        .named_param("predicate", TypeId::Varchar)
        .named_param("region", TypeId::Varchar)
        .named_param("profile", TypeId::Varchar)
        .named_param("timeout_seconds", TypeId::Integer)
        .named_param("result_reuse_minutes", TypeId::Integer)
        .projection_pushdown(true)
        .bind(read_athena_bind)
        .init(read_athena_init)
        .scan(read_athena)
}
