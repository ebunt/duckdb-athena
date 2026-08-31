use libduckdb_sys::duckdb_connection;
use quack_rs::entry_point;
use quack_rs::error::ExtensionError;
use std::sync::LazyLock;
use tokio::runtime::Runtime;

mod results;
mod table_function;
mod types;

use crate::table_function::build_table_function_def;

static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("Creating Tokio runtime"));

fn register_functions(con: duckdb_connection) -> Result<(), ExtensionError> {
    let builder = build_table_function_def();
    unsafe { builder.register(con) }
}

entry_point!(athena_init_c_api, register_functions);
