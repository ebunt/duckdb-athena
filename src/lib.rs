use aws_config::BehaviorVersion;
use lazy_static::lazy_static;
use libduckdb_sys::duckdb_connection;
use quack_rs::entry_point;
use quack_rs::error::ExtensionError;
use tokio::runtime::Runtime;

pub mod error;
mod table_function;
mod types;

use crate::table_function::build_table_function_def;

lazy_static! {
    static ref RUNTIME: Runtime = tokio::runtime::Runtime::new().expect("Creating Tokio runtime");
    static ref AWS_CONFIG: aws_config::SdkConfig =
        RUNTIME.block_on(aws_config::defaults(BehaviorVersion::latest()).load());
}

fn register_functions(con: duckdb_connection) -> Result<(), ExtensionError> {
    let builder = build_table_function_def();
    unsafe { builder.register(con) }
}

entry_point!(duckdb_athena_init_c_api, register_functions);
