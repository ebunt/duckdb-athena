use std::sync::LazyLock;
use tokio::runtime::Runtime;

mod results;
mod scan;
mod types;

// Two builds, one crate. The default is the C API v1 adapter, which is what
// ships. `--features capi-v2` swaps in the C API v2 adapter instead: filter
// pushdown and a progress bar, but pinned to one exact DuckDB version. See
// CLAUDE.md before reaching for it.
#[cfg(any(test, feature = "capi-v2"))]
mod predicate;
#[cfg(not(feature = "capi-v2"))]
mod table_function;
#[cfg(feature = "capi-v2")]
mod v2;

static RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("Creating Tokio runtime"));

#[cfg(not(feature = "capi-v2"))]
mod v1_entry {
    use crate::table_function::build_table_function_def;
    use libduckdb_sys::duckdb_connection;
    use quack_rs::entry_point;
    use quack_rs::error::ExtensionError;

    fn register_functions(con: duckdb_connection) -> Result<(), ExtensionError> {
        let builder = build_table_function_def();
        unsafe { builder.register(con) }
    }

    entry_point!(athena_init_c_api, register_functions);
}
