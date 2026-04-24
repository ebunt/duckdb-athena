# TODO: Athena Filter/Predicate Pushdown

Current state: `athena_scan` registers schema from Glue in `read_athena_bind`, then `read_athena_init` submits `SELECT * FROM "db"."table" [WHERE predicate] [LIMIT n]` to Athena and materializes all result pages before `read_athena` serves chunks to DuckDB. The `predicate=` parameter is manual Athena-side filtering for the MVP; ordinary DuckDB `WHERE` clauses are still applied after Athena returns rows.

## 0. MVP status

- Implemented an explicit `predicate` named parameter, for example `athena_scan(..., predicate='year = 2024')`.
- Added conservative validation to reject empty predicates, semicolons, comments, and obvious full SQL statements.
- Added query-builder tests for identifier quoting, predicate placement, and validation.
- Remaining MVP hardening: validate referenced columns against Glue metadata and decide whether to allow only a narrower predicate grammar.

## 1. Establish the pushdown API path

- Confirm whether the DuckDB C table-function API version used by `libduckdb-sys` exposes filter pushdown. The local bindings expose projection pushdown (`duckdb_table_function_supports_projection_pushdown`) but no table-filter callback or `duckdb_table_filter` accessors.
- If the C API still does not expose predicate pushdown, decide between two implementation tracks:
  - Implement the extension against DuckDB's C++ extension API for native table filter pushdown.
  - Continue hardening the explicit user-facing `predicate` SQL parameter, with clear docs that this is manual pushdown, not optimizer-driven pushdown.
- Track this decision in the README so users understand whether normal `WHERE` clauses are pushed into Athena automatically.

## 2. Add SQL query construction infrastructure

- Replace ad-hoc query string construction in `read_athena_init` with a small query builder module.
- Keep identifier quoting separate from literal rendering. Identifiers should reject or correctly escape double quotes; literals must be type-aware and SQL-escaped.
- Build queries as `SELECT <columns> FROM <qualified_table> [WHERE <predicate>] [LIMIT n]`.
- Add unit tests for identifier quoting, literal escaping, limit handling, and clause ordering.

## 3. Capture and model table schema at bind time

- Extend `ScanBindData` to retain ordered column metadata from Glue: column name, DuckDB type, Athena/Glue type string, ordinal, and whether the column is a partition column.
- Preserve the current ordering rule: data columns first, partition columns after, matching Athena `SELECT *` output.
- Use this metadata later to map DuckDB filter column indexes back to Athena column names.

## 4. Implement projection pushdown first

- Enable `.projection_pushdown(true)` in `build_table_function_def`.
- In `read_athena_init`, read projected column indexes with `quack_rs::table::InitInfo::projected_column_count()` and `projected_column_index()`.
- Generate `SELECT col_a, col_b` instead of `SELECT *` when DuckDB only needs a subset of columns.
- Update `ScanInitData` to store the projected schema used for result conversion, because Athena metadata and DuckDB output vectors will only contain projected columns.
- Verify queries like `SELECT year FROM athena_scan(...) WHERE year = 2024` do not fetch unneeded columns.

## 5. Define supported predicate subset

- Start with predicates that translate safely to Athena SQL:
  - Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`
  - Null checks: `IS NULL`, `IS NOT NULL`
  - Boolean conjunctions: `AND`
  - Small `IN (...)` lists
- Defer or explicitly reject initially:
  - `OR` unless the API exposes enough expression structure to preserve semantics safely
  - `LIKE`, regex, functions, casts, arithmetic expressions, nested structs, arrays, maps
  - Floating-point edge cases such as NaN comparisons
- Keep unsupported filters in DuckDB when using real optimizer-driven pushdown. Never drop a filter unless DuckDB will still evaluate it locally.

## 6. Translate DuckDB predicates to Athena SQL

- Add a `predicate.rs` module that converts DuckDB filter expressions into an internal AST, then renders Athena SQL.
- Validate column references against the bind-time schema; never interpolate raw column names from DuckDB without quoting.
- Render values based on the Glue/Athena type:
  - Numeric and boolean literals without quotes after parsing.
  - String, varchar, and char literals with SQL escaping.
  - Date and timestamp values as typed Athena-compatible literals or quoted strings, depending on how DuckDB exposes them.
- Add tests for each supported operator and type, including escaping cases like embedded single quotes.

## 7. Integrate predicates into Athena execution

- Store the pushed predicate SQL in `ScanBindData` or collect it in `read_athena_init`, depending on which DuckDB API path is chosen.
- Change `read_athena_init` to submit `SELECT <projection> FROM <table> WHERE <pushed_predicate> LIMIT <n>`.
- Log the generated Athena query at debug/trace level rather than always printing raw query text.
- Preserve existing error handling: invalid or unsupported pushdown should either fall back to DuckDB-side filtering or return a clear bind/init error for manual `where=` mode.

## 8. Avoid materializing all Athena result pages

- Convert `ScanInitData` from `Vec<GetQueryResultsOutput>` to streaming paginator state plus current page rows.
- Fetch result pages lazily from `read_athena` so pushed filters, projections, and limits reduce memory and latency end-to-end.
- Keep first-page header skipping behavior, but make it explicit in the page reader state.

## 9. Add verification coverage

- Add pure Rust unit tests for query building and predicate rendering.
- Add integration tests behind an opt-in feature or environment gate for live AWS, for example `ATHENA_TEST_DATABASE`, `ATHENA_TEST_TABLE`, and `ATHENA_TEST_OUTPUT`.
- Verify with Athena query statistics that partition predicates reduce `data_scanned_in_bytes`.
- Test fallback behavior for unsupported predicates to ensure results stay correct.

## 10. Update docs and examples

- Update README limitations once pushdown is implemented.
- Document the supported predicate subset and any fallback behavior.
- Add examples for partition-column filters, ordinary column filters, projection plus filter, and unsupported filters.
- Include a troubleshooting note explaining that Athena can still scan substantial data unless the predicate matches table partitions or file-format statistics.
