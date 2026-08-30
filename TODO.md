# TODO

Current state: `athena_scan` registers schema from Glue in `read_athena_bind`,
reads DuckDB's projected columns in `read_athena_init`, submits
`SELECT <projection> FROM "db"."table" [WHERE predicate] [LIMIT n]` to Athena,
then streams the result CSV Athena wrote to S3 — one `GetObject`, parsed
incrementally, a vector of rows per `read_athena` call, so peak memory stays
near one byte chunk. `GetQueryResults` paging (1000 rows per call) remains only
as the fallback for executions exposing no S3 location, e.g. Athena-managed
query results. Column projection is pushed into Athena; filtering with
`predicate=` is manual, and a plain DuckDB `WHERE` runs locally after rows
return.

## Blocked

- **Optimizer-driven filter pushdown.** `libduckdb-sys` exposes projection
  pushdown but no table-filter callback / `duckdb_table_filter` accessors, so a
  plain `WHERE` cannot be pushed into Athena automatically. Real predicate
  pushdown needs the DuckDB C++ extension API — a separate architectural
  decision. Manual `predicate=` is the supported path until then. Design notes
  for that path are kept at the bottom of this file.

## Deferred

- **Live AWS integration tests.** Gate behind env vars (`ATHENA_TEST_DATABASE`,
  `ATHENA_TEST_TABLE`, `ATHENA_TEST_OUTPUT`); assert partition predicates reduce
  `data_scanned_in_bytes`. Not wired into CI (needs credentials + fixtures).

  Both paths that had only unit coverage have since been checked by hand
  against live Athena (2026-08-30), on throwaway resources deleted afterwards:

  - **`GetQueryResults` fallback** — a workgroup with
    `ManagedQueryResultsConfiguration` enabled produces executions whose
    `ResultConfiguration.OutputLocation` is null, so the scan pages instead of
    reading S3. `sampledb.elb_logs` returned 4229 rows and `SUM(sentbytes)` of
    35299472, identical to the S3 path; `maxrows=2500` capped correctly.
  - **Native `DATE`/`TIMESTAMP`/`DECIMAL`** — a CTAS table with those Glue types
    round-tripped exactly through both paths: pre-epoch `1969-12-31`, millisecond
    timestamps, `DECIMAL(10,2)` values `-0.05`/`1234.56`/`0.00`, NULLs preserved
    per column, and an empty string staying empty rather than becoming NULL.

  Neither check is automated, so both can regress silently.

## Open review finding

- **Validate predicate column references** against the bind-time schema instead
  of accepting any raw expression (carryover MVP hardening).

## Predicate translation design (for the C++ API path)

If/when the extension moves to the C++ extension API for native filter pushdown:

- Supported subset first: `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] NULL`,
  `AND`, small `IN (...)` lists. Defer `OR`, `LIKE`, functions, casts,
  arithmetic, and complex types. Never drop a filter unless DuckDB still
  evaluates it locally.
- Add a `predicate.rs` that lowers DuckDB filter expressions to an internal AST,
  then renders Athena SQL — quoting column references from the schema and
  rendering literals type-aware (numeric/boolean unquoted, strings SQL-escaped,
  date/timestamp as typed literals). Unit-test each operator and escaping case.
