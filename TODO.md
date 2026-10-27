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
return. A `predicate=` is checked at bind: no statement separators, comments or
statement keywords, and every column it names must exist in the Glue schema.

## Not done

- **Native `LIST`/`STRUCT`/`MAP` for complex columns.** They currently arrive as
  JSON text (`CAST(col AS JSON)`), which is queryable via DuckDB's json
  functions. Going native needs a recursive parser for Glue type strings, the
  nested logical-type constructors, and writing nested vectors through raw FFI
  (`duckdb_list_vector_reserve`/`set_size`/`get_child`, `duckdb_struct_vector_get_child`;
  quack-rs wraps none of these, and DuckDB's `MAP` is a `LIST` of
  `STRUCT(key, value)` underneath). Roughly a week, most of it unsafe code.

## Blocked

- **Optimizer-driven filter pushdown.** `libduckdb-sys` exposes projection
  pushdown but no table-filter callback / `duckdb_table_filter` accessors, so a
  plain `WHERE` cannot be pushed into Athena automatically. Still true for the
  stable C API (v1, `duckdb_extension.h`) as of DuckDB v2.0.0.

  Upstream: duckdb/duckdb#25163, filed 2026-08-30, argument kept in
  `docs/upstream-issue-filter-pushdown.md`. Closed 2026-10-13 as "fixed on
  nightly" — checked against the actual commit (`src/main/capi/v2/capi_v2_func_table.cpp`,
  Oct 7-11 2026, branch `v2.0-cyanoptera`) and it isn't usable yet:

  - It shipped in a brand-new **C API v2**, not as an addition to the stable
    v1 API this extension builds against.
  - The shape differs from what was requested: no column/comparator/constant
    accessors. The callback (`duckdb_v2_table_function_set_filter_pushdown_callback`)
    hands over `duckdb_v2_expression_handle` nodes
    (`..._filter_pushdown_get_filter`) to walk via the v2 expression API, and
    an index-based `..._accept` to mark each one handled.
  - `quack-rs` (the Rust wrapper this extension depends on) has no path to it:
    0.19.0, its latest release as of 2026-10-23, states in its own changelog
    that DuckDB 2.0 loads a v2 pushdown binary "only through a C API v2 entry
    point (`<name>_init_c_api_v2`), which quack-rs does not generate."

  **Still blocked as of DuckDB 2.0.0 "Cyanoptera" (released 2026-10-21).**
  DuckDB's own 2.0 announcement is explicit that C API v2 is not yet marked
  stable — the team wants field testing first. So the release that was
  supposed to resolve this changed nothing here: v2 is in a tagged release
  now, but it's still unstable and `quack-rs` still doesn't wrap it. Revisit
  when either lands: `quack-rs` adds v2 support, or DuckDB marks v2 stable.
  Manual `predicate=` is the supported path meanwhile, and on partitioned
  tables it already prunes.

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

  Partitioned tables were checked on 2026-08-30 and behave correctly: Glue keeps
  partition keys separate from data columns and Athena returns them last in
  `SELECT *`, which is the order bind registers them in. Selecting only a
  partition column reads 0 bytes (the values come from the catalog), and a
  `predicate=` on a partition key prunes (284 -> 189 bytes on the fixture). Two
  of these are now in `live-check.sh`, guarded on a partitioned fixture existing
  (`ATHENA_TEST_PARTITIONED_TABLE`, default `default.claude_part_test`).

  Neither of the two checks above is automated, but `scripts/live-check.sh` now
  covers the rest:
  value parity against Athena computing the same aggregates, a full scan
  completing in seconds rather than paging, `maxrows`/`predicate` reaching
  Athena, projection pushdown asserted against the SQL Athena actually
  received, and an unknown predicate column being rejected at bind. Run it
  before cutting a release; it needs credentials, so CI cannot.

## Assumptions, and what backs them

- **One result object per query.** `open_result_csv` reads a single S3 object,
  the execution's `ResultConfiguration.OutputLocation`. Tested at 53,513,100
  rows (a cross join over nyctaxi), where Athena still wrote exactly one
  553.7 MiB `<query-id>.csv` plus its `.metadata` sibling — no splitting. The
  two ways this could go wrong are both handled rather than assumed: a location
  that is not a readable object makes `GetObject` fail, which logs and falls
  back to `GetQueryResults` paging, and a stream that ends early now fails
  loudly because bytes read are checked against the object's `Content-Length`
  (a cut on a row boundary parses cleanly and would otherwise be reported as a
  complete, shorter result).

## Predicate translation design (for whichever filter-pushdown API arrives)

If/when native filter pushdown becomes available (C++ extension API, or a
wrapped DuckDB C API v2):

- Supported subset first: `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] NULL`,
  `AND`, small `IN (...)` lists. Defer `OR`, `LIKE`, functions, casts,
  arithmetic, and complex types. Never drop a filter unless DuckDB still
  evaluates it locally.
- Add a `predicate.rs` that lowers DuckDB filter expressions to an internal AST,
  then renders Athena SQL — quoting column references from the schema and
  rendering literals type-aware (numeric/boolean unquoted, strings SQL-escaped,
  date/timestamp as typed literals). Unit-test each operator and escaping case.
