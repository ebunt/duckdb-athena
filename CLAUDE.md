# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

Always use `make` rather than `cargo build --release` directly — the Makefile handles compilation, the required `.duckdb_extension` rename, and appending the DuckDB extension footer:

```bash
make        # cargo build --release + copy to target/release/duckdb_athena.duckdb_extension + append footer
make clean  # cargo clean
```

DuckDB 1.0+ only loads files ending in `.duckdb_extension`, and requires a metadata footer (platform, extension version, ABI) appended to the binary. The Makefile installs the pinned quack-rs `append_metadata` binary under `target/tools` and uses it to package the platform-specific output (`libduckdb_athena.dylib` on macOS, `.so` on Linux, `.dll` on Windows).

The footer's extension version — what `duckdb_extensions().extension_version` reports — defaults to the `Cargo.toml` version (`v0.2.2`), so a local build reports what it is; `release.yml` overrides `DUCKDB_EXTENSION_VERSION` in the environment with the tag name. Keep the crate version in step with the tag when releasing.

## Lint & format

CI (`.github/workflows/release.yml`) runs `cargo fmt --check`, `cargo clippy` and `cargo test --locked` before building release binaries for 2 platforms (linux amd64, macOS arm64). Each is published as a `duckdb_athena-<platform>.tar.gz` containing `duckdb_athena.duckdb_extension` — the file name must stay exactly that, since DuckDB derives the init symbol (`duckdb_athena_init_c_api`) from it. Run the same locally before pushing:

```bash
cargo fmt --check
cargo clippy
cargo test --locked
```

## Loading the extension in DuckDB

```bash
AWS_REGION=us-east-1 duckdb -unsigned
```

```sql
LOAD 'target/release/duckdb_athena.duckdb_extension';
```

`-unsigned` is required because locally compiled extensions lack a release signature. `allow_extensions_metadata_mismatch` is *not* required (verified on DuckDB 1.5.4/1.5.5, CLI and Python): it only bypasses the footer's platform check, which a build stamped for the host platform already passes. Leave it off so a wrong-platform asset fails loudly.

## Live checks

`scripts/live-check.sh [path/to/extension]` runs the extension against real Athena and compares each answer with Athena computing the same thing natively (12 checks; needs credentials, so CI cannot run it). Run it before cutting a release. It asserts on the SQL Athena received where the value alone would not prove anything — projection pushdown being the case in point.

## Testing

`cargo test --locked` runs the unit tests (query building, type mapping, date/timestamp parsing). There is no automated end-to-end suite; verifying against live Athena requires AWS credentials:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

Required IAM permissions: `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`, `glue:GetTable`, `s3:PutObject`/`s3:GetObject` on the results bucket.

## Architecture

Rust `cdylib` crate implementing a DuckDB loadable extension that exposes a single table function, `athena_scan`.

- `src/lib.rs` — entry point; registers `athena_scan` via the `quack-rs` `entry_point!` macro and holds a `LazyLock` Tokio runtime reused for all AWS SDK calls (`RUNTIME.block_on(...)`).
- `src/table_function.rs` — DuckDB's bind/init/scan lifecycle. Bind discovers the schema via Glue `GetTable`; init submits the Athena query, polls until it completes, and prepares a lazy `GetQueryResults` paginator; scan streams one result page per call (peak memory is one page, not the whole result set). Projection pushdown is implemented (`SELECT` only requested columns); filter pushdown is not — the loadable-extension C API has no table-filter callback, so `WHERE` runs in DuckDB unless pushed manually with `predicate=`. DuckDB never projects zero columns — it keeps one placeholder column even for `COUNT(*)` — so there is no cardinality-only path to special-case.
- `src/results.rs` — streams the result CSV Athena writes to S3: `parse_s3_uri`, an incremental parser for Athena's CSV dialect (every non-NULL value quoted, NULL as an unquoted empty field, `""` as an empty string, doubled quotes and newlines inside quoted fields), and `CsvRowStream`, which pulls `GetObject` byte chunks and yields at most a vector of rows per scan call.
- `src/types.rs` — maps Athena/Glue types to DuckDB types, including native `DATE`/`TIMESTAMP` and `DECIMAL(width, scale)`. Complex types (`array<…>`, `map<…>`, `struct<…>`) resolve to `ColType::Json`: registered as `Varchar`, but selected as `CAST(col AS JSON)` so the text is parseable — Athena's default rendering is not (`array['a,b', 'c']` prints as `[a,b, c]`, and map/struct use unescaped `=` and `,`). Unmapped types fall back to `Varchar`.

Errors are `Result<_, String>`, surfaced to DuckDB as `Athena(DuckDB): <message>`.

**Key constraints**:
- Results are streamed from the single CSV Athena writes at `GetQueryExecution`'s `ResultConfiguration.OutputLocation` (one `GetObject`, needs `s3:GetObject` on the results bucket): 1.07M rows ≈ 3s. `GetQueryResults` paging (1000 rows per call, ~8 rows/ms, ≈135s for the same table) remains only as the fallback for executions exposing no S3 location, e.g. Athena-managed query results. Athena's printed `Run time` is engine time and excludes fetching
- `maxrows` is unlimited by default (no `LIMIT` clause); pass `maxrows=N` (> 0) to add `LIMIT N` to the Athena SQL. Unset or any value <= 0 (e.g. `maxrows=-1`) means all rows, so aggregates/joins see the full table
- `database` defaults to `"default"` (the Glue database name)
- Filter pushdown is not implemented — all filtering happens in DuckDB after the full scan; use `predicate=` to push a raw Athena `WHERE` predicate instead
- The Athena query workgroup defaults to `primary`; override with the `workgroup=` named parameter
- `output_location` is optional; when omitted, no client `ResultConfiguration` is sent to `StartQueryExecution`, so Athena applies the workgroup's own result configuration (location, encryption, ACL, managed results). Athena rejects the query at start time if the workgroup has none. An explicitly empty `output_location`/`workgroup` is a bind error
- The first row of Athena's first result page is always the column header and is skipped in `read_athena`
