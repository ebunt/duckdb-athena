# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

The build goes through DuckDB's own C-API extension makefiles, vendored as the
`extension-ci-tools` submodule, because that is exactly what
`duckdb/community-extensions` runs. Clone with `--recurse-submodules`, or run
`git submodule update --init` once.

```bash
make configure   # one-off: python venv + platform/version detection under configure/
make release     # cargo build --release, footer appended, packaged
make clean       # cargo clean + build/
```

`make release` leaves the loadable extension at
`build/release/extension/athena/athena.duckdb_extension`.

DuckDB 1.0+ only loads files ending in `.duckdb_extension`, and requires a
metadata footer (platform, extension version, ABI). `base.Makefile` appends it
via `append_extension_metadata.py`; nothing in this repo does that by hand.

Three names must agree, or DuckDB loads the file and finds no entry point:
`EXTENSION_NAME` in the Makefile, `[lib] name` in `Cargo.toml`, and the symbol
passed to `entry_point!` in `src/lib.rs` — DuckDB derives `athena_init_c_api`
from the file name. Renaming the extension means changing all three.

The footer's extension version — what `duckdb_extensions().extension_version`
reports — is derived from the `Cargo.toml` version, so a local build reports
what it is; `release.yml` passes `EXTENSION_VERSION` with the tag name. Keep the
crate version in step with the tag when releasing.

Deliberately *not* set: `USE_UNSTABLE_C_API`. quack-rs targets the stable
`C_STRUCT` ABI, so one build loads across DuckDB 1.x. The official Rust template
sets that flag only because duckdb-rs needs unstable C API functions, which pins
those extensions to a single DuckDB version.

## Lint & format

CI (`.github/workflows/release.yml`) runs `cargo fmt --check`, `cargo clippy` and `cargo test --locked` before building release binaries for 2 platforms (linux amd64, macOS arm64). Each is published as a `athena-<platform>.tar.gz` containing `athena.duckdb_extension` — the file name must stay exactly that, since DuckDB derives the init symbol (`athena_init_c_api`) from it. Run the same locally before pushing:

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
LOAD 'build/release/extension/athena/athena.duckdb_extension';
```

`-unsigned` is required because locally compiled extensions lack a release signature. `allow_extensions_metadata_mismatch` is *not* required (verified on DuckDB 1.5.4/1.5.5, CLI and Python): it only bypasses the footer's platform check, which a build stamped for the host platform already passes. Leave it off so a wrong-platform asset fails loudly.

## Live checks

`scripts/live-check.sh [path/to/extension]` runs the extension against real Athena and compares each answer with Athena computing the same thing natively (19 checks; needs credentials, so CI cannot run it). Run it before cutting a release. It asserts on the SQL Athena received where the value alone would not prove anything — projection pushdown being the case in point.

It reads the database `uv run project/bootstrap.py create` builds: three months of NYC green taxi trips fetched from the TLC's CDN and written to the workgroup's own result bucket, as `duckdb_athena_demo.trips` plus a partitioned `trips_by_month`. That is deliberate — the checks used to depend on tables that existed only in one account, so nobody else could run them. The tables carry a real `DECIMAL(10,2)`, a `BOOLEAN` and `TIMESTAMP`s so the type mapping is exercised end to end rather than only in unit tests. Override the name with `ATHENA_DEMO_DATABASE`, or point the partition checks elsewhere with `ATHENA_TEST_PARTITIONED_TABLE`.

## Testing

`cargo test --locked` runs the unit tests (query building, type mapping, date/timestamp parsing). There is no automated end-to-end suite; verifying against live Athena requires AWS credentials:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

Required IAM permissions: `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`, `athena:StopQueryExecution`, `glue:GetTable`, `s3:PutObject`/`s3:GetObject` on the results bucket.

## Architecture

Rust `cdylib` crate implementing a DuckDB loadable extension that exposes a single table function, `athena_scan`.

- `src/lib.rs` — entry point; registers `athena_scan` via the `quack-rs` `entry_point!` macro and holds a `LazyLock` Tokio runtime reused for all AWS SDK calls (`RUNTIME.block_on(...)`).
- `src/table_function.rs` — DuckDB's bind/init/scan lifecycle. Bind discovers the schema via Glue `GetTable`; init submits the Athena query, polls until it completes, and prepares a lazy `GetQueryResults` paginator; scan streams one result page per call (peak memory is one page, not the whole result set). Projection pushdown is implemented (`SELECT` only requested columns); filter pushdown is not — the loadable-extension C API has no table-filter callback, so `WHERE` runs in DuckDB unless pushed manually with `predicate=`. DuckDB never projects zero columns — it keeps one placeholder column even for `COUNT(*)` — so there is no cardinality-only path to special-case.
- `src/results.rs` — streams the result CSV Athena writes to S3: `parse_s3_uri`, an incremental parser for Athena's CSV dialect (every non-NULL value quoted, NULL as an unquoted empty field, `""` as an empty string, doubled quotes and newlines inside quoted fields), and `CsvRowStream`, which pulls `GetObject` byte chunks and yields at most a vector of rows per scan call.
- `src/types.rs` — maps Athena/Glue types to DuckDB types, including native `DATE`/`TIMESTAMP` and `DECIMAL(width, scale)`. Complex types (`array<…>`, `map<…>`, `struct<…>`) resolve to `ColType::Json`: registered as `Varchar`, but selected as `CAST(col AS JSON)` so the text is parseable — Athena's default rendering is not (`array['a,b', 'c']` prints as `[a,b, c]`, and map/struct use unescaped `=` and `,`). Only when every nested leaf type is JSON-castable, though: Athena rejects `CAST(array(varbinary) AS JSON)` outright, and a rejected cast fails the whole query, so `binary`, `char`, `time` and unrecognised leaves keep the plain-text fallback. Type strings are trimmed and lowercased before matching, because Glue is
  inconsistent about case and spells sized types `varchar(256)`/`char(10)`: matched
  raw, a Glue `Int` silently arrived as VARCHAR and broke arithmetic in DuckDB with
  nothing to explain it. `binary`/`varbinary` map to `Varchar`; genuinely unmapped
  types still fall back to `Varchar` at the call site.
- Values that do not parse become NULL rather than a default, boolean included:
  `populate_column` only accepts `true`/`false` (case-insensitive), so a `"1"` or
  `"t"` is unknown, not `false`. A wrong boolean joins the false rows in every
  aggregate unnoticed; a NULL is excluded and visible.

Errors are `Result<_, String>`, handed to DuckDB via `duckdb_function_set_error`
(bind) or `duckdb_init_set_error` (init); DuckDB adds its own prefix, so they
reach the user as `Binder Error: <message>` from bind and `Invalid Input Error:
<message>` from init. Verified, not assumed:

```
Binder Error: table "duckdb_athena_demo"."no_such_table" in region us-east-1: EntityNotFoundException: Entity Not Found
Binder Error: database must not be empty; omit it to use the default
Invalid Input Error: Athena query 891ed2c0-... Failed: INVALID_LITERAL: line 1:71: 'not-a-date' is not a valid TIMESTAMP literal
```

A failed or cancelled query carries Athena's own `StateChangeReason` — the
syntax error, the denied permission — and says `(Athena gave no reason)` when
Athena supplies none, rather than trailing an empty colon.

**Key constraints**:
- Results are streamed from the single CSV Athena writes at `GetQueryExecution`'s `ResultConfiguration.OutputLocation` (one `GetObject`, needs `s3:GetObject` on the results bucket): 1.07M rows ≈ 3s. `GetQueryResults` paging (1000 rows per call, ~8 rows/ms, ≈135s for the same table) remains only as the fallback for executions exposing no S3 location, e.g. Athena-managed query results. Athena's printed `Run time` is engine time and excludes fetching
- A query still running after 3s prints a heartbeat (`Athena query RUNNING, 4s
  elapsed, 1.31 GB scanned`), repeated every 5s; the poll sleep is clamped to that
  deadline as well as the timeout, or the backoff would stretch a 5s cadence to 9s.
  DuckDB's own progress bar cannot move — the C API exposes no table-function
  progress callback (duckdb/duckdb#25199), so it renders at 0% for the whole wait
- `maxrows` is unlimited by default (no `LIMIT` clause); pass `maxrows=N` (> 0) to add `LIMIT N` to the Athena SQL. Unset or any value <= 0 (e.g. `maxrows=-1`) means all rows, so aggregates/joins see the full table
- `database` defaults to `"default"` (the Glue database name)
- Filter pushdown is not implemented — all filtering happens in DuckDB after the full scan; use `predicate=` to push a raw Athena `WHERE` predicate instead
- The Athena query workgroup defaults to `primary`; override with the `workgroup=` named parameter
- `profile=` selects a named profile from `~/.aws/config`, set as an explicit
  `ProfileFileCredentialsProvider` rather than via `loader.profile_name()`: that
  only tells the profile-file provider which profile to read, and the default chain
  consults environment credentials first, so with `AWS_ACCESS_KEY_ID` set the
  parameter was silently ignored and the scan ran against the environment's
  account. Region resolves `region=` → the profile's region → `AWS_REGION`, via a
  `RegionProviderChain` (a bare profile region provider replaces the chain, which
  drops `AWS_REGION` as a fallback). AWS SDK errors
  display only their outermost layer — for a bad profile that layer is the literal
  string `unhandled error` — so `error_chain` walks `source()` and joins every
  layer, which is what turns that into ``profile `nope` was not defined``
- `region=` overrides the region the AWS config chain resolves; `SdkConfig` is loaded once per region and cached in `aws_config_for`, since bind and init both need a client
- `timeout_seconds=` bounds the poll loop (default 1 hour, `DEFAULT_POLL_WAIT`); on expiry the query is stopped, not abandoned
- `result_reuse_minutes=` enables Athena's `ResultReuseConfiguration` (max 7 days). A reused result scans 0 bytes: measured 846 KB/540 ms → 0 bytes/154 ms on a repeat
- Glue and Athena failures are wrapped with the table, database and region (`describe_target`) — a wrong region reports the same `EntityNotFoundException` as a missing table
- `output_location` is optional; when omitted, no client `ResultConfiguration` is sent to `StartQueryExecution`, so Athena applies the workgroup's own result configuration (location, encryption, ACL, managed results). Athena rejects the query at start time if the workgroup has none. An explicitly empty `output_location`/`workgroup` is a bind error
- The first row of Athena's first result page is always the column header and is skipped in `read_athena`
