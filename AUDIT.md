# Project Audit

Audit date: 2026-04-25
Last updated: 2026-04-26

## Summary

`duckdb-athena` is a compact Rust DuckDB extension that registers `athena_scan`, discovers schema from Glue, submits an Athena query, and converts Athena result pages into DuckDB data chunks. The current MVP is understandable and has useful unit tests around SQL construction, but it should be hardened before being treated as a reliable extension.

The highest-value hardening items from the initial audit have been implemented: strict Clippy now passes, Athena result pages are fetched lazily, NULL/parse handling is explicit, workgroup and verbosity are configurable, AWS config is cached, packaging is Rust-native, and projection pushdown is enabled.

## Verification

- `cargo fmt --check` passes.
- `cargo test` passes: 14 unit tests passed.
- `cargo clippy --all-targets --all-features -- -D warnings` passes.
- `make` builds and packages `target/release/duckdb_athena.duckdb_extension`.
- Local DuckDB smoke load passes with `duckdb -unsigned -c "LOAD 'target/release/duckdb_athena.duckdb_extension';"`.
- CI includes a native macOS packaged-extension load smoke test.
- Opt-in AWS integration tests are available in `tests/aws_integration.rs`.
- No live AWS integration tests were run.

## Implemented

- Clippy warnings fixed and Clippy added to CI.
- Rust packaging binary replaces the Python footer script.
- Missing Athena values and invalid typed parses now become DuckDB NULLs.
- Athena result pages are fetched lazily during scan instead of all being collected in init.
- `workgroup` named parameter added; default remains `primary`.
- `verbose` named parameter added; extension progress output is quiet by default.
- Athena failure messages include state-change reason when available.
- Glue tables with no supported columns return a bind error.
- AWS SDK config is cached instead of loaded separately in bind and init.
- Projection pushdown is enabled and rendered into narrower Athena `SELECT` lists.
- `predicate=` is documented as trusted raw Athena SQL with minimal guardrails, not as a full parser.
- AWS integration tests cover basic scan, projection scan, manual predicate scan, and multi-page pagination when the required environment variables are set.

## Remaining High Priority

1. Run the live AWS integration suite against a real test table.

   The tests are implemented but were only verified in skip mode locally. They still need execution against real Athena/Glue/S3 resources.

2. Decide whether to replace trusted raw `predicate=` with an allowlisted predicate parser.

   The current behavior is documented as trusted raw Athena SQL with minimal guardrails. If untrusted callers will pass predicates, implement a parser/renderer for comparisons, `AND`, `IS NULL`, and bounded `IN` lists.

## Medium Priority

1. Cache AWS clients, not just AWS config.

   `SdkConfig` is now cached, but `GlueClient` and `AthenaClient` are still constructed from it. This is cheap, but shared clients could simplify future instrumentation and retries.

2. Narrow dependencies where practical.

   `tokio` enables `full`, and `anyhow` is used only in limited places. Trim features and dependencies after the execution path is stable to reduce compile time and extension footprint.

3. Make `DEFAULT_LIMIT` explicit in the function API.

   The README documents 10,000 rows, but the bind path maps missing or non-positive `maxrows` to `DEFAULT_LIMIT`. Consider clearer naming such as `limit`, and distinguish omitted, zero, and negative values if users need precise control.

## Suggested Next Steps

1. Run a live AWS smoke query manually against a small table.
2. Add opt-in live AWS integration tests.
3. Extend the CI load smoke test to Linux native artifacts if a pinned DuckDB CLI install path is added.
