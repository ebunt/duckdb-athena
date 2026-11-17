# TODO

Recommendations for `athena_scan`, in rough priority order within each section.
Last reviewed 2026-11-14 against DuckDB 2.0.0 (2.0.1 is due 2026-11-18).

## Where DuckDB 2.0 leaves this extension

- **Stable C API (v1, `duckdb_extension.h`)**: what the published build uses.
  2.0.0 adds two functions to it over v1.5.6 (`duckdb_create_timestamp_tz_ns`,
  `duckdb_get_timestamp_tz_ns`); nothing new for table functions. One build
  stamped `C_STRUCT` / `v1.2.0` loads in DuckDB 1.4.4, 1.5.6 and 2.0.0. CI
  checks 1.5.6 and 2.0.0 on every push; 1.4 left support on 2026-11-17 and is
  no longer tested.
- **C API v2 (`duckdb_extension_v2.h`)**: new in 2.0, unstable, one build per
  DuckDB version. `make release_v2` builds this extension on it through
  `duckdb-neo`, with filter pushdown, a progress callback and bind-time
  cardinality. Maintainers have suggested stabilizing function registration
  "for 2.1" (2027 H1).

Each release publishes both: the v1 build for DuckDB 1.4 through 2.0, and the
v2 build for DuckDB v2.0.0 only. community-extensions builds v1 only.

## Submitting 0.4.3

0.4.3 is tagged on `3025a22` and published on GitHub (2026-11-16), and
`docs/community-extension-description.yml` is pinned to it. community-extensions
builds with `extension-ci-tools` `v2.0-cyanoptera` (its `build.yml`, checked
2026-11-16), the branch this repo's submodule tracks. What remains is the
submission PR there.

## C API v2 track

1. **Accept pushed filters once upstream settles.** Filters are pushed but not
   accepted, so DuckDB re-checks every row. That is deliberate: it makes the
   known v2 defects harmless (duckdb/duckdb#27729, `BETWEEN` bounds not
   exposed; #27475, accepted filters lost across plan serialization). When
   both are fixed in a release, accept filters whose semantics are exact and
   keep the rest advisory.
2. **Widen what is pushed.** Not yet lowered: a bare boolean column
   (`WHERE shipped`), `NOT IN`, `LIKE 'prefix%'`, `IS DISTINCT FROM`, and
   comparisons where DuckDB cast the column rather than the constant. Float
   and double are excluded on purpose; revisit only with accept (item 1).
3. **Parallel scan** via local init and `max_threads`, once results arrive as
   multiple objects (item 8).

4. **Progress freezes when the client thread runs the scan.** DuckDB
   refreshes query progress from the client thread between tasks; when that
   thread picks up the scan, it blocks in `exec` for the whole Athena wait
   and the bar stays at its first reading. Always so with `SET threads = 1`,
   occasionally otherwise (seen once in CI on a 3-core macOS runner,
   2026-11-17). A fix needs a way to yield from `exec` without finishing,
   which C API v2 does not expose yet.

## Both builds

5. **Stop the Athena query when the DuckDB query is interrupted.** Confirmed
   broken, against the fake and against real Athena (2026-11-16: `RUNNING` at
   interrupt, `SUCCEEDED` afterwards): neither C API tells a table function it was interrupted, so the
   poll loop runs until Athena finishes and the query bills in full (an
   interrupt at 1s was honoured at 8s). `tests/e2e` holds this as a strict
   xfail. Options, none free:
   - Ask upstream for an interrupt check on the table-function context.
     Drafted in `docs/upstream-issue-interrupt.md`, not filed yet.
   - Watch SIGINT inside the extension (`tokio::signal`) and stop the query.
     Works for the CLI; intrusive for a library, and blind to
     `connection.interrupt()` from Python or JDBC.
6. **`SET athena_*` session options: not worth it yet.** Session defaults
   ship as environment variables instead (`ATHENA_WORKGROUP`,
   `ATHENA_OUTPUT_LOCATION`; region and profile already have `AWS_REGION`
   and `AWS_PROFILE`), and CI stopped testing 1.4 (2026-11-17). Real `SET`
   options need quack-rs's `duckdb-1-5` feature, which per quack-rs 0.20.0
   means stamping C API v1.5.6: refused by DuckDB 1.4 to 1.5.5, and under its
   `Strict` policy refused by any release quack-rs does not list (2.0.1
   included) until a quack-rs bump or a rebuild with
   `QUACK_RS_TARGET_DUCKDB_VERSION`. Revisit if DuckDB declares config options
   stable in the C API. DuckDB secrets cannot supply credentials on either
   path: the C API has no `duckdb_secret_*` functions, and `duckdb_secrets()`
   redacts the key.
7. **`athena_query('SELECT ...')`.** Arbitrary SQL passthrough. The schema
    must be known at bind, so either run the query at bind and reuse the
    execution in init, or describe it via a prepared statement.
8. **`UNLOAD` to Parquet instead of CSV.** `UNLOAD (SELECT ...) TO 's3://...'
    WITH (format = 'PARQUET')` returns typed, columnar, multi-file results. It
    removes the CSV dialect parser, the text-to-type conversions and the JSON
    detour for nested types, and enables parallel reads. Needs a writable
    prefix, cleanup of the unloaded files, and a Parquet reader. Largest change
    here; measure against the current 1.07M rows in about 3s first.
9. **Native `LIST`/`STRUCT`/`MAP`.** Currently JSON text via
    `CAST(col AS JSON)`. Going native on the CSV path needs a recursive Glue
    type parser and nested vector writes in both adapters. Item 8 would make
    most of this unnecessary.
10. **`catalog=` parameter** for cross-account Glue catalogs (`CatalogId`) and
    Athena federated data catalogs.
11. **Better cardinality.** The estimate is `maxrows`, else Glue's
    `recordCount`/`numRows`, and ignores `predicate=` and pushed filters. On
    partitioned tables, partition counts from Glue would tighten it.

## Build, CI and release

12. **Dependency auditing.** Add `cargo deny` or `cargo audit` to CI. An
    earlier lockfile carried `zip` 6.0.0 via `libduckdb-sys` 1.10502.0, which
    had open RustSec advisories.
13. **Declare an MSRV.** `libduckdb-sys` 1.20000.x needs Rust 1.88. Set
    `rust-version` in `Cargo.toml` after confirming the AWS SDK's own floor.
14. **Platforms.** Enable `linux_arm64` first (Graviton), after building and
    load-testing it here. Leave `osx_amd64` excluded; DuckDB is phasing it out
    in 2.1.
15. **Follow DuckDB patch releases.** Add each to the `load` matrix in
    `ci.yml`. The v2 build moves only when `duckdb-neo` and `libduckdb-sys`
    publish for the new version; bump them with `V2_DUCKDB_VERSION` and the
    `api_version` in `src/v2.rs`.

## Testing

16. **Fake AWS limits.** It now covers truncated result objects, an
    unreadable result location, a refused `StopQueryExecution`, the result
    reuse request, and `profile=`/`region=` resolution (by the key and region
    each request is signed with). A truncated body is caught by the HTTP
    client before `results.rs`'s own `Content-Length` check, which stays
    untested end to end.
17. **Live checks not yet scripted**: the managed-results paging fallback and
    the `DATE`/`TIMESTAMP`/`DECIMAL` round trip (both verified by hand on
    2026-08-30).
18. **Live pushdown check for the v2 build**: assert `DataScannedInBytes`
    drops on the partitioned fixture when a plain `WHERE` names the partition
    column. The fake cannot show pruning.
19. **Check the fake against AWS Emulator**
    ([aws/aws-emulator](https://github.com/aws/aws-emulator)), AWS's own local
    emulator, as a complement to `tests/e2e/fake_aws.py` rather than a
    replacement. The fake stays for what an emulator is unlikely to do on
    demand: a slow query, a 403 or truncated result object, a refused stop,
    and asserting on the SQL and signing region each request carried. What the
    emulator could add is response shapes AWS maintains: run a few scans
    against it and diff its Glue `GetTable` (partition keys, `Parameters`),
    `GetQueryExecution` and S3 `GetObject` against what the fake returns.
    To settle first (checked 2026-11-16 from its repo):
    - **SQL dialect.** Its Athena runs SQLite, not Trino; `CAST(col AS JSON)`
      and `DECIMAL '...'` literals likely fail, so keep those scans simple.
    - **Connecting.** Server mode is HTTPS on `127.0.0.1:9297` with a
      self-signed certificate the extension's Rust SDK must trust. Embedded
      mode is Rust-only.
    - **CI.** Linux packages exist; macOS needs a container or a source build.
    - **Coverage.** Its published fidelity scores and `issues/` for Athena,
      Glue `GetTable` and S3 `GetObject`.

## Cleanup

20. Work through `AUDIT.md`: `project/` duplicates `QUERIES.md` and
    `live-check.sh`; README repeats `QUERIES.md`; the predicate preflight is a
    partial SQL lexer that could shrink to explicit safety checks.
21. Drop the "Work in progress" banner from `README.md`, or say what is.
22. Add a `ruff` config for `tests/` and `scripts/` so Python formatting is
    checked in CI rather than by hand.

## Assumptions, and what backs them

- **One result object per query.** `open_result_csv` reads the execution's
  `ResultConfiguration.OutputLocation`. Tested at 53,513,100 rows: Athena wrote
  one 553.7 MiB CSV. A location that is not a readable object falls back to
  `GetQueryResults` paging, and a stream that ends early fails because bytes
  read are checked against `Content-Length`.
- **Partitioned tables.** Glue keeps partition keys separate and Athena returns
  them last in `SELECT *`, matching bind order. Selecting only a partition
  column reads 0 bytes; a `predicate=` on a partition key prunes. Covered by
  `live-check.sh` when a partitioned fixture exists.
- **v2 bind data belongs to one scan.** The pushdown callback stores its
  predicate in bind data. A self-join and a CTE scanned twice with different
  filters each got their own (covered in `tests/e2e/test_pushdown.py`). If
  DuckDB ever shares bind data between scans, a second, different offer makes
  the scan push nothing; since filters are never accepted the answer is still
  right.
