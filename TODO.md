# TODO

Recommendations for `athena_scan`, in rough priority order within each section.
Last reviewed 2026-11-14 against DuckDB 2.0.0 (2.0.1 is due 2026-11-18).

## Where DuckDB 2.0 leaves this extension

- **Stable C API (v1, `duckdb_extension.h`)**: what the published build uses.
  2.0.0 adds two functions to it over v1.5.6 (`duckdb_create_timestamp_tz_ns`,
  `duckdb_get_timestamp_tz_ns`); nothing new for table functions. One build
  stamped `C_STRUCT` / `v1.2.0` loads in DuckDB 1.4.4, 1.5.6 and 2.0.0, and CI
  now checks that on every push.
- **C API v2 (`duckdb_extension_v2.h`)**: new in 2.0, unstable, one build per
  DuckDB version. `make release_v2` builds this extension on it through
  `duckdb-neo`, with filter pushdown, a progress callback and bind-time
  cardinality. Maintainers have suggested stabilizing function registration
  "for 2.1" (2027 H1).

The shipped artifact stays on v1. The v2 build is kept green in CI so it is
ready when v2 stabilizes.

## Before releasing 0.4.1

Done on 2026-11-16: `scripts/live-check.sh` passed on a fresh `make release`
of the refactored scan, `scripts/cancel-check.sh` confirmed the interrupt leak
against real Athena (item 5), and the `extension-ci-tools` submodule moved to
`c286b12` on `v2.0-cyanoptera`.

1. **Merge, tag, repin.** Push `duckdb-2.0-migration`, merge it, and tag the
   merge commit `v0.4.1` (the crate and descriptor versions are already
   bumped; the tag triggers `release.yml`). Then set `ref` in
   `docs/community-extension-description.yml` to that merge commit: it still
   names the 0.4.0 one. Before submitting, confirm `v2.0-cyanoptera` is the
   `extension-ci-tools` ref duckdb/community-extensions builds 2.0 extensions
   with. A local build keeps reporting v0.4.0 until
   `configure/extension_version.txt` is removed and `make configure` re-run.

## C API v2 track

2. **Accept pushed filters once upstream settles.** Filters are pushed but not
   accepted, so DuckDB re-checks every row. That is deliberate: it makes the
   known v2 defects harmless (duckdb/duckdb#27729, `BETWEEN` bounds not
   exposed; #27475, accepted filters lost across plan serialization). When
   both are fixed in a release, accept filters whose semantics are exact and
   keep the rest advisory.
3. **Widen what is pushed.** Not yet lowered: a bare boolean column
   (`WHERE shipped`), `NOT IN`, `LIKE 'prefix%'`, `IS DISTINCT FROM`, and
   comparisons where DuckDB cast the column rather than the constant. Float
   and double are excluded on purpose; revisit only with accept (item 2).
4. **Parallel scan** via local init and `max_threads`, once results arrive as
   multiple objects (item 8).

## Both builds

5. **Stop the Athena query when the DuckDB query is interrupted.** Confirmed
   broken, against the fake and against real Athena (2026-11-16: `RUNNING` at
   interrupt, `SUCCEEDED` afterwards): neither C API tells a table function it was interrupted, so the
   poll loop runs until Athena finishes and the query bills in full (an
   interrupt at 1s was honoured at 8s). `tests/e2e` holds this as a strict
   xfail. Options, none free:
   - Ask upstream for an interrupt check on the v2 table-function context
     (the v2 feedback thread lists interruption as expected "eventually").
   - Watch SIGINT inside the extension (`tokio::signal`) and stop the query.
     Works for the CLI; intrusive for a library, and blind to
     `connection.interrupt()` from Python or JDBC.
6. **Raise the floor to DuckDB 1.5** once 1.4 leaves support (2026-11-17).
   Building with a quack-rs `duckdb-1-5*` feature (stamped v1.5.6) unlocks
   config options and the client context: `SET athena_workgroup = ...`,
   `athena_output_location`, `athena_region`, `athena_profile` as session
   defaults instead of repeating named parameters. Cost: no more 1.4 loads.
   Investigate whether DuckDB secrets (`CREATE SECRET (TYPE s3)`) can supply
   credentials on this path.
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

16. **Grow the fake AWS** (`tests/e2e/fake_aws.py`) where it is thin: short
    reads and truncated result objects, an unreadable result location (the
    paging fallback's other trigger), a refused `StopQueryExecution`, result
    reuse, and `profile=`/`region=` resolution.
17. **Live checks not yet scripted**: the managed-results paging fallback and
    the `DATE`/`TIMESTAMP`/`DECIMAL` round trip (both verified by hand on
    2026-08-30).
18. **Live pushdown check for the v2 build**: assert `DataScannedInBytes`
    drops on the partitioned fixture when a plain `WHERE` names the partition
    column. The fake cannot show pruning.

## Cleanup

19. Work through `AUDIT.md`: `project/` duplicates `QUERIES.md` and
    `live-check.sh`; README repeats `QUERIES.md`; the predicate preflight is a
    partial SQL lexer that could shrink to explicit safety checks.
20. Drop the "Work in progress" banner from `README.md`, or say what is.
21. Add a `ruff` config for `tests/` and `scripts/` so Python formatting is
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
