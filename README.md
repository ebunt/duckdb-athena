# DuckDB Athena Extension

> **Work in progress** — things may not work as expected

Query Amazon Athena tables directly from DuckDB using `athena_scan`.

## Install

Prebuilt (unsigned) binaries are published on the
[Releases page](https://github.com/ebunt/duckdb-athena/releases) for
`linux_amd64` and `osx_arm64` only. On any other platform, [build from
source](#build).

```bash
# pick the asset matching your platform
gh release download --repo ebunt/duckdb-athena \
  --pattern 'athena-osx_arm64.tar.gz'
tar -xzf athena-osx_arm64.tar.gz
```

The Linux binary is built in the manylinux image DuckDB's own community
extensions use, so it needs no glibc newer than **2.28** — it loads on Amazon
Linux 2023, RHEL 8+, Ubuntu 20.04+, Debian 11+ and AWS CloudShell. CI fails the
build if that baseline ever rises.

The archive contains a single file, `athena.duckdb_extension`. **Do not
rename it.** DuckDB derives the extension's init symbol from the file name, so a
renamed file fails to load with:

```
IO Error: File "..." did not contain function "<name>_init_c_api": symbol not found
```

Load it by path, or install it into DuckDB's extension directory once and load
it by name in later sessions:

```sql
-- load from the path
LOAD '/path/to/athena.duckdb_extension';

-- or install once, then load by name in any later session
INSTALL '/path/to/athena.duckdb_extension';
LOAD athena;
```

Either way DuckDB must be started with the flags shown under [Load](#load).
`INSTALL` does not sign anything, so an installed extension still needs
`-unsigned` in every later session.

## Prerequisites

- Rust stable toolchain (`rustup install stable`) — only to [build from source](#build)
- DuckDB CLI v1.5+
- AWS credentials with access to Athena, Glue, and S3

## Build

The build uses DuckDB's own C-API extension makefiles, vendored as a submodule,
so a local build is byte-for-byte the process `duckdb/community-extensions`
runs.

```bash
git submodule update --init   # once, if not cloned with --recurse-submodules
make configure                # once: python venv, platform and version detection
make release
```

This places the extension at `build/release/extension/athena/athena.duckdb_extension`.

## Releasing

Pushing a `v*` tag triggers the release workflow (`.github/workflows/release.yml`):
it runs `cargo fmt --check`, clippy and the tests, builds the extension for the
two supported platforms (`linux_amd64`, `osx_arm64`), and publishes a GitHub
Release with a `athena-<platform>.tar.gz` asset for each.

```bash
# land your changes on main first
git switch main
git pull

# cut a release
git tag v0.1.0
git push origin v0.1.0
```

Regular pushes to `main` do **not** trigger a release — they only run tests via
`.github/workflows/ci.yml`. To rebuild the artifacts without cutting a tag, run
the workflow manually from the Actions tab (`workflow_dispatch`).

## AWS credentials

Set standard AWS environment variables, or use any credential source the AWS SDK supports (instance profile, SSO, `~/.aws/credentials`):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

Required IAM permissions:
- `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
- `athena:StopQueryExecution` — used by `timeout_seconds=` and to cancel by hand;
  without it a timed-out query keeps scanning and billing
- `glue:GetTable` on the target table
- `s3:PutObject`, `s3:GetObject` on the S3 results bucket

## Load

Start DuckDB with the `-unsigned` flag, required because the extension is built
without a release signature:

```bash
duckdb -unsigned
```

Then load the extension:

```sql
LOAD 'build/release/extension/athena/athena.duckdb_extension';
```

-OR-

In one statement:

```bash
duckdb -unsigned -cmd "LOAD 'build/release/extension/athena/athena.duckdb_extension';"
```

> `SET allow_extensions_metadata_mismatch=true` is **not** required, on either a
> `make` build or a release asset. It only disables the footer's platform check,
> and a build stamped for your own platform passes that check. If loading fails
> with `The file was built for the platform 'linux_amd64', but we can only load
> extensions built for platform 'osx_arm64'`, you have the wrong asset —
> download the matching one rather than forcing the load.

## Usage

See [QUERIES.md](QUERIES.md) for runnable examples against real Glue databases
(projection pushdown, `predicate=` filtering, cross-table joins, and gotchas
like hyphenated database names and columns with spaces).

### Basic scan

The Glue table name is the only required argument. The Athena results S3
location is taken from the workgroup's default result configuration:

```sql
SELECT * FROM athena_scan('my_table');
```

> **Large tables:** a bare `SELECT *` returns every row. Results stream one
> Athena result page at a time (peak memory is a single page), but Athena must
> finish executing the query first — the extension polls until the query
> completes — so time-to-first-row is gated on that poll, not on the
> streaming. On a big table this can take a long time and looks like a hang.
> When exploring, bound it with `maxrows=N` — see [Limit rows](#limit-rows).

### Results location and workgroup

By default the query runs in the `primary` workgroup. When `output_location` is
omitted, no client result configuration is sent, so Athena applies the
workgroup's own settings — output location, encryption, and managed query
results included. Override either with named parameters:

```sql
-- explicit results location
SELECT * FROM athena_scan('my_table', output_location='s3://my-results-bucket/prefix/');

-- different workgroup (uses that workgroup's own result configuration)
SELECT * FROM athena_scan('my_table', workgroup='analytics');
```

If no `output_location` is given and the workgroup has no result configuration,
Athena rejects the query when it starts.

### Region

Defaults to whatever the AWS config chain resolves — `AWS_REGION`, or the active
profile. Pass `region=` to read a table in another region without restarting
DuckDB:

```sql
SELECT * FROM athena_scan('my_table', region='eu-west-1');
```

Region is worth setting explicitly when a lookup fails: a table that exists in
another region reports the same "Entity Not Found" as one that does not exist,
so the error message always names the region it searched.

### Profile

`profile=` picks a named profile from `~/.aws/config` for that scan, so one
session can read across accounts without restarting DuckDB to change
`AWS_PROFILE`:

```sql
SELECT * FROM athena_scan('my_table', profile='prod', region='eu-west-1');
```

The named profile supplies the credentials, overriding `AWS_ACCESS_KEY_ID` and
friends for that scan — otherwise the default chain would read the environment
first and `profile=` would silently change nothing. Region resolves in the order
`region=`, then the profile's own region, then `AWS_REGION` and the rest of the
chain. A profile that does not exist fails at bind, naming itself:

```
Binder Error: table "duckdb_athena_demo"."trips" in region us-east-1: dispatch failure:
other: the credentials provider was not properly configured: ProfileFile
provider could not be built: profile `nope` was not defined
```

### Reuse a recent result

Athena can return a previous identical query's result instead of re-running it,
which scans no data and therefore costs nothing. Opt in per scan with a maximum
age in minutes (Athena's own limit is 7 days):

```sql
SELECT COUNT(*) FROM athena_scan('my_table', result_reuse_minutes=60);
```

Measured on the demo table: the first run scanned 259.32 KB in 693 ms of engine
time, the second scanned **0 bytes** in 147 ms and returned the same answer. Leave it off when the underlying data changes within the window.

### Bound how long a query may run

The scan waits up to an hour for Athena to finish, then stops the query rather
than leaving it running. `timeout_seconds=` overrides that, to wait longer than
a workgroup whose DML limit has been raised, or to fail fast:

```sql
SELECT * FROM athena_scan('my_table', timeout_seconds=120);
```

> **Ctrl-C does not stop the Athena query.** The scan spends its time blocked in
> the poll loop, which does not check DuckDB's interrupt flag, so an interrupted
> query keeps running to completion and you are billed for every byte it scans —
> DuckDB only reports the cancellation once the poll returns. Measured: a 91 MB
> `COUNT(*)` interrupted 1.2 s in still reached `SUCCEEDED`, scanning 95,774,673
> bytes. To cancel for real, stop it in Athena using the execution id the scan
> printed when it started:
>
> ```bash
> aws athena stop-query-execution --query-execution-id <execution id> --region <region>
> ```
>
> `--region` matters: with `region=` the query runs in that region, not the one
> your CLI defaults to, and a stop sent to the wrong region cancels nothing.
>
> `timeout_seconds=` is the exception: on expiry the extension stops the query
> itself rather than leaving it running — provided the caller holds
> `athena:StopQueryExecution`. If the stop is refused, the error says so instead
> of leaving a silent charge.

### Specify a Glue database

Defaults to the `default` database. Pass `database=` to override:

```sql
SELECT * FROM athena_scan('my_table', database='my_database');
```

### Limit rows

By default all rows are returned, so aggregates and joins over `athena_scan`
see the full table. Pass `maxrows=N` to cap the Athena query with `LIMIT N`:

```sql
SELECT * FROM athena_scan('my_table', maxrows=1000);
```

Note: a plain DuckDB `LIMIT` in the outer query is not pushed to Athena (there
is no filter pushdown), so Athena still returns every row and DuckDB trims
afterward. Use `maxrows=` to limit what Athena actually returns.

### Filter results

DuckDB `WHERE` clauses are not pushed down automatically yet. For the MVP, pass an Athena SQL predicate with `predicate=` to add a `WHERE` clause to the query submitted to Athena:

```sql
SELECT *
FROM athena_scan(
  'my_table',
  database='my_database',
  predicate='year = 2024'
);
```

You can still use a normal DuckDB `WHERE` clause for local filtering after Athena returns results:

```sql
SELECT *
FROM athena_scan('my_table', predicate='year = 2024')
WHERE event_type = 'click';
```

### Count rows

Athena writes each query's full result set as a single CSV object at the
execution's result location, and the extension streams that object with one
`GetObject`. Counting the demo table's 167,585 rows takes about two seconds,
and the same approach returns a million rows in about three:

```sql
SELECT COUNT(*) FROM athena_scan('my_table');
```

`COUNT(*)` is not special-cased — DuckDB keeps one placeholder column for it, so
the scan reads one column of every row, which the streamed result makes cheap.
`maxrows=N` and `predicate=` still bound what Athena returns:

```sql
SELECT COUNT(*) FROM athena_scan('my_table', maxrows=1000);
SELECT COUNT(*) FROM athena_scan('my_table', predicate='year = 2024');
```

If a workgroup uses Athena-managed query results and exposes no S3 location, the
scan falls back to paging `GetQueryResults` 1000 rows at a time, which is roughly
8 rows/ms — the old behavior, and slow on large results.

The execution id is printed when the query is submitted and Athena's own
statistics when it finishes. In between, any query still running after three
seconds reports a heartbeat, and repeats it every five seconds:

```
Running Athena query, execution id: 00000000-0000-0000-0000-000000000000
Athena query RUNNING, 4s elapsed, 1.31 GB scanned
Time in queue: 97 ms
Run time: 5050 ms
Data scanned: 1.31 GB
```

Bytes appear once Athena starts executing; while it is still planning it
publishes none, and the line omits them rather than reporting `0 bytes scanned`,
which would read as a measurement instead of an absence. Once real they are
worth watching, being the bill as well as the progress.

> **DuckDB's own progress bar stays at 0% for the whole wait.** Nothing in this
> extension can move it: the C loadable-extension API's entire table-function
> surface is bind/init/function, parameters, and projection pushdown — there is
> no progress callback, though C++ table functions have `table_scan_progress`.
> The heartbeat above scrolls that bar away, which is the trade this makes
> deliberately: a bar frozen at 0% conveys nothing, and a slow query with no
> output at all is indistinguishable from a hang. Requested upstream as
> [duckdb/duckdb#25199](https://github.com/duckdb/duckdb/issues/25199).

### When a query fails

Errors carry Athena's own explanation rather than only the execution id, so the
cause is in the terminal instead of the Athena console:

```
Invalid Input Error: Athena query 00000000-0000-0000-0000-000000000000 Failed:
INVALID_LITERAL: line 1:71: 'not-a-date' is not a valid TIMESTAMP literal
```

Lookup failures name the database, table and region, because a table in another
region reports the same "Entity Not Found" as one that does not exist:

```
Binder Error: table "duckdb_athena_demo"."no_such_table" in region us-east-1: EntityNotFoundException: Entity Not Found
```

`Run time` is Athena's engine time and excludes fetching the result.

## Projection pushdown

Column projection is pushed into Athena automatically. When your query only
references a subset of columns, only those columns are read from Athena — on
columnar formats (Parquet/ORC) this reduces `data_scanned_in_bytes` and cost:

```sql
-- Only "year" is selected from Athena, not every column in the table.
SELECT year FROM athena_scan('my_table');

-- COUNT(*) reads one placeholder column of every row; see "Count rows".
SELECT COUNT(*) FROM athena_scan('my_table');
```

## Limitations

- Complex types (`array`, `map`, `struct`) come back as JSON text, not native DuckDB `LIST`/`STRUCT`/`MAP`. They are selected as `CAST(col AS JSON)`, because Athena's default rendering is ambiguous — `array['a,b', 'c']` prints as `[a,b, c]`, where the comma inside the element cannot be told from the separator. Query them with DuckDB's json functions:

  ```sql
  SELECT json_extract_string(tags, '$[0]'), json_extract_string(info, '$.s')
  FROM athena_scan('events');
  ```

  A complex column whose nested types Athena cannot cast to JSON — `binary`/`varbinary`, `char`, `time`, or anything unrecognised — keeps the old plain-text rendering instead, since a rejected cast would fail the whole query. The decision is per column, so one such column does not affect the others.
- `predicate=` is checked at bind only for what Athena cannot catch: it must be a single expression, with no semicolons, SQL comments, or statement keywords (matched as tokens, so `(SELECT` and `SELECT\n` are caught too — a subquery would otherwise scan and bill a table you never named). Column names are **not** pre-validated — Athena rejects an unknown column itself in about half a second having scanned 0 bytes (so at no cost), and its reason is surfaced verbatim: `COLUMN_NOT_FOUND: line 1:30: Column 'nosuchcol' cannot be resolved`. Pre-parsing the expression here meant maintaining a partial SQL grammar, which rejected valid queries such as `EXTRACT(year FROM ts) = 2024` whenever a keyword was missing from it
- Automatic *filter* pushdown is not implemented — the DuckDB C loadable-extension API exposes projection pushdown but no table-filter callback, so `WHERE` clauses are evaluated in DuckDB after Athena returns rows. Use `predicate=` to push a filter into Athena manually.
- Returns all rows by default; pass `maxrows=N` to cap (an outer DuckDB `LIMIT` is not pushed to Athena)
- Results are streamed from the query's result CSV on S3 (one `GetObject`), which needs `s3:GetObject` on the results bucket. Workgroups using Athena-managed query results expose no S3 location, so those fall back to `GetQueryResults` paging at 1000 rows per call (~8 rows/ms)
- Workgroup defaults to `primary`; override with `workgroup=`
- A value Athena returns that does not parse as its declared type becomes NULL,
  never a default. Booleans accept only `true`/`false` (case-insensitive): a `"1"`
  or `"t"` is NULL rather than `false`, since a wrong boolean would join the false
  rows in every aggregate unnoticed while a NULL is excluded and visible
- Glue type strings are matched case-insensitively and with sizes stripped, so
  `Int`, `BIGINT ` and `varchar(256)` map to the types their lowercase spellings
  would. Types with no DuckDB counterpart still come back as `VARCHAR`

## License

MIT — see [LICENSE](LICENSE). This extension began as a fork of
[dacort/duckdb-athena-extension](https://github.com/dacort/duckdb-athena-extension)
by Damon P. Cortesi, whose copyright is retained alongside later work.
