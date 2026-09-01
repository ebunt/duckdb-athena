# Athena Extension — Python Examples

Runs the [QUERIES.md](../QUERIES.md) examples against Athena through the
`athena` loadable extension, using the DuckDB Python library.

## Prerequisites

- The extension built at `../build/release/extension/athena/athena.duckdb_extension`
  (run `make configure && make release` in the repo root).
- AWS credentials with Athena, Glue, and S3 access, and a region set via
  `AWS_REGION` or an `~/.aws/config` profile.

## Build the demo database first

Every example reads one database you create yourself, so nothing here depends on
tables that happen to exist in someone else's catalog:

```bash
uv run project/bootstrap.py create
```

That fetches three months of NYC green taxi trips (167,585 rows, ~4 MB) from the
TLC's CDN, writes them to your Athena workgroup's own result bucket under a
`duckdb-athena-demo/` prefix, and creates `duckdb_athena_demo.trips` plus a
partitioned `duckdb_athena_demo.trips_by_month`.

```bash
uv run project/bootstrap.py verify   # row counts, partitions, decimal total
uv run project/bootstrap.py drop     # tables, database, and the S3 objects
```

It needs no new bucket: it writes under the workgroup's own result prefix, so
`s3://bucket/athena-results/` becomes
`s3://bucket/athena-results/duckdb-athena-demo/` — an account that grants Athena
users the result prefix and nothing else still works. Override with
`ATHENA_DEMO_LOCATION=s3://bucket/prefix/`, or rename the database with
`ATHENA_DEMO_DATABASE`.

`drop` deletes only the `trips/` and `trips_by_month/` trees it created, not
everything under the prefix — pointing `ATHENA_DEMO_LOCATION` at somewhere that
already holds data is safe.

The tables are deliberately shaped to exercise the extension rather than just to
hold data: `total_decimal` is a real `DECIMAL(10,2)`, `is_disputed` a `BOOLEAN`,
the pickup and dropoff columns are `TIMESTAMP`, and `trips_by_month` is
partitioned so pruning is measurable.

## Run

```bash
# from the repo root -- uv resolves the script against project/pyproject.toml,
# so no --project or explicit python is needed
uv run project/athena_examples.py --list  # list example names
uv run project/athena_examples.py count   # run one example
uv run project/athena_examples.py all     # run every example

# or from inside project/
cd project
uv run athena_examples.py projection
```

The queries hit live Athena and scan real data. On this dataset that is
fractions of a cent — a full scan of `trips` reads 3.82 MB — but it is real
money and a real query each time. `basic` is the cheapest smoke test:
`maxrows=10` becomes `LIMIT 10`, so Athena returns immediately.

Three things change the query Athena actually runs, and so reduce what is
scanned and billed: the columns you select (pushed into the `SELECT` list
automatically — `projection` sends two columns, not fifteen, and reads 258 KB
instead of 3.82 MB), `predicate=`, and `maxrows`. A DuckDB `WHERE` or `LIMIT`
does not: those filter after the rows arrive.

Fetching the result is not the bottleneck it once was: it is streamed from the
single CSV Athena writes, in one request, so `count` returns all 167,585 rows in
about two seconds. It used to be paged back 1000 rows at a time. Aggregates like
`avg_tip` read every row; consumers that stop early stop the download.

`reuse` is worth running twice. The second run is served from Athena's result
cache — 0 bytes scanned, nothing billed — which `result_reuse_minutes=` opts
into.

`decimal`, `booleans` and `by_hour` exist to show the type mapping: a native
`DECIMAL(10,2)` that sums without drift, a `BOOLEAN` whose unparseable values
are NULL rather than false, and a `TIMESTAMP` that takes date functions
directly.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `ATHENA_DEMO_DATABASE` | `duckdb_athena_demo` | database the bootstrap creates and the examples read |
| `ATHENA_DEMO_LOCATION` | (the workgroup's result bucket, `duckdb-athena-demo/` prefix) | where the bootstrap writes the demo data |
| `ATHENA_OUTPUT_LOCATION` | (workgroup default) | optional Athena results S3 path; when unset, the workgroup's own is used |
| `ATHENA_EXTENSION_PATH` | `../build/release/extension/athena/athena.duckdb_extension` | built extension to load |
| `ATHENA_WORKGROUP` | `primary` | workgroup the bootstrap reads its result location from |
| `AWS_REGION` | (from `~/.aws/config`) | AWS region for Athena/Glue; `region=` on a scan overrides it, as the `region` example shows |
