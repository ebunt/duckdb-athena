# Athena Extension — Python Examples

Runs the [QUERIES.md](../QUERIES.md) examples against Athena through the
`athena` loadable extension, using the DuckDB Python library.

## Prerequisites

- The extension built at `../build/release/extension/athena/athena.duckdb_extension`
  (run `make configure && make release` in the repo root).
- AWS credentials with Athena, Glue, and S3 access, and a region set via
  `AWS_REGION` or an `~/.aws/config` profile.

## Run

```bash
# from the repo root -- uv resolves the script against project/pyproject.toml,
# so no --project or explicit python is needed
uv run project/athena_examples.py --list      # list example names
uv run project/athena_examples.py taxi_count  # run one example
uv run project/athena_examples.py all         # run every example

# or from inside project/
cd project
uv run athena_examples.py taxi_projection
```

The queries hit live Athena and scan real data (they cost money). `taxi_basic`
is the cheapest smoke test: `maxrows=10` becomes `LIMIT 10` in the Athena query,
so it returns immediately.

Examples without `maxrows` make Athena execute over the whole table and write
the full result to S3 before the first row is available.

Three things change the query Athena actually runs, and so reduce what is
scanned and billed: the columns you select (pushed into the `SELECT` list
automatically — `taxi_projection` sends two columns, not eighteen, which on
Parquet and ORC means fewer bytes read), `predicate=`, and `maxrows`. A DuckDB
`WHERE` or `LIMIT` does not: those filter after the rows arrive.

Fetching that result is no longer the bottleneck it once was: it is streamed
from the single CSV Athena writes, in one request, so `taxi_count` returns the
true 1,070,262 in about 3 seconds. It used to be paged back 1000 rows at a
time, which took roughly 135 seconds. Consumers that stop early still stop the
download; aggregates like `taxi_avg_tip` have no such shortcut and read every
row.

`elb_reuse` is worth running twice. The second run is served from Athena's
result cache — 0 bytes scanned, nothing billed — which `result_reuse_minutes=`
opts into.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `ATHENA_OUTPUT_LOCATION` | (workgroup default) | Optional Athena results S3 path; when unset, derived from the workgroup default |
| `ATHENA_EXTENSION_PATH` | `../build/release/extension/athena/athena.duckdb_extension` | built extension to load |
| `AWS_REGION` | (from `~/.aws/config`) | AWS region for Athena/Glue; `region=` on a scan overrides it, as `elb_region` shows |
| `ATHENA_PARTITIONED_TABLE` | `default.claude_part_test` | `db.table` fixture for the `partitioned` example |
| `ATHENA_PARTITIONED_PREDICATE` | `yr = 2024` for the default fixture, otherwise none | pruning predicate for that example; pointing at your own table without setting this just omits it, since partition keys differ |
