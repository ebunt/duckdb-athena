# Athena Extension — Python Examples

Runs the [QUERIES.md](../QUERIES.md) examples against Athena through the
`athena` loadable extension, using the DuckDB Python library.

## Prerequisites

- The extension built at `../build/release/extension/athena/athena.duckdb_extension`
  (run `make` in the repo root).
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

Examples without `maxrows` (`taxi_projection`, `taxi_avg_tip`) make Athena
execute over the whole table and write the full result set to S3 before the
first row is available. That is the slow part, and only `maxrows` reduces it —
it is the one knob that changes the query Athena actually runs.

How much then crosses the network depends on the query. Result pages are
fetched lazily, so a consumer that stops early stops the download: `.show()`
previews a few thousand rows, so `taxi_projection` only pulls the first few
pages. An aggregate has no such shortcut — `taxi_avg_tip` must pull every row
to group it, as would an unbounded `COUNT(*)`. A DuckDB `LIMIT` can likewise
cut the download short, but cannot shrink the query sent to Athena.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `ATHENA_OUTPUT_LOCATION` | (workgroup default) | Optional Athena results S3 path; when unset, derived from the workgroup default |
| `ATHENA_EXTENSION_PATH` | `../build/release/extension/athena/athena.duckdb_extension` | built extension to load |
| `AWS_REGION` | (from `~/.aws/config`) | AWS region for Athena/Glue |
