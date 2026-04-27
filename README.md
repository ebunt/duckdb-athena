# DuckDB Athena Extension

Work-in-progress DuckDB extension for querying Amazon Athena tables with `athena_scan`.

## Requirements

- Rust stable
- DuckDB CLI v1.5+
- AWS credentials with Athena, Glue, and S3 access

Required IAM actions:
- `athena:StartQueryExecution`
- `athena:GetQueryExecution`
- `athena:GetQueryResults`
- `glue:GetTable`
- `s3:PutObject`
- `s3:GetObject`

## Build

```bash
make
```

This builds `target/release/duckdb_athena.duckdb_extension` using the Rust packager in `src/bin/package_extension.rs`.

## Load

```bash
duckdb -unsigned -cmd "LOAD 'target/release/duckdb_athena.duckdb_extension';"
```

## Usage

```sql
SELECT *
FROM athena_scan('my_table', 's3://my-results-bucket/prefix/');
```

Options:
- `database`: Glue database, default `default`.
- `workgroup`: Athena workgroup, default `primary`.
- `maxrows`: Athena `LIMIT`, default `10000`; pass `-1` to disable.
- `predicate`: trusted raw Athena SQL `WHERE` expression for manual pushdown.
- `verbose`: print Athena status and scan stats, default `false`.

Examples:

```sql
SELECT *
FROM athena_scan(
  'my_table',
  's3://my-results-bucket/prefix/',
  database='analytics',
  workgroup='analytics',
  predicate='year = 2024',
  maxrows=1000
);
```

```sql
SELECT COUNT(*)
FROM athena_scan('my_table', 's3://my-results-bucket/prefix/');
```

DuckDB projection pushdown is enabled, so selecting fewer columns submits a narrower Athena `SELECT` list. Normal DuckDB `WHERE` clauses are still evaluated locally unless passed through `predicate`.

## Tests

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

AWS integration tests are opt-in:

```bash
make
export ATHENA_TEST_DATABASE=my_database
export ATHENA_TEST_TABLE=my_table
export ATHENA_TEST_OUTPUT=s3://my-results-bucket/prefix/
cargo test --test aws_integration
```

Optional integration variables:
- `ATHENA_TEST_WORKGROUP`
- `ATHENA_TEST_PROJECTION_COLUMN`
- `ATHENA_TEST_PREDICATE`
- `ATHENA_TEST_PAGINATION_ROWS`
- `ATHENA_EXTENSION_PATH`

## Limitations

- Complex Athena types such as `array`, `map`, and `struct` are not supported.
- `predicate` is raw Athena SQL for trusted callers, not a full SQL parser.
- Automatic DuckDB filter pushdown is not implemented.
