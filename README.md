# DuckDB Athena Extension

> **Work in progress** — things may not work as expected

Query Amazon Athena tables directly from DuckDB using `athena_scan`.

## Prerequisites

- Rust stable toolchain (`rustup install stable`)
- DuckDB CLI v1.5+
- AWS credentials with access to Athena, Glue, and S3

## Build

```bash
make
```

This compiles the extension and places it at `target/release/duckdb_athena.duckdb_extension`.

## AWS credentials

Set standard AWS environment variables, or use any credential source the AWS SDK supports (instance profile, SSO, `~/.aws/credentials`):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

Required IAM permissions:
- `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
- `glue:GetTable` on the target table
- `s3:PutObject`, `s3:GetObject` on the S3 results bucket

## Load

Start DuckDB with the `-unsigned` flag (required because the extension is locally compiled without a release signature):

```bash
duckdb -unsigned
```

Then load the extension:

```sql
LOAD 'target/release/duckdb_athena.duckdb_extension';
```

-OR-

In one statement:

```bash
duckdb -unsigned -cmd "LOAD 'target/release/duckdb_athena.duckdb_extension';"
```

## Usage

### Basic scan

Provide the Glue table name and an S3 output location for Athena results:

```sql
SELECT * FROM athena_scan('my_table', 's3://my-results-bucket/prefix/');
```

### Specify a Glue database

Defaults to the `default` database. Pass `database=` to override:

```sql
SELECT * FROM athena_scan('my_table', 's3://my-results-bucket/prefix/', database='my_database');
```

### Return all rows

The default limit is 10,000 rows. Pass `maxrows=-1` to remove it:

```sql
SELECT * FROM athena_scan('my_table', 's3://my-results-bucket/prefix/', maxrows=-1);
```

### Filter results

DuckDB `WHERE` clauses are not pushed down automatically yet. For the MVP, pass an Athena SQL predicate with `predicate=` to add a `WHERE` clause to the query submitted to Athena:

```sql
SELECT *
FROM athena_scan(
  'my_table',
  's3://my-results-bucket/prefix/',
  database='my_database',
  predicate='year = 2024'
);
```

You can still use a normal DuckDB `WHERE` clause for local filtering after Athena returns results:

```sql
SELECT *
FROM athena_scan('my_table', 's3://my-results-bucket/prefix/', predicate='year = 2024')
WHERE event_type = 'click';
```

### Count rows

```sql
SELECT COUNT(*) FROM athena_scan('my_table', 's3://my-results-bucket/prefix/');
```

Query progress is printed to the console:

```
Running Athena query, execution id: 152a20c7-ff32-4a19-bb71-ae0135373ca6
State: Running, sleeping 5 secs...
Total execution time: 1307 millis
```

## Limitations

- Not all Athena data types are supported (complex types: array, map, struct)
- Automatic DuckDB filter pushdown is not implemented; use `predicate=` for manual Athena-side filtering
- Defaults to 10,000 rows (`maxrows=-1` to disable)
- Workgroup is hardcoded to `primary`
