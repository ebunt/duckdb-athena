# Runnable examples

Every query here runs against one database you build yourself, so the numbers
below are reproducible in any AWS account rather than only where the author's
catalog happens to exist.

```bash
uv run project/bootstrap.py create
```

That fetches three months of New York City green taxi trips from the TLC's CDN,
writes them to your Athena workgroup's own result bucket under a
`duckdb-athena-demo/` prefix, and creates a Glue database with two tables:

| table | rows | notes |
| --- | --- | --- |
| `trips` | 167,585 | flat, 3.82 MB on S3 |
| `trips_by_month` | 167,585 | same rows, partitioned by `yr`, `mn` (6 partitions) |

`uv run project/bootstrap.py drop` removes the tables, the database, and the S3
objects. Everything below assumes `database='duckdb_athena_demo'`; the
[Python runner](project/README.md) has the same queries as named examples.

The examples omit `output_location`, so the extension uses the workgroup's
default result configuration (the `primary` workgroup needs one, e.g.
`s3://<your-bucket>/athena-results/`). Pass `output_location='s3://...'` to
override it, or `workgroup='name'` to use a different workgroup's default.

## Basic scan

```sql
SELECT * FROM athena_scan('trips', database='duckdb_athena_demo', maxrows=10);
```

`maxrows` adds `LIMIT` to the Athena query. Without it a bare `SELECT *` returns
every row — an outer DuckDB `LIMIT` would not help, because there is no limit
pushdown, so Athena would still return everything and DuckDB would trim it.

## Projection pushdown

Only the columns your query mentions are selected from Athena, which on Parquet
is the difference between reading a file and reading a column of it:

```sql
SELECT trip_distance, total_amount
FROM athena_scan('trips', database='duckdb_athena_demo');
```

Measured on this dataset — Athena's own `data_scanned_in_bytes`:

| query | scanned |
| --- | --- |
| `SELECT *` | 3.82 MB |
| one column | 258 KB |

That is what Athena bills for, so projection is a cost control, not just a
speed one.

## Counting rows

```sql
SELECT COUNT(*) FROM athena_scan('trips', database='duckdb_athena_demo');
```

`COUNT(*)` is not special-cased — DuckDB keeps one placeholder column for it, so
the scan reads one column of every row. The result is streamed from the single
CSV Athena writes to S3 in one `GetObject`, rather than paged 1000 rows at a
time, which is why an unbounded count finishes in seconds rather than minutes.

## Types

`DECIMAL(p,s)`, `TIMESTAMP`, `DATE` and `BOOLEAN` map to native DuckDB types, so
they need no casting and no string parsing:

```sql
-- exact money, no float drift: total_decimal is DECIMAL(10,2)
SELECT ROUND(SUM(total_decimal), 2) AS exact_total,
       ROUND(SUM(total_amount), 2)  AS float_total
FROM athena_scan('trips', database='duckdb_athena_demo');

-- a real TIMESTAMP, so date functions work directly
SELECT EXTRACT(hour FROM pickup_datetime) AS hour_of_day, COUNT(*) AS trips
FROM athena_scan('trips', database='duckdb_athena_demo')
GROUP BY 1 ORDER BY trips DESC LIMIT 5;

-- a value that does not parse as its declared type becomes NULL, never a
-- default, so the unknown case is countable rather than hidden among the false
SELECT COUNT(*) FILTER (WHERE is_disputed)         AS disputed,
       COUNT(*) FILTER (WHERE NOT is_disputed)     AS not_disputed,
       COUNT(*) FILTER (WHERE is_disputed IS NULL) AS unknown
FROM athena_scan('trips', database='duckdb_athena_demo');
```

Complex types (`array`, `map`, `struct`) come back as JSON text — see
[Limitations](README.md#limitations).

## Filtering in Athena with `predicate=`

A DuckDB `WHERE` runs *after* Athena has returned every row. `predicate=` is
evaluated by Athena, so it cuts what is scanned, returned, and billed:

```sql
SELECT vendorid, passenger_count, total_amount
FROM athena_scan('trips', database='duckdb_athena_demo',
                 predicate='passenger_count > 4');
```

Both together — Athena narrows, DuckDB finishes the job locally:

```sql
SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(tip_amount), 2) AS avg_tip
FROM athena_scan('trips', database='duckdb_athena_demo',
                 predicate='payment_type = 1')
WHERE tip_amount > 5
GROUP BY payment_type;
```

`predicate=` is a raw Athena expression. Column names are not pre-validated:
Athena rejects an unknown column itself in about half a second having scanned
nothing, and its reason is surfaced verbatim —
`COLUMN_NOT_FOUND: Column 'nosuchcol' cannot be resolved`.

## Partitioned tables

Glue keeps partition keys out of the column list and Athena returns them last,
so `yr` and `mn` appear after the data columns. A predicate on one prunes:

```sql
SELECT yr, mn, COUNT(*) AS trips
FROM athena_scan('trips_by_month', database='duckdb_athena_demo',
                 predicate='yr = 2024 AND mn = 2')
GROUP BY yr, mn;
```

| query | scanned |
| --- | --- |
| all six partitions | 3.85 MB |
| one partition | 1.23 MB |

Pruning happens in Athena, before any data reaches DuckDB, so a DuckDB `WHERE
yr = 2024` would read all six and discard five.

## Joining two scans

Each `athena_scan` runs its own Athena query; the join happens in DuckDB:

```sql
SELECT m.mn, COUNT(*) AS trips, ROUND(AVG(t.total_amount), 2) AS avg_fare
FROM athena_scan('trips', database='duckdb_athena_demo',
                 predicate='passenger_count = 1') t
JOIN athena_scan('trips_by_month', database='duckdb_athena_demo',
                 predicate='yr = 2024 AND mn = 1') m
  ON t.pickup_datetime = m.pickup_datetime
GROUP BY m.mn;
```

Push what you can into each side with `predicate=` — everything else crosses the
wire.

## Reusing a result

Athena can return a previous identical query's result instead of re-running it,
which scans nothing and therefore costs nothing:

```sql
SELECT COUNT(*) FROM athena_scan('trips', database='duckdb_athena_demo',
                                 result_reuse_minutes=60);
```

Leave it off when the data underneath changes within the window.

## Credentials and region

```sql
-- read a table in another region without restarting DuckDB
SELECT COUNT(*) FROM athena_scan('trips', database='duckdb_athena_demo',
                                 region='us-east-1');

-- use a named profile from ~/.aws/config for this scan only
SELECT COUNT(*) FROM athena_scan('trips', database='duckdb_athena_demo',
                                 profile='prod');
```

`region=` beats the profile's own region, which beats `AWS_REGION`. A profile
that does not exist fails at bind, naming itself.

## Bounding a slow query

```sql
SELECT * FROM athena_scan('trips', database='duckdb_athena_demo',
                          timeout_seconds=120);
```

On expiry the extension stops the Athena query rather than leaving it running —
which needs `athena:StopQueryExecution`. Note that Ctrl-C does **not** stop it;
see the caveat in [README.md](README.md#bound-how-long-a-query-may-run).

## Quoting

Identifiers reach Athena quoted, so hyphens, reserved words, and spaces in
database or column names survive:

```sql
SELECT * FROM athena_scan('trips', database='duckdb_athena_demo', maxrows=1);
-- sends: SELECT ... FROM "duckdb_athena_demo"."trips" LIMIT 1
```

A column whose name needs quoting in DuckDB is quoted the same way:

```sql
SELECT "total_amount" FROM athena_scan('trips', database='duckdb_athena_demo');
```
