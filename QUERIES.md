# Example Queries

Runnable `athena_scan` examples against real Glue databases in this account
(`us-east-1`). The table name is the only positional argument; everything else
is named. The examples below omit `output_location`, so the extension derives
the results S3 location from the workgroup's default (this account's `primary`
workgroup is configured with `s3://ebunt-athena/athena-results/`). Pass
`output_location='s3://...'` to override it, or `workgroup='name'` to use a
different workgroup's default.

Load the extension first (see README for the `-unsigned` flags):

```sql
LOAD 'build/release/extension/athena/athena.duckdb_extension';
```

Signature: `athena_scan('table', database=..., output_location=..., workgroup=..., maxrows=..., predicate=...)`.
`maxrows` is unlimited by default; pass `maxrows=N` to cap the Athena query with
`LIMIT N`. Filtering with a plain DuckDB `WHERE` happens *after* Athena returns
rows; `predicate=` pushes a filter into the Athena query itself.

```sql
-- explicit results location and workgroup
SELECT * FROM athena_scan('data', database='nyctaxi',
                          output_location='s3://my-bucket/athena/',
                          workgroup='analytics');
```

---

## nyctaxi.data — NYC green taxi trips

Basic scan (bounded — nyctaxi is large, and an unbounded `SELECT *` pages every
row back from Athena 1000 at a time; use `maxrows=N` to cap it at the Athena
side):

```sql
SELECT * FROM athena_scan('data', database='nyctaxi', maxrows=10);
```

Projection pushdown — only these two columns are read from Athena:

```sql
SELECT trip_distance, total_amount
FROM athena_scan('data', database='nyctaxi');
```

Count rows. DuckDB keeps one placeholder column for `COUNT(*)`, so Athena is
sent `SELECT "vendorid" FROM "nyctaxi"."data"` — but the result is streamed from
S3 in one request, so the full count returns in about 3 seconds:

```sql
-- 1,070,262 in ~3s
SELECT COUNT(*) FROM athena_scan('data', database='nyctaxi');

-- returns 1000, not nyctaxi's row count
SELECT COUNT(*) FROM athena_scan('data', database='nyctaxi', maxrows=1000);
```

Aggregate in DuckDB — average tip by payment type:

```sql
SELECT payment_type,
       COUNT(*)                  AS trips,
       ROUND(AVG(tip_amount), 2) AS avg_tip
FROM athena_scan('data', database='nyctaxi')
GROUP BY payment_type
ORDER BY trips DESC;
```

---

## flights.flights — US flight on-time data

Push the filter into Athena with `predicate=` (Athena evaluates it, not DuckDB):

```sql
SELECT carrier, origin, dest, dep_delay
FROM athena_scan(
  'flights',
  database='flights',
  predicate='dep_delay > 120'
);
```

Combine `predicate=` (Athena-side) with a DuckDB `WHERE` (local) — carriers with
the most 1-hour-plus departure delays in 2016:

```sql
SELECT carrier, COUNT(*) AS long_delays
FROM athena_scan(
  'flights',
  database='flights',
  predicate='year = 2016'
)
WHERE dep_delay > 60
GROUP BY carrier
ORDER BY long_delays DESC
LIMIT 10;
```

---

## covid-19 — public COVID dataset (note the hyphenated database name)

Filter to one state in Athena and return all matching rows:

```sql
SELECT date, county, state, cases, deaths
FROM athena_scan(
  'nytimes_counties',
  database='covid-19',
  predicate='state = ''New York'''
);
```

Join two `athena_scan` calls in DuckDB. `county_populations` stores the count in a
column literally named `population estimate 2018` (spaces), so it must be double
quoted, and it is a string, so cast it:

```sql
SELECT c.state,
       SUM(c.cases)                                                  AS total_cases,
       MAX(TRY_CAST(p."population estimate 2018" AS BIGINT))         AS pop_2018
FROM athena_scan('nytimes_counties', database='covid-19') c
JOIN athena_scan('county_populations', database='covid-19') p
  ON c.county = p.county AND c.state = p.state
GROUP BY c.state
ORDER BY total_cases DESC
LIMIT 10;
```

---

## tpcds.customer — wide table, projection matters

Only 3 of 18 columns are read from Athena:

```sql
SELECT c_first_name, c_last_name, c_birth_year
FROM athena_scan(
  'customer',
  database='tpcds',
  predicate='c_birth_year >= 1980'
);
```

---

## sampledb.elb_logs — classic ELB access logs

Response-code distribution:

```sql
SELECT elbresponsecode, COUNT(*) AS n
FROM athena_scan('elb_logs', database='sampledb')
GROUP BY elbresponsecode
ORDER BY n DESC;
```
