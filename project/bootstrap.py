"""Create the Glue database the examples and live checks run against.

Every example in QUERIES.md, every query in athena_examples.py, and most of
scripts/live-check.sh read from one database built by this script, so anyone
with an AWS account can reproduce them. Nothing here depends on tables that
happen to exist in the author's catalog.

The data is New York City green taxi trips, published by the NYC Taxi and
Limousine Commission and fetched from their CDN. Three months is about 170k
rows and 4 MB -- small enough that a full scan costs a fraction of a cent, big
enough that streaming and partition pruning are measurable.

Usage (from the repo root):
    uv run project/bootstrap.py create     # build it
    uv run project/bootstrap.py verify     # count rows in each table
    uv run project/bootstrap.py drop       # remove tables, database, and data

Where it puts things: the `primary` workgroup's own query-result bucket, under
a `duckdb-athena-demo/` prefix, so no new bucket is needed and nothing is
written outside a location Athena already writes to. Override with
ATHENA_DEMO_LOCATION=s3://bucket/prefix/.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import duckdb

DATABASE = os.environ.get("ATHENA_DEMO_DATABASE", "duckdb_athena_demo")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
MONTHS = ["2024-01", "2024-02", "2024-03"]
SOURCE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{month}.parquet"
)

# Lower-cased and trimmed to the columns the examples actually use. The source
# parquet spells them `VendorID`, `PULocationID` and so on; Glue lower-cases
# every column name, so they are normalised here rather than relying on Athena
# to match a name it has already changed.
#
# total_decimal exists to give the examples a real DECIMAL column: it is the one
# type with a two-part registration (width and scale) and a hand-written value
# path, so it has the most surface to get wrong, and a wrong scale is silent.
COLUMNS = [
    ("vendorid", "INT"),
    ("pickup_datetime", "TIMESTAMP"),
    ("dropoff_datetime", "TIMESTAMP"),
    ("store_and_fwd_flag", "STRING"),
    ("pickup_location_id", "INT"),
    ("dropoff_location_id", "INT"),
    ("passenger_count", "BIGINT"),
    ("trip_distance", "DOUBLE"),
    ("fare_amount", "DOUBLE"),
    ("tip_amount", "DOUBLE"),
    ("tolls_amount", "DOUBLE"),
    ("total_amount", "DOUBLE"),
    ("total_decimal", "DECIMAL(10,2)"),
    ("payment_type", "BIGINT"),
    ("is_disputed", "BOOLEAN"),
]

SELECT_LIST = """
    CAST("VendorID" AS INTEGER)                      AS vendorid,
    lpep_pickup_datetime                             AS pickup_datetime,
    lpep_dropoff_datetime                            AS dropoff_datetime,
    store_and_fwd_flag,
    CAST("PULocationID" AS INTEGER)                  AS pickup_location_id,
    CAST("DOLocationID" AS INTEGER)                  AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    CAST(total_amount AS DECIMAL(10,2))              AS total_decimal,
    payment_type,
    payment_type = 4                                 AS is_disputed,
    CAST(year(lpep_pickup_datetime) AS INTEGER)      AS yr,
    CAST(month(lpep_pickup_datetime) AS INTEGER)     AS mn
"""


def location() -> tuple[str, str | None]:
    """Where the demo data goes, and a result location if Athena needs one told.

    Returns (prefix, result_config). `result_config` is None when the workgroup
    already has its own result configuration, because passing one anyway
    conflicts with a workgroup that enforces its settings.
    """
    if explicit := os.environ.get("ATHENA_DEMO_LOCATION"):
        prefix = explicit.rstrip("/") + "/"
        # The workgroup may still have no result location of its own, in which
        # case every statement below needs one or Athena rejects it outright.
        return prefix, None if workgroup_output() else f"{prefix}athena-results/"

    output = workgroup_output()
    if not output:
        sys.exit(
            f"workgroup {WORKGROUP!r} has no result location, so there is nowhere "
            "obvious to put the demo data. Set ATHENA_DEMO_LOCATION=s3://bucket/prefix/"
        )
    # Append to the whole configured prefix rather than keeping only the bucket.
    # Accounts commonly grant Athena users access to the result prefix and not
    # to the bucket root, so `s3://bucket/athena-results/` must become
    # `s3://bucket/athena-results/duckdb-athena-demo/`, not a sibling of it.
    return output.rstrip("/") + "/duckdb-athena-demo/", None


def workgroup_output() -> str | None:
    wg = boto3.client("athena").get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]
    return (
        wg.get("Configuration", {})
        .get("ResultConfiguration", {})
        .get("OutputLocation")
    )


def athena(sql: str, *, database: str | None = None, results: str | None = None) -> str:
    """Run one statement and wait. Returns the execution id."""
    client = boto3.client("athena")
    kwargs: dict = {"QueryString": sql, "WorkGroup": WORKGROUP}
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    if results:
        kwargs["ResultConfiguration"] = {"OutputLocation": results}
    qid = client.start_query_execution(**kwargs)["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]
        state = status["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        sys.exit(
            f"Athena {state}: {status.get('StateChangeReason', 'no reason given')}\n  {sql}"
        )
    return qid


RESULTS: str | None = None  # set by create()/verify()/drop() from location()


def scalar(sql: str) -> str:
    """Run a query and return its single value."""
    client = boto3.client("athena")
    qid = athena(sql, database=DATABASE, results=RESULTS)
    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return rows[1]["Data"][0].get("VarCharValue", "")


def build_local(out: Path) -> int:
    """Fetch the source months and write partitioned parquet under `out`."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    urls = ", ".join(f"'{SOURCE.format(month=m)}'" for m in MONTHS)
    con.execute(f"""
        CREATE VIEW trips AS SELECT {SELECT_LIST} FROM read_parquet([{urls}])
    """)
    rows = con.execute("SELECT COUNT(*) FROM trips").fetchone()[0]

    # Unpartitioned copy: what most examples read. Written inside a directory
    # rather than as a bare file -- Athena's LOCATION must be a prefix, and
    # pointing it at a single object fails with "Cannot create table on file".
    (out / "trips").mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * EXCLUDE (yr, mn) FROM trips)
        TO '{out / "trips" / "trips.parquet"}' (FORMAT PARQUET)
    """)
    # Partitioned copy: Glue keeps partition keys out of the column list and
    # Athena returns them last, which the examples and live checks assert on.
    con.execute(f"""
        COPY (SELECT * FROM trips)
        TO '{out / "trips_by_month"}' (FORMAT PARQUET, PARTITION_BY (yr, mn))
    """)
    return rows


def upload(local: Path, prefix: str) -> int:
    bucket = prefix.split("/")[2]
    key_root = "/".join(prefix.split("/")[3:]).rstrip("/")
    s3 = boto3.client("s3")
    count = 0
    for path in sorted(local.rglob("*")):
        if path.is_file():
            key = f"{key_root}/{path.relative_to(local).as_posix()}"
            s3.upload_file(str(path), bucket, key)
            count += 1
    return count


def ddl(prefix: str) -> None:
    columns = ",\n  ".join(f"`{name}` {type_}" for name, type_ in COLUMNS)
    athena(f"CREATE DATABASE IF NOT EXISTS {DATABASE}", results=RESULTS)
    athena(f"DROP TABLE IF EXISTS {DATABASE}.trips", results=RESULTS)
    athena(f"DROP TABLE IF EXISTS {DATABASE}.trips_by_month", results=RESULTS)
    athena(f"""
        CREATE EXTERNAL TABLE {DATABASE}.trips (
          {columns}
        )
        STORED AS PARQUET
        LOCATION '{prefix}trips/'
    """, results=RESULTS)
    athena(f"""
        CREATE EXTERNAL TABLE {DATABASE}.trips_by_month (
          {columns}
        )
        PARTITIONED BY (`yr` INT, `mn` INT)
        STORED AS PARQUET
        LOCATION '{prefix}trips_by_month/'
    """, results=RESULTS)
    # Without this the partitioned table reports zero rows: the data is on S3
    # but Glue has no partitions registered for it.
    athena(f"MSCK REPAIR TABLE {DATABASE}.trips_by_month", results=RESULTS)


def create() -> None:
    global RESULTS
    prefix, RESULTS = location()
    print(f"database : {DATABASE}")
    print(f"location : {prefix}")
    with TemporaryDirectory() as tmp:
        out = Path(tmp)
        print(f"fetching : {len(MONTHS)} months of green taxi data from the TLC CDN")
        rows = build_local(out)
        print(f"           {rows:,} rows")
        files = upload(out, prefix)
        print(f"uploading: {files} parquet files")
    print("creating : database, trips, trips_by_month")
    ddl(prefix)
    verify()
    print("\nRun the examples with:  uv run project/athena_examples.py all")


def verify() -> None:
    global RESULTS
    if RESULTS is None:
        _, RESULTS = location()
    print("\n-- verify")
    for table in ("trips", "trips_by_month"):
        print(f"  {table:15} {int(scalar(f'SELECT COUNT(*) FROM {table}')):>9,} rows")
    partitions = scalar(
        "SELECT COUNT(*) FROM (SELECT DISTINCT yr, mn FROM trips_by_month)"
    )
    print(f"  {'partitions':15} {partitions:>9}")
    print(f"  {'decimal check':15} {scalar('SELECT SUM(total_decimal) FROM trips'):>9}")


def drop() -> None:
    global RESULTS
    prefix, RESULTS = location()
    athena(f"DROP TABLE IF EXISTS {DATABASE}.trips", results=RESULTS)
    athena(f"DROP TABLE IF EXISTS {DATABASE}.trips_by_month", results=RESULTS)
    athena(f"DROP DATABASE IF EXISTS {DATABASE}", results=RESULTS)

    # Only the two trees this script writes. Deleting everything under the
    # prefix would take unrelated objects with it whenever ATHENA_DEMO_LOCATION
    # points somewhere that already holds data -- which the documented override
    # invites.
    bucket = prefix.split("/")[2]
    key_root = "/".join(prefix.split("/")[3:])
    s3 = boto3.client("s3")
    deleted = 0
    for table in ("trips", "trips_by_month"):
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{key_root}{table}/"
        )
        for page in pages:
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
    print(f"dropped {DATABASE} and deleted {deleted} objects under {prefix}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "verify", "drop"])
    args = parser.parse_args()

    if not (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")):
        print(
            "warning: AWS_REGION not set; using your ~/.aws/config default.",
            file=sys.stderr,
        )

    {"create": create, "verify": verify, "drop": drop}[args.action]()


if __name__ == "__main__":
    main()
