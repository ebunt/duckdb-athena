"""Run the QUERIES.md examples against Athena via the athena extension.

Usage (from repo root):
    uv run project/athena_examples.py --list
    uv run project/athena_examples.py taxi_count
    uv run project/athena_examples.py all

Requires:
    - The extension built at ../build/release/extension/athena/athena.duckdb_extension
      (run `make release` in the repo root), or set ATHENA_EXTENSION_PATH.
    - AWS credentials with Athena/Glue/S3 access and a region (AWS_REGION, or a
      profile in ~/.aws/config). The queries hit live Athena and cost money.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXT = (
    REPO_ROOT / "build" / "release" / "extension" / "athena" / "athena.duckdb_extension"
)

# Everything below reads the database `project/bootstrap.py` creates, so these
# examples run in any AWS account rather than only where the author's catalog
# happens to exist. Build it first:
#
#     uv run project/bootstrap.py create
#
# Optional explicit Athena results location. When unset, athena_scan uses the
# workgroup's default result configuration, so the queries only add
# `output_location=` when ATHENA_OUTPUT_LOCATION is provided. Single quotes are
# doubled to keep the embedded SQL string literal valid.
DB = os.environ.get("ATHENA_DEMO_DATABASE", "duckdb_athena_demo")

OUTPUT = os.environ.get("ATHENA_OUTPUT_LOCATION")
_OUT = f", output_location='{OUTPUT.replace(chr(39), chr(39) * 2)}'" if OUTPUT else ""

# Named examples, mirroring QUERIES.md. Keys are what you pass on the CLI.
QUERIES: dict[str, str] = {
    # maxrows caps the Athena query with LIMIT -- without it a bare SELECT *
    # returns every row. A plain DuckDB LIMIT would not help: there is no limit
    # pushdown, so Athena would still return everything.
    "basic": f"""
        SELECT * FROM athena_scan('trips'{_OUT}, database='{DB}', maxrows=10)
    """,
    # Only the referenced columns are selected from Athena. On Parquet that
    # directly reduces data_scanned_in_bytes, which is what Athena bills for.
    "projection": f"""
        SELECT trip_distance, total_amount
        FROM athena_scan('trips'{_OUT}, database='{DB}')
    """,
    # An unbounded count, back in seconds: the result is streamed from the
    # single CSV Athena writes to S3 rather than paged 1000 rows at a time.
    "count": f"""
        SELECT COUNT(*) AS trips
        FROM athena_scan('trips'{_OUT}, database='{DB}')
    """,
    "avg_tip": f"""
        SELECT payment_type,
               COUNT(*)                  AS trips,
               ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM athena_scan('trips'{_OUT}, database='{DB}')
        GROUP BY payment_type
        ORDER BY trips DESC
    """,
    # DECIMAL(10,2) maps to a native DuckDB DECIMAL, so this sums without a
    # cast and without float drift -- compare with SUM(total_amount), which is
    # the same money as a DOUBLE.
    "decimal": f"""
        SELECT ROUND(SUM(total_decimal), 2) AS exact_total,
               ROUND(SUM(total_amount), 2)  AS float_total
        FROM athena_scan('trips'{_OUT}, database='{DB}')
    """,
    # Booleans that do not parse become NULL rather than false, so a three-way
    # count is meaningful: true, false, and unknown.
    "booleans": f"""
        SELECT COUNT(*) FILTER (WHERE is_disputed)          AS disputed,
               COUNT(*) FILTER (WHERE NOT is_disputed)      AS not_disputed,
               COUNT(*) FILTER (WHERE is_disputed IS NULL)  AS unknown
        FROM athena_scan('trips'{_OUT}, database='{DB}')
    """,
    # TIMESTAMP arrives as a native DuckDB timestamp, so date functions work
    # without parsing strings.
    "by_hour": f"""
        SELECT EXTRACT(hour FROM pickup_datetime) AS hour_of_day,
               COUNT(*)                           AS trips
        FROM athena_scan('trips'{_OUT}, database='{DB}')
        GROUP BY 1
        ORDER BY trips DESC
        LIMIT 5
    """,
    # predicate= is evaluated by Athena, so it cuts what is scanned and
    # returned. A plain DuckDB WHERE cannot: the C extension API exposes no
    # filter pushdown (duckdb/duckdb#25163).
    "predicate": f"""
        SELECT vendorid, passenger_count, total_amount
        FROM athena_scan('trips'{_OUT}, database='{DB}',
                         predicate='passenger_count > 4')
    """,
    # Both filters at once: Athena narrows to one payment type, DuckDB applies
    # the rest locally once the rows arrive.
    "combined": f"""
        SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM athena_scan('trips'{_OUT}, database='{DB}',
                         predicate='payment_type = 1')
        WHERE tip_amount > 5
        GROUP BY payment_type
    """,
    # Glue keeps partition keys out of the column list and Athena returns them
    # last. A predicate on one prunes instead of scanning: this reads one month
    # of the six.
    "partitioned": f"""
        SELECT yr, mn, COUNT(*) AS trips
        FROM athena_scan('trips_by_month'{_OUT}, database='{DB}',
                         predicate='yr = 2024 AND mn = 2')
        GROUP BY yr, mn
    """,
    # Two scans joined inside DuckDB: each runs its own Athena query, and the
    # join happens locally.
    "join": f"""
        SELECT m.mn, COUNT(*) AS trips, ROUND(AVG(t.total_amount), 2) AS avg_fare
        FROM athena_scan('trips'{_OUT}, database='{DB}',
                         predicate='passenger_count = 1') t
        JOIN athena_scan('trips_by_month'{_OUT}, database='{DB}',
                         predicate='yr = 2024 AND mn = 1') m
          ON t.pickup_datetime = m.pickup_datetime
        GROUP BY m.mn
    """,
    # Run this twice: the second run is served from Athena's result cache,
    # scanning 0 bytes and costing nothing. Only safe while the data underneath
    # is not changing.
    "reuse": f"""
        SELECT COUNT(*) AS trips
        FROM athena_scan('trips'{_OUT}, database='{DB}', result_reuse_minutes=60)
    """,
    # region= overrides whatever the AWS config chain resolved, so one session
    # can read tables in more than one region. profile= does the same for
    # credentials.
    "region": f"""
        SELECT COUNT(*) AS trips
        FROM athena_scan('trips'{_OUT}, database='{DB}', region='us-east-1')
    """,
}


def connect(ext_path: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB and load the locally built Athena extension."""
    if not ext_path.exists():
        sys.exit(
            f"Extension not found at {ext_path}\n"
            "Build it with `make release` in the repo root, or set ATHENA_EXTENSION_PATH."
        )
    # Locally compiled extensions carry no release signature, hence
    # allow_unsigned_extensions. Deliberately not setting
    # allow_extensions_metadata_mismatch: a build stamped for this platform does
    # not need it, and leaving it off means a wrong-platform binary fails loudly
    # instead of being loaded anyway.
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext_path.as_posix()}';")
    return con


def run(con: duckdb.DuckDBPyConnection, name: str) -> float:
    """Run one example, print its result, and return wall-clock seconds."""
    sql = QUERIES[name].strip()
    print(f"\n=== {name} ===")
    print(sql)
    # .show() uses DuckDB's native pretty-printer (no pandas) and previews large
    # result sets rather than dumping every row. It also forces execution, so
    # timing around it captures the full Athena round-trip.
    start = time.perf_counter()
    con.sql(sql).show()
    elapsed = time.perf_counter() - start
    print(f"elapsed: {elapsed:.2f}s")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "query",
        nargs="?",
        help="example name to run, or 'all' for every example",
    )
    parser.add_argument(
        "--list", action="store_true", help="list example names and exit"
    )
    parser.add_argument(
        "--ext",
        type=Path,
        default=Path(os.environ.get("ATHENA_EXTENSION_PATH", DEFAULT_EXT)),
        help="path to the .duckdb_extension file",
    )
    args = parser.parse_args()

    if args.list or not args.query:
        print("Available examples (pass one, or 'all'):")
        for name in QUERIES:
            print(f"  {name}")
        return

    if args.query != "all" and args.query not in QUERIES:
        sys.exit(f"Unknown example '{args.query}'. Use --list to see options.")

    if not (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")):
        print(
            "warning: AWS_REGION not set; relying on your ~/.aws/config default region.",
            file=sys.stderr,
        )

    con = connect(args.ext)
    names = list(QUERIES) if args.query == "all" else [args.query]
    total = sum(run(con, name) for name in names)
    if len(names) > 1:
        print(f"\ntotal: {total:.2f}s across {len(names)} queries")


if __name__ == "__main__":
    main()
