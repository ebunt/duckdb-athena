"""Run the QUERIES.md examples against Athena via the duckdb_athena extension.

Usage (from repo root):
    uv run --project project python project/athena_examples.py --list
    uv run --project project python project/athena_examples.py taxi_count
    uv run --project project python project/athena_examples.py all

Requires:
    - The extension built at ../target/release/duckdb_athena.duckdb_extension
      (run `make` in the repo root), or set ATHENA_EXTENSION_PATH.
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
DEFAULT_EXT = REPO_ROOT / "target" / "release" / "duckdb_athena.duckdb_extension"

# Optional explicit Athena results location. When unset, athena_scan uses the
# workgroup's default result configuration, so the queries below only add
# `output_location=` when ATHENA_OUTPUT_LOCATION is provided. Single quotes are
# doubled to keep the embedded SQL string literal valid.
OUTPUT = os.environ.get("ATHENA_OUTPUT_LOCATION")
_OUT = f", output_location='{OUTPUT.replace(chr(39), chr(39) * 2)}'" if OUTPUT else ""

# Named examples, mirroring QUERIES.md. Keys are what you pass on the CLI.
QUERIES: dict[str, str] = {
    # maxrows caps the Athena query with LIMIT — without it a bare SELECT * over
    # the full nyctaxi table materializes every row before returning and looks
    # like a hang. A plain DuckDB LIMIT would not help (no limit pushdown).
    "taxi_basic": f"""
        SELECT * FROM athena_scan('data'{_OUT}, database='nyctaxi', maxrows=10)
    """,
    "taxi_projection": f"""
        SELECT trip_distance, total_amount
        FROM athena_scan('data'{_OUT}, database='nyctaxi')
    """,
    # COUNT(*) projects no columns, so Athena scans no column data — but it
    # still returns one row per table row, paged back 1000 at a time. Unbounded
    # on nyctaxi that never finishes, so cap it: this counts the capped rows,
    # not the table. For a true count, run COUNT(*) in Athena directly.
    "taxi_count": f"""
        SELECT COUNT(*) AS trips
        FROM athena_scan('data'{_OUT}, database='nyctaxi', maxrows=1000)
    """,
    "taxi_avg_tip": f"""
        SELECT payment_type,
               COUNT(*)                  AS trips,
               ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM athena_scan('data'{_OUT}, database='nyctaxi')
        GROUP BY payment_type
        ORDER BY trips DESC
    """,
    "flights_predicate": f"""
        SELECT carrier, origin, dest, dep_delay
        FROM athena_scan('flights'{_OUT}, database='flights',
                         predicate='dep_delay > 120')
    """,
    "flights_combined": f"""
        SELECT carrier, COUNT(*) AS long_delays
        FROM athena_scan('flights'{_OUT}, database='flights',
                         predicate='year = 2016')
        WHERE dep_delay > 60
        GROUP BY carrier
        ORDER BY long_delays DESC
        LIMIT 10
    """,
    "covid_state": f"""
        SELECT date, county, state, cases, deaths
        FROM athena_scan('nytimes_counties'{_OUT}, database='covid-19',
                         predicate='state = ''New York''')
    """,
    "covid_join": f"""
        SELECT c.state,
               SUM(c.cases)                                         AS total_cases,
               MAX(TRY_CAST(p."population estimate 2018" AS BIGINT)) AS pop_2018
        FROM athena_scan('nytimes_counties'{_OUT}, database='covid-19') c
        JOIN athena_scan('county_populations'{_OUT}, database='covid-19') p
          ON c.county = p.county AND c.state = p.state
        GROUP BY c.state
        ORDER BY total_cases DESC
        LIMIT 10
    """,
    "tpcds_customer": f"""
        SELECT c_first_name, c_last_name, c_birth_year
        FROM athena_scan('customer'{_OUT}, database='tpcds',
                         predicate='c_birth_year >= 1980')
    """,
    "elb_status": f"""
        SELECT elbresponsecode, COUNT(*) AS n
        FROM athena_scan('elb_logs'{_OUT}, database='sampledb')
        GROUP BY elbresponsecode
        ORDER BY n DESC
    """,
}


def connect(ext_path: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB and load the locally built Athena extension."""
    if not ext_path.exists():
        sys.exit(
            f"Extension not found at {ext_path}\n"
            "Build it with `make` in the repo root, or set ATHENA_EXTENSION_PATH."
        )
    # allow_unsigned_extensions + metadata mismatch: locally compiled extensions
    # lack the signature/metadata DuckDB expects from official releases.
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("SET allow_extensions_metadata_mismatch=true;")
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
    parser.add_argument("--list", action="store_true", help="list example names and exit")
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
