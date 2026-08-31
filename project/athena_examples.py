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

# Optional explicit Athena results location. When unset, athena_scan uses the
# workgroup's default result configuration, so the queries below only add
# `output_location=` when ATHENA_OUTPUT_LOCATION is provided. Single quotes are
# doubled to keep the embedded SQL string literal valid.
# Partitioned fixture for the `partitioned` example; override if yours differs.
PART_DB, _, PART_TABLE = os.environ.get(
    "ATHENA_PARTITIONED_TABLE", "default.claude_part_test"
).partition(".")

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
    # An unbounded count over 1.07M rows, back in seconds: the result is
    # streamed from the single CSV Athena writes to S3 rather than paged 1000
    # rows at a time. DuckDB keeps one placeholder column for COUNT(*), so this
    # reads one column of every row.
    "taxi_count": f"""
        SELECT COUNT(*) AS trips
        FROM athena_scan('data'{_OUT}, database='nyctaxi')
    """,
    "taxi_avg_tip": f"""
        SELECT payment_type,
               COUNT(*)                  AS trips,
               ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM athena_scan('data'{_OUT}, database='nyctaxi')
        GROUP BY payment_type
        ORDER BY trips DESC
    """,
    # predicate= is evaluated by Athena, so it cuts what is scanned and
    # returned. A plain DuckDB WHERE cannot: the C extension API exposes no
    # filter pushdown (duckdb/duckdb#25163).
    "taxi_predicate": f"""
        SELECT vendorid, passenger_count, total_amount
        FROM athena_scan('data'{_OUT}, database='nyctaxi',
                         predicate='passenger_count = 9')
    """,
    # Both filters at once: Athena narrows to one payment type, DuckDB applies
    # the rest locally once the rows arrive.
    "taxi_combined": f"""
        SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM athena_scan('data'{_OUT}, database='nyctaxi',
                         predicate='payment_type = 1')
        WHERE tip_amount > 5
        GROUP BY payment_type
    """,
    # A hyphenated Glue database name has to survive quoting on the way to
    # Athena.
    "covid_population": f"""
        SELECT county, state, "population estimate 2018" AS pop_2018
        FROM athena_scan('county_populations'{_OUT}, database='covid-19')
        WHERE state = 'New York'
        ORDER BY TRY_CAST("population estimate 2018" AS BIGINT) DESC
        LIMIT 10
    """,
    # Two scans joined inside DuckDB: each runs its own Athena query, and the
    # join happens locally.
    "tpcds_join": f"""
        SELECT a.ca_state, COUNT(*) AS customers
        FROM athena_scan('customer'{_OUT}, database='tpcds',
                         predicate='c_birth_year >= 1980') c
        JOIN athena_scan('customer_address'{_OUT}, database='tpcds') a
          ON c.c_current_addr_sk = a.ca_address_sk
        GROUP BY a.ca_state
        ORDER BY customers DESC
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
    # Run this twice: the second run is served from Athena's result cache,
    # scanning 0 bytes and costing nothing. Only safe while the data underneath
    # is not changing.
    "elb_reuse": f"""
        SELECT COUNT(url) AS urls
        FROM athena_scan('elb_logs'{_OUT}, database='sampledb',
                         result_reuse_minutes=60)
    """,
    # region= overrides whatever the AWS config chain resolved, so one session
    # can read tables in more than one region.
    "elb_region": f"""
        SELECT COUNT(*) AS n
        FROM athena_scan('elb_logs'{_OUT}, database='sampledb', region='us-east-1')
    """,
    # Glue keeps partition keys out of the column list and Athena returns them
    # last; a predicate on one prunes instead of scanning. Needs the fixture
    # named by ATHENA_PARTITIONED_TABLE.
    "partitioned": f"""
        SELECT * FROM athena_scan('{PART_TABLE}'{_OUT}, database='{PART_DB}',
                                  predicate='yr = 2024')
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
