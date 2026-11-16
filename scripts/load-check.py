"""Load the packaged extension into whichever DuckDB this interpreter has.

No AWS credentials needed: LOAD exercises the footer, platform stamp and init
symbol, and an empty `database=` fails at bind before any AWS call.

    uv run --no-project --with duckdb==2.0.0 python scripts/load-check.py \
        build/release/extension/athena/athena.duckdb_extension
"""

import sys

import duckdb

EXPECTED_BIND_ERROR = "database must not be empty"


def main(extension: str) -> int:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension}'")

    row = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'athena'"
    ).fetchone()
    if row is None or not row[0]:
        print(f"duckdb {duckdb.__version__}: extension reported no version")
        return 1

    try:
        con.execute("SELECT * FROM athena_scan('t', database='')")
    except duckdb.Error as exc:
        if EXPECTED_BIND_ERROR not in str(exc):
            print(f"duckdb {duckdb.__version__}: unexpected error: {exc}")
            return 1
    else:
        print(f"duckdb {duckdb.__version__}: bind accepted an empty database")
        return 1

    print(f"duckdb {duckdb.__version__}: loaded athena {row[0]}, bind reachable")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} path/to/athena.duckdb_extension")
    sys.exit(main(sys.argv[1]))
