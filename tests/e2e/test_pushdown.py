"""Filter pushdown and progress: C API v2 build only.

Every case checks two things. The answer must equal the fixture's -- pushdown
may never change a result -- and the SQL "Athena" received must carry the
filter, or deliberately not carry it.
"""

from __future__ import annotations

import threading
import time

import duckdb
import pytest
from conftest import DATABASE
from fake_aws import FakeAws

pytestmark = pytest.mark.usefixtures("v2_only")

SCAN = f"athena_scan('orders', database='{DATABASE}')"
TABLE = f'"{DATABASE}"."orders"'

PUSHED = [
    ("qty = 30", '"qty" = 30'),
    ("30 > qty", '"qty" < 30'),
    ("id <> 5", '"id" <> 5'),
    ("NOT (id > 5)", '"id" <= 5'),
    ("tiny = 3 AND small < 10", '("tiny" = 3) AND ("small" < 10)'),
    ("customer = 'cust-1'", "\"customer\" = 'cust-1'"),
    ("note = 'has \"quotes\"'", '"note" = \'has "quotes"\''),
    ("note = 'it''s'", "\"note\" = 'it''s'"),
    ("order_date >= DATE '2024-06-01'", "\"order_date\" >= DATE '2024-06-01'"),
    (
        "created_at < TIMESTAMP '2024-03-01 12:00:00.5'",
        "\"created_at\" < TIMESTAMP '2024-03-01 12:00:00.5'",
    ),
    ("amount > 100.5", "\"amount\" > DECIMAL '100.50'"),
    ("big_amount > 5000000000000", "\"big_amount\" > DECIMAL '5000000000000.000'"),
    ("id IN (1, 2, 3)", '"id" IN (1, 2, 3)'),
    ("customer IS NULL", '"customer" IS NULL'),
    ("shipped = true", '"shipped" = true'),
    ("id = 5 OR id = 7", '("id" = 5) OR ("id" = 7)'),
    # A partition column: this is the filter that prunes in real Athena.
    ("yr = '2024'", "\"yr\" = '2024'"),
    # Only the part Athena can be trusted with is sent; DuckDB does the rest.
    (
        "yr = '2024' AND qty > 100 AND lower(note) = 'plain'",
        '("yr" = \'2024\') AND ("qty" > 100)',
    ),
    # DuckDB hands every range over as BETWEEN without saying whether the
    # bounds are inclusive (duckdb/duckdb#27729). The inclusive form is sent,
    # and DuckDB trims the ends -- the counts below prove it does.
    ("id > 100 AND id <= 200", '"id" >= 100 AND "id" <= 200'),
    ("id > 100 AND id < 200", '"id" >= 100 AND "id" <= 200'),
    ("id BETWEEN 100 AND 200", '"id" >= 100 AND "id" <= 200'),
]

NOT_PUSHED = [
    # Floating point: equal on Athena's binary value, re-checked on parsed text.
    "ratio > 600",
    "weight = 2.5",
    # One unpushable operand makes the whole OR unpushable.
    "id = 5 OR ratio > 600",
    # The comparison is on a function of the column, not the column.
    "lower(customer) = 'cust-1'",
    "id::VARCHAR = '5'",
    "customer LIKE 'cust-1%'",
]


def check(con: duckdb.DuckDBPyConnection, aws: FakeAws, where: str) -> str:
    """Runs a filtered scan, compares it with the fixture, returns the SQL sent."""
    columns = "COUNT(*), SUM(id), MIN(amount)"
    got = con.execute(f"SELECT {columns} FROM {SCAN} WHERE {where}").fetchall()
    want = aws.fixture.cursor().execute(
        f"SELECT {columns} FROM {DATABASE}.orders WHERE {where}"
    )
    assert got == want.fetchall()
    assert len(aws.queries) == 1
    return aws.queries[0]


@pytest.mark.parametrize(("where", "sent"), PUSHED)
def test_filter_reaches_athena(
    con: duckdb.DuckDBPyConnection, aws: FakeAws, where: str, sent: str
) -> None:
    assert check(con, aws, where).endswith(f"FROM {TABLE} WHERE {sent}")


@pytest.mark.parametrize("where", NOT_PUSHED)
def test_filter_stays_in_duckdb(
    con: duckdb.DuckDBPyConnection, aws: FakeAws, where: str
) -> None:
    assert check(con, aws, where).endswith(f"FROM {TABLE}")


def test_pushed_filter_combines_with_predicate(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = f"athena_scan('orders', database='{DATABASE}', predicate='qty > 30 OR tiny = 0')"
    got = con.execute(f"SELECT COUNT(*) FROM {scan} WHERE yr = '2024'").fetchall()
    want = aws.fixture.cursor().execute(
        f"SELECT COUNT(*) FROM {DATABASE}.orders WHERE (qty > 30 OR tiny = 0) AND yr = '2024'"
    )
    assert got == want.fetchall()
    assert aws.queries[0].endswith("WHERE (qty > 30 OR tiny = 0) AND (\"yr\" = '2024')")


def test_each_scan_of_a_self_join_gets_its_own_filter(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # Two scans of one table with different filters must not see each other's.
    got = con.execute(
        f"SELECT a.id, b.id FROM {SCAN} AS a JOIN {SCAN} AS b ON a.qty = b.qty + 3 "
        "WHERE a.id < 4 AND b.yr = '2024' ORDER BY ALL"
    ).fetchall()
    want = aws.fixture.cursor().execute(
        f"SELECT a.id, b.id FROM {DATABASE}.orders AS a JOIN {DATABASE}.orders AS b "
        "ON a.qty = b.qty + 3 WHERE a.id < 4 AND b.yr = '2024' ORDER BY ALL"
    )
    assert got == want.fetchall()
    sent = sorted(query.partition(" WHERE ")[2] for query in aws.queries)
    assert sent == ['"id" < 4', "\"yr\" = '2024'"]


def test_cte_scanned_twice_with_different_filters(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    query = (
        "WITH o AS (SELECT * FROM {source}) "
        "SELECT (SELECT COUNT(*) FROM o WHERE id <= 10), (SELECT COUNT(*) FROM o WHERE yr = '2023')"
    )
    got = con.execute(query.format(source=SCAN)).fetchall()
    assert (
        got
        == aws.fixture.cursor()
        .execute(query.format(source=f"{DATABASE}.orders"))
        .fetchall()
    )


def test_progress_moves_while_athena_runs(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # DuckDB refreshes query progress from the client thread between tasks.
    # When that thread runs the scan itself, it blocks in exec for the whole
    # wait and the bar freezes at its first reading (always with threads=1;
    # now and then on a 3-core CI runner). So the bar must move in at least
    # one of a few attempts, and never go backwards in any of them.
    aws.run_seconds = 3.0
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_progress_bar_print = false")
    attempts: list[list[float]] = []
    for _ in range(3):
        seen: list[float] = []
        done = threading.Event()

        def watch(seen: list[float] = seen, done: threading.Event = done) -> None:
            while not done.is_set():
                seen.append(con.query_progress())
                time.sleep(0.1)

        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            assert con.execute(f"SELECT COUNT(*) FROM {SCAN}").fetchall() == [(5000,)]
        finally:
            done.set()
            watcher.join()
        moving = [p for p in seen if 0 < p < 100]
        assert moving == sorted(moving), "progress went backwards"
        attempts.append(moving)
        if len(set(moving)) > 1:
            break
    assert any(len(set(m)) > 1 for m in attempts), (
        f"progress never moved: {[sorted(set(m)) for m in attempts]}"
    )
