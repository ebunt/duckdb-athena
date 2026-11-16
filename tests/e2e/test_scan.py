from __future__ import annotations

import threading

import duckdb
import pytest
from conftest import DATABASE
from fake_aws import FakeAws

SCAN = f"athena_scan('orders', database='{DATABASE}')"
# The fixture's own answer, shaped like the scan's: complex columns arrive as
# JSON text, everything else keeps its type.
TRUTH = f"SELECT * REPLACE (tags::JSON::VARCHAR AS tags) FROM {DATABASE}.orders"


def truth(aws: FakeAws, sql: str) -> list[tuple]:
    return aws.fixture.cursor().execute(sql).fetchall()


def test_full_scan_matches_the_fixture(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    got = con.execute(f"SELECT * FROM {SCAN} ORDER BY id").fetchall()
    assert got == truth(aws, f"{TRUTH} ORDER BY id")


def test_types_are_native(con: duckdb.DuckDBPyConnection) -> None:
    described = con.execute(f"DESCRIBE SELECT * FROM {SCAN}").fetchall()
    assert [(name, type_) for name, type_, *_ in described] == [
        ("id", "BIGINT"),
        ("tiny", "TINYINT"),
        ("small", "SMALLINT"),
        ("qty", "INTEGER"),
        ("customer", "VARCHAR"),
        ("note", "VARCHAR"),
        ("shipped", "BOOLEAN"),
        ("order_date", "DATE"),
        ("created_at", "TIMESTAMP"),
        ("amount", "DECIMAL(10,2)"),
        ("big_amount", "DECIMAL(25,3)"),
        ("ratio", "DOUBLE"),
        ("weight", "FLOAT"),
        ("tags", "VARCHAR"),
        ("yr", "VARCHAR"),
    ]


def test_aggregates_match(con: duckdb.DuckDBPyConnection, aws: FakeAws) -> None:
    aggregates = (
        "COUNT(*), COUNT(customer), SUM(amount), SUM(big_amount), MIN(order_date), "
        "MAX(created_at), COUNT(*) FILTER (WHERE shipped), COUNT(*) FILTER (WHERE note = '')"
    )
    got = con.execute(f"SELECT {aggregates} FROM {SCAN}").fetchall()
    assert got == truth(aws, f"SELECT {aggregates} FROM {DATABASE}.orders")


def test_projection_reaches_athena(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    con.execute(f"SELECT SUM(qty), MAX(yr) FROM {SCAN}").fetchall()
    assert aws.queries == [f'SELECT "qty", "yr" FROM "{DATABASE}"."orders"']


def test_complex_columns_are_selected_as_json(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    got = con.execute(f"SELECT tags FROM {SCAN} WHERE id = 1").fetchall()
    assert got == [('["t1","a,b"]',)]
    assert 'CAST("tags" AS JSON)' in aws.queries[0]


def test_maxrows_and_predicate_reach_athena(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = f"athena_scan('orders', database='{DATABASE}', maxrows=17, predicate='qty > 30')"
    assert con.execute(f"SELECT COUNT(*), MIN(qty) FROM {scan}").fetchall() == [
        (17, 33)
    ]
    assert aws.queries[0].endswith("WHERE qty > 30 LIMIT 17")


def test_paging_fallback_matches(con: duckdb.DuckDBPyConnection, aws: FakeAws) -> None:
    aws.expose_result_location = False
    got = con.execute(f"SELECT * FROM {SCAN} ORDER BY id").fetchall()
    assert got == truth(aws, f"{TRUTH} ORDER BY id")


def test_slow_query_is_waited_for(con: duckdb.DuckDBPyConnection, aws: FakeAws) -> None:
    aws.run_seconds = 1.5
    assert con.execute(f"SELECT COUNT(*) FROM {SCAN}").fetchall() == [(5000,)]


def test_timeout_stops_the_query(con: duckdb.DuckDBPyConnection, aws: FakeAws) -> None:
    aws.run_seconds = 30
    scan = f"athena_scan('orders', database='{DATABASE}', timeout_seconds=1)"
    with pytest.raises(duckdb.Error, match="still Queued after 1s"):
        con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    assert len(aws.stopped) == 1


def test_failed_query_carries_athenas_reason(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = (
        f"athena_scan('orders', database='{DATABASE}', predicate='no_such_column = 1')"
    )
    with pytest.raises(duckdb.Error, match="no_such_column"):
        con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()


def test_missing_table_names_what_was_looked_up(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(duckdb.Error, match=r'table "demo"\."nope" in region us-east-1'):
        con.execute(
            f"SELECT * FROM athena_scan('nope', database='{DATABASE}')"
        ).fetchall()


def test_join_against_a_local_table(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    con.execute(
        "CREATE TABLE wanted AS SELECT * FROM (VALUES (5), (50), (500)) AS t(id)"
    )
    got = con.execute(
        f"SELECT s.id, s.amount FROM {SCAN} AS s JOIN wanted USING (id) ORDER BY s.id"
    ).fetchall()
    assert got == truth(
        aws,
        f"SELECT id, amount FROM {DATABASE}.orders WHERE id IN (5, 50, 500) ORDER BY id",
    )


@pytest.mark.xfail(
    strict=True,
    reason="neither C API tells a table function its query was interrupted, "
    "so the Athena query runs (and bills) to completion; see TODO.md",
)
def test_interrupt_stops_the_athena_query(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    aws.run_seconds = 3
    threading.Timer(0.5, con.interrupt).start()
    with pytest.raises(duckdb.Error, match="Interrupted"):
        con.execute(f"SELECT COUNT(*) FROM {SCAN}").fetchall()
    assert len(aws.stopped) == 1
