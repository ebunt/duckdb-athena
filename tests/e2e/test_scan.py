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


def test_unreadable_result_object_falls_back_to_paging(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # A results bucket the caller may not read (403) must not fail the scan:
    # the same rows are still reachable through GetQueryResults.
    aws.result_object_status = 403
    got = con.execute(f"SELECT * FROM {SCAN} ORDER BY id").fetchall()
    assert got == truth(aws, f"{TRUTH} ORDER BY id")
    assert aws.requests("AmazonAthena.GetQueryResults")


def test_truncated_result_object_fails_rather_than_returning_a_prefix(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # Cut on a row boundary, so the CSV parses cleanly: only the byte count
    # against Content-Length can tell 3 rows from 5000. Today the HTTP client
    # rejects the short body before results.rs's own check runs; either way
    # the scan must fail rather than answer 3.
    aws.truncate_result_after_rows = 3
    with pytest.raises(duckdb.Error, match="Athena result"):
        con.execute(f"SELECT COUNT(id) FROM {SCAN}").fetchall()


def test_refused_stop_is_reported_not_swallowed(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # The user must learn the query may still be running and billing, and how
    # to stop it themselves.
    aws.run_seconds = 30
    aws.refuse_stop = True
    scan = f"athena_scan('orders', database='{DATABASE}', timeout_seconds=1)"
    with pytest.raises(duckdb.Error) as raised:
        con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    message = str(raised.value)
    assert "Stopping it failed" in message
    assert "aws athena stop-query-execution" in message
    assert "--region us-east-1" in message
    assert aws.requests("AmazonAthena.StopQueryExecution")
    assert aws.stopped == []


def test_result_reuse_reaches_athena(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = f"athena_scan('orders', database='{DATABASE}', result_reuse_minutes=90)"
    con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    con.execute(f"SELECT COUNT(*) FROM {SCAN}").fetchall()
    with_reuse, without = aws.requests("AmazonAthena.StartQueryExecution")
    assert with_reuse["ResultReuseConfiguration"] == {
        "ResultReuseByAgeConfiguration": {"Enabled": True, "MaxAgeInMinutes": 90}
    }
    assert "ResultReuseConfiguration" not in without


@pytest.fixture
def profile_config(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An ~/.aws/config with one profile whose key and region differ from the
    environment's, so every signed request shows which one was used."""
    config = tmp_path / "config"
    config.write_text(
        "[profile other]\n"
        "aws_access_key_id = profile-key\n"
        "aws_secret_access_key = profile-secret\n"
        "region = ap-southeast-2\n"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent"))


def test_region_parameter_signs_every_call_for_that_region(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = f"athena_scan('orders', database='{DATABASE}', region='eu-west-1')"
    con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    assert aws.signers == {("eu-west-1", "fake")}


@pytest.mark.usefixtures("profile_config")
def test_profile_beats_environment_credentials(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    # AWS_ACCESS_KEY_ID is set (conftest). The default chain reads it before
    # any profile, which once made profile= a silent no-op: the scan ran as
    # the environment's identity, possibly in another account.
    scan = f"athena_scan('orders', database='{DATABASE}', profile='other')"
    con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    assert aws.signers == {("ap-southeast-2", "profile-key")}


@pytest.mark.usefixtures("profile_config")
def test_region_parameter_beats_the_profiles_region(
    con: duckdb.DuckDBPyConnection, aws: FakeAws
) -> None:
    scan = (
        f"athena_scan('orders', database='{DATABASE}', profile='other', "
        "region='eu-west-1')"
    )
    con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()
    assert aws.signers == {("eu-west-1", "profile-key")}


@pytest.mark.usefixtures("profile_config")
def test_unknown_profile_is_named(con: duckdb.DuckDBPyConnection) -> None:
    scan = f"athena_scan('orders', database='{DATABASE}', profile='nope')"
    with pytest.raises(duckdb.Error, match="profile `nope` was not defined"):
        con.execute(f"SELECT COUNT(*) FROM {scan}").fetchall()


def test_environment_supplies_session_defaults(
    con: duckdb.DuckDBPyConnection, aws: FakeAws, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Read at bind, not at LOAD: a session sets them once instead of repeating
    # workgroup= and output_location= on every scan.
    monkeypatch.setenv("ATHENA_WORKGROUP", "etl")
    monkeypatch.setenv("ATHENA_OUTPUT_LOCATION", "s3://elsewhere/prefix/")
    con.execute(f"SELECT COUNT(*) FROM {SCAN}").fetchall()
    con.execute(
        f"SELECT COUNT(*) FROM athena_scan('orders', database='{DATABASE}', "
        "workgroup='adhoc', output_location='s3://mine/')"
    ).fetchall()
    from_env, named = aws.requests("AmazonAthena.StartQueryExecution")
    assert from_env["WorkGroup"] == "etl"
    assert from_env["ResultConfiguration"] == {
        "OutputLocation": "s3://elsewhere/prefix/"
    }
    assert named["WorkGroup"] == "adhoc"
    assert named["ResultConfiguration"] == {"OutputLocation": "s3://mine/"}
