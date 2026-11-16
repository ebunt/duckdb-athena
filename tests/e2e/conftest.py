"""End-to-end tests: the built extension, a real DuckDB, a fake AWS.

make e2e       # the default (C API v1) build
make e2e_v2    # the experimental C API v2 build; adds the pushdown tests
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from fake_aws import FakeAws, Table

DATABASE = "demo"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--extension",
        default="build/release/extension/athena/athena.duckdb_extension",
        help="path to the packaged athena.duckdb_extension",
    )


@pytest.fixture(scope="session")
def extension_path(request: pytest.FixtureRequest) -> str:
    path = Path(request.config.getoption("--extension")).resolve()
    if not path.is_file():
        pytest.exit(f"no extension at {path}; build it first", returncode=2)
    return str(path)


@pytest.fixture(scope="session")
def capi_v2(extension_path: str) -> bool:
    """Whether the extension under test is the experimental C API v2 build.

    The metadata footer says so: a v2 build is stamped C_STRUCT_UNSTABLE.
    """
    return b"C_STRUCT_UNSTABLE" in Path(extension_path).read_bytes()[-1024:]


@pytest.fixture
def v2_only(capi_v2: bool) -> None:
    if not capi_v2:
        pytest.skip("needs the C API v2 build (make release_v2)")


@pytest.fixture(scope="session")
def aws() -> Iterator[FakeAws]:
    fake = FakeAws()
    fake.fixture.execute(f"CREATE SCHEMA {DATABASE}")
    fake.fixture.execute(
        f"""
        CREATE TABLE {DATABASE}.orders AS
        SELECT
            i::BIGINT AS id,
            (i % 7)::TINYINT AS tiny,
            (i % 300)::SMALLINT AS small,
            (i * 3)::INTEGER AS qty,
            CASE WHEN i % 11 = 0 THEN NULL ELSE 'cust-' || (i % 50) END AS customer,
            CASE i % 5
                WHEN 0 THEN 'plain'
                WHEN 1 THEN 'has,comma'
                WHEN 2 THEN 'has "quotes"'
                WHEN 3 THEN ''
                ELSE E'two\\nlines'
            END AS note,
            CASE WHEN i % 13 = 0 THEN NULL ELSE i % 2 = 0 END AS shipped,
            DATE '2024-01-01' + (i % 400)::INTEGER AS order_date,
            TIMESTAMP '2024-01-01 00:00:00' + to_milliseconds(i * 86400123) AS created_at,
            (i * 1.25)::DECIMAL(10,2) AS amount,
            (i * 1000000007.125)::DECIMAL(25,3) AS big_amount,
            (i / 8)::DOUBLE AS ratio,
            (i / 4)::FLOAT AS weight,
            ['t' || (i % 3), 'a,b'] AS tags,
            (2023 + i % 2)::VARCHAR AS yr
        FROM range(1, 5001) AS r(i)
        """
    )
    fake.add_table(
        DATABASE,
        "orders",
        Table(
            columns=[
                ("id", "bigint"),
                ("tiny", "tinyint"),
                ("small", "smallint"),
                ("qty", "int"),
                ("customer", "string"),
                ("note", "varchar(64)"),
                ("shipped", "boolean"),
                ("order_date", "date"),
                ("created_at", "timestamp"),
                ("amount", "decimal(10,2)"),
                ("big_amount", "decimal(25,3)"),
                ("ratio", "double"),
                ("weight", "float"),
                ("tags", "array<string>"),
            ],
            partition_keys=[("yr", "string")],
            parameters={"recordCount": "5000"},
        ),
    )
    fake.start()
    os.environ.update(
        AWS_ENDPOINT_URL=fake.endpoint,
        AWS_ACCESS_KEY_ID="fake",
        AWS_SECRET_ACCESS_KEY="fake",
        AWS_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true",
    )
    os.environ.pop("AWS_PROFILE", None)
    os.environ.pop("AWS_SESSION_TOKEN", None)
    yield fake
    fake.stop()


@pytest.fixture
def con(aws: FakeAws, extension_path: str) -> Iterator[duckdb.DuckDBPyConnection]:
    aws.reset()
    connection = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    connection.execute(f"LOAD '{extension_path}'")
    yield connection
    connection.close()
