"""A fake Glue, Athena and S3, small enough to read, real enough to scan.

The extension talks to it through `AWS_ENDPOINT_URL`. Queries Athena would run
are run by an in-process DuckDB over fixture tables instead, and the result is
written in Athena's own CSV dialect, so a scan through the extension can be
compared with the fixture directly. Every request is recorded, which is what
lets a test assert on the SQL "Athena" received.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import duckdb

RESULT_BUCKET = "fake-athena-results"
PAGE_SIZE = 1000


@dataclass
class Table:
    """A Glue table: Glue type strings, kept apart from the fixture's SQL types."""

    columns: list[tuple[str, str]]
    partition_keys: list[tuple[str, str]] = field(default_factory=list)
    parameters: dict[str, str] = field(default_factory=dict)


@dataclass
class Execution:
    query: str
    workgroup: str
    started: float
    csv: bytes | None = None
    rows: list[list[str | None]] = field(default_factory=list)
    error: str | None = None
    cancelled: bool = False


def athena_cell(value: Any) -> str | None:
    """Renders one value the way Athena prints it in a result file."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.") + f"{value.microsecond // 1000:03d}"
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    return str(value)


def athena_csv(header: list[str], rows: list[list[str | None]]) -> bytes:
    """Athena's dialect: every non-NULL value quoted, NULL an empty field."""

    def cell(value: str | None) -> str:
        return "" if value is None else '"' + value.replace('"', '""') + '"'

    lines = [",".join(cell(name) for name in header)]
    lines.extend(",".join(cell(value) for value in row) for row in rows)
    return ("\n".join(lines) + "\n").encode()


class FakeAws:
    def __init__(self) -> None:
        self.fixture = duckdb.connect()
        self.tables: dict[tuple[str, str], Table] = {}
        self.executions: dict[str, Execution] = {}
        self.stopped: list[str] = []
        # Seconds a query spends QUEUED then RUNNING before it finishes.
        self.run_seconds = 0.0
        # False makes executions expose no S3 location, as a workgroup with
        # Athena-managed results does; the extension must page instead.
        self.expose_result_location = True
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def add_table(self, database: str, name: str, table: Table) -> None:
        self.tables[(database, name)] = table

    @property
    def queries(self) -> list[str]:
        return [execution.query for execution in self.executions.values()]

    def reset(self) -> None:
        self.executions.clear()
        self.stopped.clear()
        self.run_seconds = 0.0
        self.expose_result_location = True

    # -- service operations -------------------------------------------------

    def get_table(self, request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        key = (request["DatabaseName"], request["Name"])
        table = self.tables.get(key)
        if table is None:
            return 400, {
                "__type": "EntityNotFoundException",
                "Message": "Entity Not Found",
            }

        def columns(items: list[tuple[str, str]]) -> list[dict[str, str]]:
            return [{"Name": name, "Type": glue_type} for name, glue_type in items]

        return 200, {
            "Table": {
                "Name": key[1],
                "DatabaseName": key[0],
                "StorageDescriptor": {"Columns": columns(table.columns)},
                "PartitionKeys": columns(table.partition_keys),
                "Parameters": table.parameters,
            }
        }

    def start_query(self, request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        execution = Execution(
            query=request["QueryString"],
            workgroup=request.get("WorkGroup", "primary"),
            started=time.monotonic(),
        )
        with self._lock:
            try:
                cursor = self.fixture.cursor()
                cursor.execute(execution.query)
                header = [column[0] for column in cursor.description]
                execution.rows = [
                    [athena_cell(v) for v in row] for row in cursor.fetchall()
                ]
                execution.csv = athena_csv(header, execution.rows)
                execution.rows.insert(0, header)
            except duckdb.Error as exc:
                execution.error = str(exc).splitlines()[0]
        execution_id = str(uuid.uuid4())
        self.executions[execution_id] = execution
        return 200, {"QueryExecutionId": execution_id}

    def state_of(self, execution: Execution) -> str:
        if execution.cancelled:
            return "CANCELLED"
        elapsed = time.monotonic() - execution.started
        if elapsed < self.run_seconds / 4:
            return "QUEUED"
        if elapsed < self.run_seconds:
            return "RUNNING"
        return "FAILED" if execution.error else "SUCCEEDED"

    def get_query_execution(
        self, request: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        execution_id = request["QueryExecutionId"]
        execution = self.executions[execution_id]
        state = self.state_of(execution)
        status: dict[str, Any] = {"State": state}
        if state == "FAILED":
            status["StateChangeReason"] = execution.error
        body: dict[str, Any] = {
            "QueryExecutionId": execution_id,
            "Query": execution.query,
            "Status": status,
            "Statistics": {"DataScannedInBytes": len(execution.csv or b"")},
        }
        if self.expose_result_location:
            body["ResultConfiguration"] = {
                "OutputLocation": f"s3://{RESULT_BUCKET}/{execution_id}.csv"
            }
        return 200, {"QueryExecution": body}

    def stop_query(self, request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        execution_id = request["QueryExecutionId"]
        self.executions[execution_id].cancelled = True
        self.stopped.append(execution_id)
        return 200, {}

    def get_query_results(self, request: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        execution = self.executions[request["QueryExecutionId"]]
        start = int(request.get("NextToken", 0))
        size = min(int(request.get("MaxResults", PAGE_SIZE)), PAGE_SIZE)
        page = execution.rows[start : start + size]
        body: dict[str, Any] = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{} if v is None else {"VarCharValue": v} for v in row]}
                    for row in page
                ]
            }
        }
        if start + size < len(execution.rows):
            body["NextToken"] = str(start + size)
        return 200, body

    def get_object(self, path: str) -> tuple[int, bytes]:
        bucket, _, key = path.lstrip("/").partition("/")
        execution = self.executions.get(key.removesuffix(".csv"))
        if bucket != RESULT_BUCKET or execution is None or execution.csv is None:
            return 404, b"<Error><Code>NoSuchKey</Code></Error>"
        return 200, execution.csv

    # -- http ---------------------------------------------------------------

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        fake = self
        operations = {
            "AWSGlue.GetTable": fake.get_table,
            "AmazonAthena.StartQueryExecution": fake.start_query,
            "AmazonAthena.GetQueryExecution": fake.get_query_execution,
            "AmazonAthena.StopQueryExecution": fake.stop_query,
            "AmazonAthena.GetQueryResults": fake.get_query_results,
        }

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:
                pass

            def _send(self, status: int, body: bytes, content_type: str) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                request = json.loads(self.rfile.read(length) or b"{}")
                operation = operations.get(self.headers.get("X-Amz-Target", ""))
                if operation is None:
                    status, payload = 400, {"__type": "UnknownOperationException"}
                else:
                    status, payload = operation(request)
                self._send(
                    status, json.dumps(payload).encode(), "application/x-amz-json-1.1"
                )

            def do_GET(self) -> None:
                status, body = fake.get_object(self.path.partition("?")[0])
                self._send(
                    status,
                    body,
                    "application/octet-stream" if status == 200 else "application/xml",
                )

        return Handler
