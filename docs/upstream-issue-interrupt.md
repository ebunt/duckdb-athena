# Upstream feature request for duckdb/duckdb

**Not filed yet.** Draft written 2026-11-16.

Third in the series after `upstream-issue-filter-pushdown.md` and
`upstream-issue-progress-callback.md`, with the same root cause: the C
extension API exposes a subset of what C++ table functions can do, and the
missing pieces are the ones a remote-backend extension needs most.

Verified against DuckDB v2.0.0 before drafting. In `src/include/duckdb_extension.h`
the only interrupt-related function is `duckdb_interrupt(duckdb_connection)`;
in `src/include/duckdb_extension_v2.h` it is `duckdb_v2_connection_interrupt`.
Both send an interrupt. Neither header has a way for a table function to learn
that one was sent. Search existing issues for "C API interrupt table function"
before filing.

Measured with `scripts/cancel-check.sh` (real Athena, macOS arm64, DuckDB
2.0.0 CLI) and against the fake AWS in `tests/e2e` (Linux, DuckDB 2.0.0 CLI and
Python): Ctrl-C during the wait leaves the Athena query `RUNNING`, DuckDB
returns `Interrupted` only once Athena has finished, and Athena runs the query
to `SUCCEEDED`.

The text below is what to file.

---

**Title:** C extension API: let a table function know its query was interrupted

**Body:**

### What I'm trying to do

I maintain a DuckDB extension that exposes AWS Athena tables as a table function
(`athena_scan`), written in Rust against the **C extension API**. A scan submits
a query to Athena, polls until it finishes, then streams the result.

### The problem

When the user presses Ctrl-C (or a client calls `duckdb_interrupt`), DuckDB
marks the query interrupted, but the table function has no way to find out. It
keeps polling until the remote query finishes, and only then does DuckDB raise
`Interrupted`. Two costs:

- **The cancel is not a cancel.** A query the user abandoned after one second
  holds the connection for as long as Athena takes: minutes, sometimes.
- **The remote work is billed in full.** Athena charges per byte scanned. The
  extension can stop the query (`StopQueryExecution`), and does so when its own
  timeout expires, but cannot do so on interrupt, because it never hears of
  one. Measured: an interrupt sent while the Athena query was `RUNNING`; the
  query then ran to `SUCCEEDED`.

This is not specific to Athena. Any table function that waits on something
remote (a warehouse query, a long HTTP request, a job queue) has a wait it
could cut short and remote work it could cancel.

### What exists today

- C API v1 (`duckdb_extension.h`): `duckdb_interrupt(duckdb_connection)` sends
  an interrupt. A table function has neither a connection handle nor any
  "was I interrupted?" accessor on `duckdb_function_info` / `duckdb_init_info`.
- C API v2 (`duckdb_extension_v2.h`): `duckdb_v2_connection_interrupt` sends
  one. Same gap on the receiving side.
- C++: table functions run with an `ExecutionContext` whose `ClientContext`
  carries the interrupted flag, so a C++ extension can check it during a long
  wait.

Returning early is not an option: a table function's only way to yield is to
return a chunk, and an empty chunk means end of stream.

### What I'd like

Either of these would do. The first is the smallest change.

```c
// Polled by the function during a long operation.
bool duckdb_function_is_interrupted(duckdb_function_info info);
bool duckdb_init_is_interrupted(duckdb_init_info info);
```

or, for C API v2, the same check on the table function's context handle, so a
function waiting in `init` or `exec` can poll it between remote calls.

A registration-time callback ("call me when interrupted") would also work, but
a poll is simpler for both sides and matches how the C++ side is used.

### Workaround, and why it is not enough

The extension could install its own SIGINT handler and stop the remote query
when it fires. That covers the CLI only: it never sees `duckdb_interrupt` from
the Python client, JDBC, or any other embedding, and a process-wide signal
handler is the wrong thing for a library to install.

### Version

DuckDB v2.0.0, C extension API (`C_STRUCT` ABI) and C API v2, extension built
with Rust.
