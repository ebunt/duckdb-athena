# Upstream feature request for duckdb/duckdb

**Filed as https://github.com/duckdb/duckdb/issues/25199** on 2026-08-31.

Companion to `upstream-issue-filter-pushdown.md`. Same root cause: the C
extension API exposes a subset of what C++ `TableFunction` supports, and the
missing pieces are the ones a remote-backend extension needs most.

Verified against DuckDB v1.5.5 before filing. In `src/include/duckdb_extension.h`
the only two occurrences of `progress` are `duckdb_query_progress(connection)`
and its macro — the consumer-side getter. `src/include/duckdb/function/
table_function.hpp:458` has `table_function_progress_t table_scan_progress`.
Searched existing issues: #22519 covers the progress bar during CTAS/COPY in
core, not the C API.

The text below is what was filed, kept here so the argument survives if the
issue is closed.

---

**Title:** C extension API: expose a progress callback for table functions

**Body:**

### What I'm trying to do

I maintain a DuckDB extension exposing AWS Athena tables as a table function
(`athena_scan`), written in Rust against the **C extension API** so one build
loads across DuckDB 1.x.

An Athena query is slow in a specific way: the extension submits it, then polls
until Athena finishes. Ten seconds is normal, minutes happen. During that whole
window the extension knows how long it has waited and how many bytes Athena has
scanned so far — real progress information — and has no way to report it.

### The problem

DuckDB's progress bar renders and sits at 0% for the entire wait, which reads as
a hang rather than as work in progress.

The C API's table-function surface in v1.5.5 is:

```
duckdb_table_function_set_bind / set_init / set_local_init / set_function
duckdb_table_function_add_parameter / add_named_parameter
duckdb_table_function_set_name / set_extra_info
duckdb_table_function_bind_get_result_column_{count,name,type}
duckdb_table_function_get_client_context
duckdb_table_function_supports_projection_pushdown
```

There is no progress callback. `duckdb_query_progress` exists but takes a
`duckdb_connection` and returns progress *to* a caller; it is not a way for a
table function to report progress *in*.

C++ table functions have exactly this, at
`src/include/duckdb/function/table_function.hpp:458`:

```cpp
typedef double (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data,
                                            const GlobalTableFunctionState *global_state);
...
table_function_progress_t table_scan_progress;
```

### What I'd like

A C equivalent, mirroring how `supports_projection_pushdown` mirrors its C++
counterpart — something like:

```c
typedef double (*duckdb_table_function_progress_t)(duckdb_function_info info);
void duckdb_table_function_set_progress(duckdb_table_function function,
                                        duckdb_table_function_progress_t progress);
```

Returning 0..1, or a negative value for "unknown", so a function that cannot
estimate stays out of the way.

### Why it matters beyond this extension

Any table function over a remote backend has the same shape: submit, wait, then
stream. Extensions reading from HTTP APIs, warehouses, or object stores can all
say something useful during the wait, and none of them can say it through the C
API. The extensions that *can* drive the bar are the ones built in C++ against
DuckDB's internals, which is the tradeoff the C API exists to avoid.

### Workaround, and why it is not enough

The extension now prints its own heartbeat to stderr during the poll:

```
Running Athena query, execution id: 00000000-0000-0000-0000-000000000000
Athena query RUNNING, 4s elapsed, 1.31 GB scanned
```

That works, but it scrolls DuckDB's progress bar away and puts extension output
where the UI should be. A progress callback would let the bar show the truth
instead, and would work identically in the CLI, the Python client, and any other
consumer of `duckdb_query_progress`.

### Version

DuckDB v1.5.5, C extension API (`C_STRUCT` ABI), extension built with Rust.
