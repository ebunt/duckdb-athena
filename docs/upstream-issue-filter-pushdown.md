# Upstream feature request for duckdb/duckdb

**Filed as https://github.com/duckdb/duckdb/issues/25163** on 2026-08-30.
**Closed 2026-10-13** as "fixed on nightly" — see Resolution below before
assuming this is usable.

Re-checked against DuckDB v1.5.5 before filing: `src/include/duckdb_extension.h`
still contains no occurrence of `filter`, and the two in `src/include/duckdb.h`
are prose about selection vectors. No existing issue or PR covered it.

The text below is what was filed, kept here so the argument and the measurements
survive if the issue is closed.

## Resolution (2026-10-13) — not actually usable yet

The issue was closed as "fixed on nightly," but checking the actual commit
shows it isn't a fix the extension can use:

- The capability landed in `src/main/capi/v2/capi_v2_func_table.cpp`, part of
  a brand-new **C API v2** on DuckDB's `v2.0-cyanoptera` branch (commits
  2026-10-07 to 2026-10-11, still being reworked — e.g. "switchup
  registration"). It is not an addition to the stable v1 API
  (`duckdb_extension.h`) this extension builds against, and no tagged DuckDB
  release contains it.
- The shape is different from what this issue asked for. There is no
  column/comparator/constant accessor set. Instead,
  `duckdb_v2_table_function_set_filter_pushdown_callback` registers a callback
  that receives `duckdb_v2_expression_handle` nodes
  (`duckdb_v2_table_function_filter_pushdown_get_filter`), to be walked via the
  v2 expression API, with `duckdb_v2_table_function_filter_pushdown_accept(index)`
  to mark each one as handled.
- `quack-rs`, the Rust wrapper this extension depends on, has no path to any
  of this: version 0.18.0 (its latest, released 2026-10-02) states in its own
  changelog that it "still targets [C API] v1.2.0."

Net effect: still blocked, just not for the original reason (API gap). Now
it's an unreleased, unstable, unwrapped API. Revisit when `quack-rs` adds v2
support, or DuckDB 2.0 ships a stable v2 API — whichever comes first. Tracked
in `TODO.md`.

---

**Title:** C extension API: expose table filters to table functions (filter pushdown)

**Body:**

### What I'm trying to do

I maintain a DuckDB extension that exposes AWS Athena tables as a table function
(`athena_scan`). It is written in Rust against the **C extension API**, so a
single build loads across DuckDB 1.x without recompiling per release.

Projection pushdown works well and matters a lot here: the extension rewrites
the `SELECT` list it sends to Athena, and on columnar formats that directly
reduces bytes scanned, which is what Athena bills for.

Filter pushdown would matter even more — a `WHERE` on a partition column can be
the difference between scanning a few MB and scanning the whole table — but
there appears to be no way to receive filters through the C API.

### What's missing

The C extension API exposes projection pushdown but nothing for filters. In the
generated bindings for C API v1.2.0, the string `filter` does not appear at all,
and the table-function surface is:

```
duckdb_table_function_add_named_parameter
duckdb_table_function_add_parameter
duckdb_table_function_bind_get_result_column_count
duckdb_table_function_bind_get_result_column_name
duckdb_table_function_bind_get_result_column_type
duckdb_table_function_get_client_context
duckdb_table_function_set_bind
duckdb_table_function_set_extra_info
duckdb_table_function_set_function
duckdb_table_function_set_init
duckdb_table_function_set_local_init
duckdb_table_function_set_name
duckdb_table_function_supports_projection_pushdown
```

with init-time accessors limited to:

```
duckdb_init_get_bind_data  duckdb_init_get_column_count  duckdb_init_get_column_index
duckdb_init_get_extra_info  duckdb_init_set_error  duckdb_init_set_init_data
duckdb_init_set_max_threads
```

The C++ extension API has `TableFunction::filter_pushdown` and hands filters to
the scan via `TableFunctionInitInput::filters`, so the capability exists — it
just isn't reachable from a C-API extension.

### Why not just use the C++ API

That is the workaround, and it is a real cost for a small extension: C++
extensions are built against a specific DuckDB version and have to be rebuilt
and re-released for each one, whereas the C API build loads across 1.x. For an
extension maintained by one person, that maintenance burden is the deciding
factor, which is why I'd rather ask than port.

### What would be enough

Something mirroring the projection-pushdown shape, e.g.:

- `duckdb_table_function_supports_filter_pushdown(table_function, bool)`
- at init: a count plus per-filter accessors — the column index, a comparison
  kind (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`), and the
  constant as a `duckdb_value`
- conjunction filters (AND/OR) either flattened or exposed as a small tree

Even the simple-comparison subset, with anything more complex left for DuckDB to
evaluate locally, would cover the majority of real predicates. Extensions must
be able to treat pushdown as advisory — evaluating a filter is an optimization,
and DuckDB re-checking it locally has to stay correct.

### Related

`duckdb_bind_set_cardinality` exists, so bind-time optimizer hints are already
part of the C API surface; this is asking for the filter half of the same idea.
