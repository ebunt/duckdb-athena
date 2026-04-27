# Implementation Plan: Athena Filter/Predicate Pushdown

## Overview
This plan organizes the TODO.md roadmap into a logical implementation sequence, accounting for dependencies and complexity. The goal is to evolve `athena_scan` from materialized result sets to a fully optimized table function with projection and predicate pushdown, lazy result pagination, and comprehensive testing.

## Phase 1: Foundation & Discovery

### Step 1: Establish the Pushdown API Path
**Prerequisite:** Determines all downstream choices.
- Investigate `libduckdb-sys` C bindings to check for filter/predicate pushdown support
- Currently have projection pushdown bindings (`duckdb_table_function_supports_projection_pushdown`), but no table-filter callbacks or `duckdb_table_filter` accessors
- **Decision point:** Can we use C API, or must we drop to C++ extension API?
- **Outcome:** Decide between optimizer-driven pushdown vs. hardened manual `predicate=` parameter
- **Update:** README to document the chosen approach

### Step 2: Add SQL Query Construction Infrastructure
**Prerequisite for:** Steps 4, 5, 6, 7  
**Rationale:** Centralize query building before adding dynamic filters; easier to test and maintain.
- Replace ad-hoc string concatenation in `read_athena_init` with a dedicated query builder module
- Separate concerns: identifier quoting vs. literal rendering
- Support: `SELECT <columns> FROM <qualified_table> [WHERE <predicate>] [LIMIT n]`
- Add unit tests for:
  - Identifier quoting (double quotes, escaping)
  - Literal type-aware SQL escaping
  - Limit clause handling
  - Clause ordering
- **No runtime behavior change yet** — existing queries should produce identical SQL

## Phase 2: Core Infrastructure

### Step 3: Capture and Model Table Schema at Bind Time
**Prerequisite for:** Steps 4, 5, 6  
**Rationale:** Schema metadata is needed for projection and predicate mapping.
- Extend `ScanBindData` to retain ordered column metadata from Glue:
  - Column name
  - DuckDB type
  - Athena/Glue type string
  - Ordinal position
  - Partition column flag
- Preserve ordering: data columns first, partition columns after (matches Athena `SELECT *`)
- **No runtime behavior change** — this is storage and preparation only

## Phase 3: Feature Implementation

### Step 4: Implement Projection Pushdown
**Prerequisite for:** Step 8  
**Rationale:** Simpler than predicate pushdown; delivers early optimization win.
- Enable `.projection_pushdown(true)` in `build_table_function_def`
- In `read_athena_init`, read projected column indexes:
  - `quack_rs::table::InitInfo::projected_column_count()`
  - `quack_rs::table::InitInfo::projected_column_index(i)`
- Generate `SELECT col_a, col_b` instead of `SELECT *` when DuckDB requests a subset
- Update `ScanInitData` to store projected schema for result conversion
- **Verification:** Test that `SELECT year FROM athena_scan(...) WHERE year = 2024` doesn't fetch unneeded columns

### Step 5: Define Supported Predicate Subset
**Rationale:** Constrains scope and reduces risk.
- **Support initially:**
  - Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`
  - Null checks: `IS NULL`, `IS NOT NULL`
  - Boolean conjunctions: `AND`
  - Small `IN (...)` lists
- **Defer/reject:**
  - `OR` (unless DuckDB API gives sufficient expression structure)
  - `LIKE`, regex, user functions, casts, arithmetic
  - Complex types (nested structs, arrays, maps)
  - Floating-point edge cases (NaN)
- Document clearly which predicates fall back to DuckDB-side filtering

### Step 6: Translate DuckDB Predicates to Athena SQL
**Depends on:** Steps 2, 3, 5  
**Rationale:** Core feature for auto-pushdown.
- Add `predicate.rs` module:
  - Parse DuckDB filter expressions into internal AST
  - Validate column references against bind-time schema (no raw interpolation)
  - Render Athena-compatible SQL with type-aware literal escaping
- Support types:
  - Numeric/boolean literals (unquoted)
  - String/varchar/char (quoted, escaped)
  - Date/timestamp (as Athena literals or quoted strings)
- Add unit tests for each operator, type, and escaping edge case (e.g., embedded single quotes)

### Step 7: Integrate Predicates into Athena Execution
**Depends on:** Steps 2, 3, 6  
**Rationale:** Enable actual pushdown at query time.
- Store pushed predicate SQL in `ScanBindData`
- Modify `read_athena_init` to submit:
  - `SELECT <projection> FROM <table> WHERE <pushed_predicate> [LIMIT n]`
  - Use query builder from Step 2
- Add debug/trace logging of generated Athena queries (not always-on console print)
- Error handling: unsupported pushdown falls back to DuckDB filtering or returns clear error (for manual `predicate=` mode)
- **No behavior change for valid predicates** — should be logically equivalent, faster

## Phase 4: Optimization & Polish

### Step 8: Avoid Materializing All Athena Result Pages
**Depends on:** Steps 4, 7  
**Rationale:** Reduce memory footprint and latency; enable streaming for large result sets.
- Convert `ScanInitData` from `Vec<GetQueryResultsOutput>` to streaming paginator state
- Fetch result pages lazily from `read_athena` instead of in `read_athena_init`
- First-page header skipping logic becomes explicit in page reader state
- **Verification:** Memory usage should decrease for large result sets; latency to first chunk should improve

## Phase 5: Validation & Documentation

### Step 9: Add Verification Coverage
**Rationale:** Ensure correctness under optimization.
- Pure Rust unit tests:
  - Query building (Step 2)
  - Predicate rendering (Step 6)
  - Schema metadata handling (Step 3)
- Integration tests (behind `ATHENA_TEST_DATABASE`, `ATHENA_TEST_TABLE`, `ATHENA_TEST_OUTPUT` gates):
  - Projection pushdown reduces network/compute
  - Partition predicates reduce `data_scanned_in_bytes` (check Athena stats)
  - Unsupported predicates fall back correctly without losing rows
  - Lazy pagination works end-to-end
- End-to-end smoke tests for common patterns

### Step 10: Update Docs and Examples
**Rationale:** Clarify behavior change and guide users.
- Update README:
  - Remove/clarify "filter pushdown not implemented" note
  - Document which `WHERE` clauses are pushed (if optimizer-driven) or manual `predicate=` syntax
  - Clarify that Athena still scans partition data unless predicate matches partitions
- Add examples:
  - Partition-column filters
  - Ordinary column filters
  - Projection + filter
  - Unsupported filters and fallback
- Add troubleshooting section on Athena data scan estimation

## Timeline & Effort Estimate

| Phase | Steps | Effort | Dependencies |
|-------|-------|--------|--------------|
| 1 (Foundation) | 1–2 | 1–2 weeks | None |
| 2 (Infrastructure) | 3 | 1 week | Phase 1 |
| 3 (Features) | 4–7 | 3–4 weeks | Phase 2 |
| 4 (Optimization) | 8 | 1–2 weeks | Phase 3 |
| 5 (Polish) | 9–10 | 1–2 weeks | Phase 4 |
| **Total** | | ~7–11 weeks | Sequential |

## Key Decision Points

1. **Step 1 outcome:** Does `libduckdb-sys` expose predicate pushdown?
   - If yes: implement true optimizer-driven pushdown; update README.
   - If no: harden manual `predicate=` parameter and document it clearly.

2. **Step 5 outcome:** How strict should the predicate grammar be?
   - Conservative approach: reject anything that isn't in the safe list.
   - Permissive approach: accept more, fall back to DuckDB for unsupported cases.

3. **Step 8 timing:** Is streaming important for your use case?
   - Can defer to post-MVP if current materialization is acceptable.
   - Prioritize if users typically query large Athena tables.

## Notes

- Steps are ordered to maximize testability and minimize rework.
- Each step completes independently but builds on prior steps.
- Integration tests (Step 9) require AWS credentials and live Athena tables; use feature gates or environment variables.
- The "MVP status" in TODO.md (explicit `predicate` parameter) is already done; this plan continues from there.
