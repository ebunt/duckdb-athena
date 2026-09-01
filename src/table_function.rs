use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_sdk_athena::{
    operation::get_query_execution::GetQueryExecutionOutput,
    operation::get_query_results::GetQueryResultsOutput,
    types::{
        QueryExecutionState::{self, *},
        ResultConfiguration, ResultReuseByAgeConfiguration, ResultReuseConfiguration, Row,
    },
    Client as AthenaClient,
};
use aws_sdk_glue::Client as GlueClient;
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_data_chunk_set_size, duckdb_function_info,
    duckdb_function_set_error, duckdb_init_info, idx_t,
};
use quack_rs::{
    table::{BindInfo, FfiBindData, FfiInitData, InitInfo, TableFunctionBuilder},
    types::{LogicalType, TypeId},
};
use std::{
    ffi::CString,
    thread,
    time::{Duration, Instant},
};

use crate::results::{parse_s3_uri, CsvRowStream, ResultRow};
use crate::types::{map_type, populate_column, ColType};

struct ScanBindData {
    tablename: String,
    database: String,
    /// Explicit S3 results location, or `None` to let Athena apply the
    /// workgroup's own result configuration.
    output_location: Option<String>,
    workgroup: String,
    /// AWS config for this scan, resolved once at bind (honouring `region=`)
    /// and reused by init. Scoped to the scan: a long-lived process that
    /// changes `AWS_PROFILE` between queries must see the new profile.
    config: aws_config::SdkConfig,
    /// How long to wait for the Athena query to reach a terminal state.
    timeout: Duration,
    /// Minutes Athena may reuse a previous identical query's result for, or
    /// `None` to always re-run. Reused results scan no data and cost nothing.
    result_reuse_minutes: Option<i32>,
    limit: i32,
    predicate: Option<String>,
    /// Output column names in the order registered with DuckDB: data columns
    /// first, then partition columns. Used to map DuckDB's projected column
    /// indexes back to Athena column names for projection pushdown.
    columns: Vec<String>,
    /// Resolved DuckDB type of each column in `columns` (same order). Carried
    /// from bind so the scan writes each value — decimals especially — with the
    /// exact physical width registered here, never re-derived from result metadata.
    col_types: Vec<ColType>,
}

/// Lazily fetches the next Athena result page: `None` at end of stream, `Err`
/// on a page-fetch failure. Boxing keeps the concrete paginator type internal
/// to `read_athena_init` (no smithy types to name here). It is `Send + 'static`
/// — all `FfiInitData` requires. `athena_scan` registers no `local_init` and
/// never raises `max_threads` above DuckDB's default of 1, so `read_athena`
/// runs single-threaded and this need not be `Sync`.
type PageFetcher = Box<dyn FnMut() -> Option<Result<GetQueryResultsOutput, String>> + Send>;

/// How the scan reads the finished query's results.
enum ScanMode {
    /// Stream the single CSV object Athena wrote to S3: one `GetObject` for the
    /// whole result set instead of a `GetQueryResults` round trip per 1000 rows.
    Csv { rows: Box<CsvRowStream> },
    /// Fallback when the execution exposes no S3 result location (a workgroup
    /// using Athena-managed query results) or S3 is unreadable: page
    /// `GetQueryResults` 1000 rows at a time.
    Pages {
        next_page: PageFetcher,
        /// The first Athena page's first row is the column header, skipped once.
        first_page: bool,
    },
}

struct ScanInitData {
    mode: ScanMode,
    /// Resolved type of each projected column, in the order DuckDB projected them
    /// (which is also Athena's SELECT-list order), so writes are positional.
    col_types: Vec<ColType>,
    done: bool,
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena(info: duckdb_function_info, output: duckdb_data_chunk) {
    unsafe {
        let Some(state) = FfiInitData::<ScanInitData>::get_mut(info) else {
            duckdb_data_chunk_set_size(output, 0);
            return;
        };
        if state.done {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }

        let (page, was_first) = match &mut state.mode {
            // Read the next vector's worth of rows straight from the result CSV.
            ScanMode::Csv { rows } => {
                let capacity = libduckdb_sys::duckdb_vector_size() as usize;
                match rows.next_rows(capacity) {
                    Ok(rows) => {
                        if rows.is_empty() {
                            state.done = true;
                        }
                        if let Err(e) = rows_to_duckdb_data_chunk(&rows, &state.col_types, output) {
                            let msg = CString::new(e).unwrap_or_default();
                            duckdb_function_set_error(info, msg.as_ptr());
                            duckdb_data_chunk_set_size(output, 0);
                            state.done = true;
                        }
                    }
                    Err(e) => {
                        let msg = CString::new(e).unwrap_or_default();
                        duckdb_function_set_error(info, msg.as_ptr());
                        duckdb_data_chunk_set_size(output, 0);
                        state.done = true;
                    }
                }
                return;
            }
            // Fetch the next page lazily. Any page-fetch error surfaces here
            // rather than at init, so a query can emit earlier pages before
            // failing.
            ScanMode::Pages {
                next_page,
                first_page,
            } => match next_page() {
                Some(Ok(page)) => {
                    let was_first = *first_page;
                    *first_page = false;
                    (page, was_first)
                }
                Some(Err(e)) => {
                    let msg = CString::new(e).unwrap_or_default();
                    duckdb_function_set_error(info, msg.as_ptr());
                    duckdb_data_chunk_set_size(output, 0);
                    state.done = true;
                    return;
                }
                None => {
                    duckdb_data_chunk_set_size(output, 0);
                    state.done = true;
                    return;
                }
            },
        };

        let Some(rs) = page.result_set() else {
            duckdb_data_chunk_set_size(output, 0);
            state.done = true;
            return;
        };
        let rows = rs.rows();
        // Athena returns the column header as the first row of the first page.
        let rows_slice: &[Row] = if was_first && !rows.is_empty() {
            &rows[1..]
        } else {
            rows
        };
        let rows_owned: Vec<ResultRow> = rows_slice.iter().map(datum_row_to_result_row).collect();
        if let Err(e) = rows_to_duckdb_data_chunk(&rows_owned, &state.col_types, output) {
            let msg = CString::new(e).unwrap_or_default();
            duckdb_function_set_error(info, msg.as_ptr());
            duckdb_data_chunk_set_size(output, 0);
            state.done = true;
        }
    }
}

/// Converts one `GetQueryResults` row into the shape both paths write from.
fn datum_row_to_result_row(row: &Row) -> ResultRow {
    row.data()
        .iter()
        .map(|d| d.var_char_value().map(str::to_owned))
        .collect()
}

/// Value to write for one cell. `None` means SQL NULL, including trailing cells
/// in ragged rows.
fn result_row_cell(row: &ResultRow, col_idx: usize) -> Option<&str> {
    row.get(col_idx).and_then(|c| c.as_deref())
}

pub fn rows_to_duckdb_data_chunk(
    rows: &[ResultRow],
    col_types: &[ColType],
    chunk: duckdb_data_chunk,
) -> Result<(), String> {
    let result_size = rows.len();
    let chunk_col_count =
        unsafe { libduckdb_sys::duckdb_data_chunk_get_column_count(chunk) } as usize;

    // DuckDB data chunk vectors hold at most STANDARD_VECTOR_SIZE rows. Both
    // paths hand over at most that many (a `GetQueryResults` page caps at 1000),
    // but fail loud rather than write out of bounds if that ever changes.
    let capacity = unsafe { libduckdb_sys::duckdb_vector_size() } as usize;
    if result_size > capacity {
        return Err(format!(
            "Athena result page has {result_size} rows, exceeding DuckDB's vector capacity of {capacity}"
        ));
    }

    // Write every chunk column for every row. Bounded by both the resolved
    // column types and the DuckDB chunk column counts so we never write past the
    // chunk boundary if they diverge (e.g. unsupported column types skipped).
    let col_count = col_types.len().min(chunk_col_count);
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, &col_type) in col_types.iter().take(col_count).enumerate() {
            unsafe {
                populate_column(
                    result_row_cell(row, col_idx),
                    col_type,
                    chunk,
                    row_idx,
                    col_idx,
                )
            };
        }
    }

    unsafe { duckdb_data_chunk_set_size(chunk, result_size as idx_t) };

    Ok(())
}

/// First delay before re-polling query state; grows via `next_poll_delay`.
const POLL_INITIAL: Duration = Duration::from_millis(250);
/// Ceiling for the poll backoff, so long queries still poll at least this often.
const POLL_MAX: Duration = Duration::from_secs(5);
/// Default backstop so a query that never resolves can't hang DuckDB forever.
/// Athena's own DML timeout (default 30 min) normally fails a stuck query first;
/// this only catches genuine indefinite hangs. Override per scan with
/// `timeout_seconds=`, either to wait longer than a workgroup's raised limit or
/// to fail fast.
const DEFAULT_POLL_WAIT: Duration = Duration::from_secs(60 * 60);

/// Athena will not reuse a result older than 7 days.
const MAX_RESULT_REUSE_MINUTES: i32 = 7 * 24 * 60;

/// How long to sleep before the next state check: the backoff, but never past
/// the deadline. Without the clamp a 5s backoff can overshoot a short timeout by
/// almost 5s, so `timeout_seconds` would not bound the wait it promises to.
/// Athena's own explanation for a failed or cancelled query, when it gave one.
fn reason(resp: &GetQueryExecutionOutput) -> Option<&str> {
    resp.query_execution()
        .and_then(|qe| qe.status())
        .and_then(|s| s.state_change_reason())
}

/// The error text for a query Athena failed or cancelled.
///
/// `StateChangeReason` is where the actual cause lives -- the syntax error, the
/// denied permission, the exhausted resource. Without it the message is just
/// `Query Failed: <id>`, which sends the reader to the Athena console to find
/// out what this process already knew.
fn failure_message(id: &str, state: &str, reason: Option<&str>) -> String {
    match reason {
        Some(r) => format!("Athena query {id} {state}: {r}"),
        None => format!("Athena query {id} {state} (Athena gave no reason)"),
    }
}

/// The error text for a query that outlived `timeout_seconds=`.
///
/// A failed stop is not a footnote: the timeout promises to stop the query, and
/// when the stop is refused Athena keeps scanning and billing while DuckDB has
/// already given up. The commonest cause is an IAM policy built from the
/// documented permissions before `athena:StopQueryExecution` was among them, so
/// the message names it rather than leaving a silent charge to be discovered on
/// the bill.
///
/// The recovery command carries `--region` for the same reason `describe_target`
/// always names the region: with `region=` the query runs where the scan pointed
/// it, not where the CLI defaults, and a stop sent elsewhere cancels nothing.
fn timeout_message(
    id: &str,
    state: &str,
    secs: u64,
    region: Option<&str>,
    stop_err: Option<&str>,
) -> String {
    let base = format!("Athena query {id} still {state} after {secs}s; aborting");
    let Some(e) = stop_err else {
        return base;
    };
    // No region resolved means no correct command to print: naming a region the
    // scan did not use would be worse than telling the reader to supply one.
    let region_flag = match region {
        Some(r) => format!(" --region {r}"),
        None => " --region <the region the scan used>".to_string(),
    };
    format!(
        "{base}. Stopping it failed, so it may still be running and billing \
         -- stop it with `aws athena stop-query-execution --query-execution-id {id}{region_flag}` \
         (needs athena:StopQueryExecution): {e}"
    )
}

fn sleep_before_next_poll(
    poll_delay: Duration,
    elapsed: Duration,
    timeout: Duration,
    until_report: Duration,
) -> Duration {
    poll_delay
        .min(timeout.saturating_sub(elapsed))
        .min(until_report)
}

/// Next poll delay: exponential backoff doubling up to `cap`.
/// How long a query may run before the poll loop says anything, and how often
/// it repeats itself afterwards.
///
/// Three seconds rather than five because of where the backoff actually lands:
/// polls happen at 0.25, 0.75, 1.75, 3.75, 7.75s, so a five-second threshold is
/// not observed until 7.75s -- a six-second query, long enough to look hung,
/// would report nothing at all.
const PROGRESS_AFTER: Duration = Duration::from_secs(3);
const PROGRESS_EVERY: Duration = Duration::from_secs(5);

/// A heartbeat for the poll loop, or `None` while it should stay quiet.
///
/// DuckDB's progress bar cannot move for a table function loaded through the C
/// API: the whole surface is bind/init/function plus projection pushdown, with
/// no progress callback (C++ table functions have `table_scan_progress`; this
/// one does not). So the bar renders at 0% for the entire wait, and a long
/// query is indistinguishable from a hang. Printing our own line is the only
/// feedback available -- it scrolls the bar, which costs nothing, because a bar
/// pinned at 0% was telling the reader nothing.
///
/// Athena publishes no byte count while it plans -- as an absent field over the
/// API, but as a literal `0` through the SDK on an in-flight execution -- and
/// fills it in once execution starts (measured on 52a65dc8: `RUNNING` with
/// nothing at 0.7s, `RUNNING` with 1,243,974,270 bytes at 1.4s). Both forms mean
/// "not yet", so both are omitted: a heartbeat reading `0 bytes scanned` while
/// Athena chews through a gigabyte is worse than one that says nothing about
/// bytes at all. Once real, the figure doubles as a spend meter.
/// `since_last` is `None` until something has actually been reported: the first
/// heartbeat is gated on `PROGRESS_AFTER` alone, and only repeats wait for
/// `PROGRESS_EVERY`. Folding the two together would push the first line to the
/// 7.75s poll and undo the point of the lower threshold.
fn progress_line(
    elapsed: Duration,
    since_last: Option<Duration>,
    state: &str,
    bytes: Option<i64>,
) -> Option<String> {
    if elapsed < PROGRESS_AFTER {
        return None;
    }
    if since_last.is_some_and(|d| d < PROGRESS_EVERY) {
        return None;
    }
    let scanned = match bytes {
        Some(b) if b > 0 => format!(", {} scanned", format_bytes(b)),
        _ => String::new(),
    };
    Some(format!(
        "Athena query {state}, {}s elapsed{scanned}",
        elapsed.as_secs()
    ))
}

fn next_poll_delay(current: Duration, cap: Duration) -> Duration {
    (current * 2).min(cap)
}

fn status(resp: &GetQueryExecutionOutput) -> Option<QueryExecutionState> {
    resp.query_execution()
        .and_then(|qe| qe.status())
        .and_then(|s| s.state())
        .cloned()
}

fn scanned_bytes(resp: &GetQueryExecutionOutput) -> Option<i64> {
    resp.query_execution()
        .and_then(|qe| qe.statistics())
        .and_then(|s| s.data_scanned_in_bytes())
}

fn print_query_stats(resp: &GetQueryExecutionOutput) {
    let stats = resp.query_execution().and_then(|qe| qe.statistics());
    let Some(s) = stats else { return };

    if let Some(queue_ms) = s.query_queue_time_in_millis() {
        eprintln!("Time in queue: {} ms", queue_ms);
    }
    if let Some(run_ms) = s.engine_execution_time_in_millis() {
        eprintln!("Run time: {} ms", run_ms);
    }
    if let Some(bytes) = s.data_scanned_in_bytes() {
        eprintln!("Data scanned: {}", format_bytes(bytes));
    }
}

fn format_bytes(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.2} MB", b / MB)
    } else if b >= KB {
        format!("{:.2} KB", b / KB)
    } else {
        format!("{} bytes", bytes)
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified_table(database: &str, tablename: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(database),
        quote_identifier(tablename)
    )
}

/// Blanks out the contents of single-quoted string literals (keeping the
/// quotes themselves and every other byte in place), so a keyword scan over
/// the result only sees actual SQL syntax, not literal text. A doubled `''`
/// (SQL's escaped quote) stays inside the literal rather than closing it.
fn mask_string_literals(predicate: &str) -> String {
    let mut masked = String::with_capacity(predicate.len());
    let mut chars = predicate.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if !in_string {
            masked.push(c);
            if c == '\'' {
                in_string = true;
            }
            continue;
        }
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                masked.push(' ');
                masked.push(' ');
                chars.next();
            } else {
                masked.push('\'');
                in_string = false;
            }
        } else {
            masked.push(' ');
        }
    }
    masked
}

fn validate_predicate(predicate: &str) -> Result<String, String> {
    let predicate = predicate.trim();
    if predicate.is_empty() {
        return Err("predicate must not be empty".to_string());
    }
    if predicate.contains('\0') {
        return Err("predicate must not contain NUL bytes".to_string());
    }
    if predicate.contains(';') {
        return Err("predicate must be a single WHERE expression without semicolons".to_string());
    }
    if predicate.contains("--") || predicate.contains("/*") || predicate.contains("*/") {
        return Err("predicate must not contain SQL comments".to_string());
    }

    // Keyword scan only looks at real SQL syntax: string-literal contents are
    // blanked first so a value like `name = 'DROP everything'` isn't mistaken
    // for a DROP statement.
    let uppercase = mask_string_literals(predicate).to_ascii_uppercase();
    for keyword in [
        " SELECT ",
        " INSERT ",
        " UPDATE ",
        " DELETE ",
        " CREATE ",
        " DROP ",
        " ALTER ",
        " TRUNCATE ",
        " UNLOAD ",
        " MSCK ",
        " REPAIR ",
    ] {
        if format!(" {uppercase} ").contains(keyword) {
            return Err(
                "predicate must be a WHERE expression, not a full SQL statement".to_string(),
            );
        }
    }

    Ok(predicate.to_owned())
}

/// Words that can appear bare in a `WHERE` expression without naming a column:
/// operators, literals, and the type names used by casts and typed literals.
/// Deliberately permissive — a word listed here is simply not checked, so the
/// worst case is that a mistyped column slips through to Athena, which is where
/// it would have been caught before this validation existed.
const PREDICATE_WORDS: &[&str] = &[
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "LIKE",
    "BETWEEN",
    "TRUE",
    "FALSE",
    "ESCAPE",
    "CAST",
    "AS",
    "DISTINCT",
    "ALL",
    "ANY",
    "SOME",
    "EXISTS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    // Grammar words that appear inside expressions: EXTRACT(YEAR FROM ts),
    // INTERVAL '1' DAY TO SECOND, ts AT TIME ZONE 'UTC', DOUBLE PRECISION.
    "FROM",
    "TO",
    "AT",
    "PRECISION",
    "LOCAL",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "INTERVAL",
    "ZONE",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "VARCHAR",
    "CHAR",
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "INT",
    "BIGINT",
    "REAL",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "ARRAY",
    "MAP",
    "ROW",
    "JSON",
    "VARBINARY",
    "UUID",
];

/// Rejects a `predicate=` that references a column the table does not have.
///
/// Athena would reject it too, but only after `StartQueryExecution`, as an
/// opaque `COLUMN_NOT_FOUND` on a query the user cannot see. Catching it at bind
/// names the offending identifier and the columns that do exist.
///
/// This is a scan, not a parser: string literals are blanked first, a word
/// followed by `(` is treated as a function name, and anything in
/// `PREDICATE_WORDS` is skipped. Everything else must be a known column.
fn validate_predicate_columns(predicate: &str, columns: &[String]) -> Result<(), String> {
    let masked = mask_string_literals(predicate);
    let known: Vec<String> = columns.iter().map(|c| c.to_ascii_lowercase()).collect();
    let bytes: Vec<char> = masked.chars().collect();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];

        // Quoted identifier: "col name", with "" as an escaped quote.
        if c == '"' {
            let mut name = String::new();
            i += 1;
            while i < bytes.len() {
                if bytes[i] == '"' {
                    if bytes.get(i + 1) == Some(&'"') {
                        name.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                name.push(bytes[i]);
                i += 1;
            }
            check_identifier(&name, &known, columns)?;
            continue;
        }

        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == '_') {
                i += 1;
            }
            let word: String = bytes[start..i].iter().collect();

            // A word followed by "(" is a function call, not a column.
            let mut j = i;
            while j < bytes.len() && bytes[j].is_whitespace() {
                j += 1;
            }
            let is_call = bytes.get(j) == Some(&'(');
            let is_word = PREDICATE_WORDS.contains(&word.to_ascii_uppercase().as_str());
            // A listed word standing directly before a comparison operator is an
            // operand, not grammar, so it names a column: `year = 2024` is a
            // column reference even though YEAR is also an EXTRACT unit. Without
            // this, every keyword-shaped column name skips validation entirely.
            let compared = matches!(bytes.get(j), Some('=' | '<' | '>' | '!'));

            if !is_call && (!is_word || compared) {
                check_identifier(&word, &known, columns)?;
            }
            continue;
        }

        // Skip numeric literals whole so 2024 or 1e6 never looks like an identifier.
        if c.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == '.') {
                i += 1;
            }
            continue;
        }

        i += 1;
    }
    Ok(())
}

fn check_identifier(name: &str, known: &[String], columns: &[String]) -> Result<(), String> {
    if known.contains(&name.to_ascii_lowercase()) {
        return Ok(());
    }
    Err(format!(
        "predicate references unknown column \"{name}\"; this table has: {}",
        columns.join(", ")
    ))
}

/// Builds the Athena `SELECT` list from the columns DuckDB actually projected.
///
/// `indices` are positions into `columns` (the bind-time output schema), in the
/// order DuckDB wants them. When empty (e.g. `COUNT(*)`), selects the constant
/// `1` so Athena scans no column data but still returns one row per source row.
///
/// Complex columns are selected as `CAST(col AS JSON)`: Athena's default
/// rendering of an array, map or struct is ambiguous (`[a,b, c]` for
/// `array['a,b', 'c']`), while its JSON rendering escapes properly and keeps
/// struct field names.
fn projected_select_list(columns: &[String], col_types: &[ColType], indices: &[usize]) -> String {
    let cols: Vec<String> = indices
        .iter()
        .filter_map(|&i| columns.get(i).map(|c| (i, c)))
        .map(|(i, c)| {
            let quoted = quote_identifier(c);
            match col_types.get(i) {
                Some(ColType::Json) => format!("CAST({quoted} AS JSON)"),
                _ => quoted,
            }
        })
        .collect();
    if cols.is_empty() {
        "1".to_string()
    } else {
        cols.join(", ")
    }
}

/// A scan deadline in seconds. Zero or negative would either busy-wait or abort
/// instantly, and both are more likely typos than intent.
fn parse_timeout_seconds(seconds: i32) -> Result<Duration, String> {
    if seconds > 0 {
        Ok(Duration::from_secs(seconds as u64))
    } else {
        Err(format!("timeout_seconds must be > 0, got {seconds}"))
    }
}

/// Result-reuse window in minutes. Athena rejects anything past 7 days, so catch
/// it here with a message that explains the limit rather than passing it on.
fn parse_reuse_minutes(minutes: i32) -> Result<i32, String> {
    if minutes > 0 && minutes <= MAX_RESULT_REUSE_MINUTES {
        Ok(minutes)
    } else {
        Err(format!(
            "result_reuse_minutes must be between 1 and {MAX_RESULT_REUSE_MINUTES} \
             (Athena's 7-day limit), got {minutes}"
        ))
    }
}

/// Normalizes an optional string argument: `None` means the parameter was
/// omitted; an explicitly provided empty/whitespace value is an error rather
/// than a silent fallback to a default.
fn parse_optional_arg(name: &str, raw: Option<&str>) -> Result<Option<String>, String> {
    match raw {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Err(format!(
            "{name} must not be empty; omit it to use the default"
        )),
        Some(s) => Ok(Some(s.trim().to_owned())),
    }
}

fn build_athena_query(
    select_list: &str,
    database: &str,
    tablename: &str,
    predicate: Option<&str>,
    maxrows: i32,
) -> String {
    let mut query = format!(
        "SELECT {} FROM {}",
        select_list,
        qualified_table(database, tablename)
    );
    if let Some(predicate) = predicate {
        query.push_str(" WHERE ");
        query.push_str(predicate);
    }
    if maxrows > 0 {
        query.push_str(&format!(" LIMIT {maxrows}"));
    }
    query
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena_bind(bind_info: duckdb_bind_info) {
    unsafe {
        let bi = BindInfo::new(bind_info);
        if bi.parameter_count() < 1 {
            bi.set_error("athena_scan requires at least 1 parameter: tablename");
            return;
        }

        let tablename = match bi.get_parameter_value(0).as_str() {
            Ok(s) => s,
            Err(e) => {
                bi.set_error(&e.to_string());
                return;
            }
        };
        // Reads a named string parameter as Option: None when omitted/null,
        // Some(value) otherwise. Surfaces a wrong-type error via bind.
        let named_str = |name: &str| -> Result<Option<String>, ()> {
            let val = bi.get_named_parameter_value(name);
            if val.is_null() {
                Ok(None)
            } else {
                match val.as_str() {
                    Ok(s) => Ok(Some(s)),
                    Err(e) => {
                        bi.set_error(&e.to_string());
                        Err(())
                    }
                }
            }
        };

        // Optional explicit S3 results location. When omitted, no client
        // ResultConfiguration is sent and Athena applies the workgroup's own
        // config (location, encryption, ACL, managed results). An explicitly
        // empty value is an error, not a silent fallback.
        let Ok(output_location_raw) = named_str("output_location") else {
            return;
        };
        let output_location =
            match parse_optional_arg("output_location", output_location_raw.as_deref()) {
                Ok(loc) => loc,
                Err(e) => {
                    bi.set_error(&e);
                    return;
                }
            };

        // Workgroup defaults to `primary` when omitted; an explicitly empty
        // value is an error.
        let Ok(workgroup_raw) = named_str("workgroup") else {
            return;
        };
        let workgroup = match parse_optional_arg("workgroup", workgroup_raw.as_deref()) {
            Ok(wg) => wg.unwrap_or_else(|| "primary".to_owned()),
            Err(e) => {
                bi.set_error(&e);
                return;
            }
        };
        // maxrows > 0 caps the Athena query with `LIMIT n`. Unset (null) or any
        // value <= 0 (e.g. maxrows=-1) means no limit: return all rows so
        // aggregates and joins over athena_scan see the full table.
        let maxrows_val = bi.get_named_parameter_value("maxrows");
        let maxrows = if maxrows_val.is_null() {
            0
        } else {
            maxrows_val.as_i32()
        };

        // Positive integer, in seconds, or the default ceiling. A query still
        // running when this elapses is stopped rather than abandoned, so the
        // parameter bounds cost as well as waiting.
        let timeout_val = bi.get_named_parameter_value("timeout_seconds");
        let timeout = if timeout_val.is_null() {
            DEFAULT_POLL_WAIT
        } else {
            match parse_timeout_seconds(timeout_val.as_i32()) {
                Ok(d) => d,
                Err(e) => {
                    bi.set_error(&e);
                    return;
                }
            }
        };

        // Athena caps result reuse at 7 days; anything longer is a user error
        // rather than something to silently clamp.
        let reuse_val = bi.get_named_parameter_value("result_reuse_minutes");
        let result_reuse_minutes = if reuse_val.is_null() {
            None
        } else {
            match parse_reuse_minutes(reuse_val.as_i32()) {
                Ok(m) => Some(m),
                Err(e) => {
                    bi.set_error(&e);
                    return;
                }
            }
        };
        // Database defaults to `default` when omitted; an explicitly empty or
        // wrong-type value is an error, not a silent fallback.
        let Ok(database_raw) = named_str("database") else {
            return;
        };
        let database = match parse_optional_arg("database", database_raw.as_deref()) {
            Ok(db) => db.unwrap_or_else(|| "default".to_owned()),
            Err(e) => {
                bi.set_error(&e);
                return;
            }
        };
        // Overrides the region the AWS config chain would pick, so one session
        // can read tables in more than one region.
        let Ok(region_raw) = named_str("region") else {
            return;
        };
        let region = match parse_optional_arg("region", region_raw.as_deref()) {
            Ok(v) => v,
            Err(e) => {
                bi.set_error(&e);
                return;
            }
        };

        let predicate = {
            let predicate_val = bi.get_named_parameter_value("predicate");
            if predicate_val.is_null() {
                None
            } else {
                match predicate_val.as_str() {
                    Ok(s) if s.trim().is_empty() => None,
                    Ok(s) => match validate_predicate(&s) {
                        Ok(predicate) => Some(predicate),
                        Err(e) => {
                            bi.set_error(&e);
                            return;
                        }
                    },
                    Err(e) => {
                        bi.set_error(&e.to_string());
                        return;
                    }
                }
            }
        };

        let config = load_aws_config(region.as_deref());
        let client = GlueClient::new(&config);

        let table_result = crate::RUNTIME.block_on(
            client
                .get_table()
                .database_name(database.clone())
                .name(tablename.clone())
                .send(),
        );

        let mut columns: Vec<String> = Vec::new();
        let mut col_types: Vec<ColType> = Vec::new();
        // Resolves a Glue column type, registers the DuckDB output column with
        // the right logical type (native DECIMAL(width, scale) when applicable,
        // else the plain TypeId), and returns the resolved type. Unmappable types
        // fall back to Varchar.
        let mut register = |name: &str, type_str: &str| {
            let col_type = map_type(type_str).unwrap_or(ColType::Simple(TypeId::Varchar));
            match col_type {
                ColType::Decimal { width, scale } => {
                    bi.add_result_column_with_type(name, &LogicalType::decimal(width, scale));
                }
                // JSON text is a VARCHAR column to DuckDB; only the Athena-side
                // SELECT differs.
                ColType::Json => {
                    bi.add_result_column(name, TypeId::Varchar);
                }
                ColType::Simple(type_id) => {
                    bi.add_result_column(name, type_id);
                }
            }
            columns.push(name.to_string());
            col_types.push(col_type);
        };
        match table_result {
            Ok(resp) => {
                if let Some(table) = resp.table() {
                    if let Some(sd) = table.storage_descriptor() {
                        for column in sd.columns() {
                            register(column.name(), column.r#type().unwrap_or("varchar"));
                        }
                    }
                    // Partition columns come after data columns in Athena's SELECT * results.
                    // Registering them here keeps the DuckDB chunk column count in sync.
                    for column in table.partition_keys() {
                        register(column.name(), column.r#type().unwrap_or("varchar"));
                    }
                }
            }
            Err(err) => {
                // Glue's own message is often just "Entity Not Found", which
                // says nothing about what was looked up. Name the table, the
                // database and the region, since a wrong region looks exactly
                // like a missing table.
                let where_ = describe_target(&database, &tablename, region.as_deref(), &config);
                bi.set_error(&format!("{}: {}", where_, err.into_service_error()));
                return;
            }
        }

        if columns.is_empty() {
            bi.set_error(&format!(
                "{} has no columns in the Glue catalog",
                describe_target(&database, &tablename, region.as_deref(), &config)
            ));
            return;
        }

        // Column references can only be checked once the Glue schema is known,
        // so this runs here rather than beside the rest of predicate validation.
        if let Some(predicate) = predicate.as_deref() {
            if let Err(e) = validate_predicate_columns(predicate, &columns) {
                bi.set_error(&e);
                return;
            }
        }

        FfiBindData::<ScanBindData>::set(
            bind_info,
            ScanBindData {
                tablename,
                database,
                output_location,
                workgroup,
                limit: maxrows,
                predicate,
                config: config.clone(),
                timeout,
                result_reuse_minutes,
                columns,
                col_types,
            },
        );
    }
}

/// Names what a failed lookup was actually looking for. A wrong region and a
/// missing table produce the same Glue error, so the region is always shown.
fn describe_target(
    database: &str,
    tablename: &str,
    region: Option<&str>,
    config: &aws_config::SdkConfig,
) -> String {
    let region = region
        .map(str::to_owned)
        .or_else(|| config.region().map(|r| r.to_string()))
        .unwrap_or_else(|| "<no region configured>".to_string());
    format!("table \"{database}\".\"{tablename}\" in region {region}")
}

/// Resolves AWS config for one scan. Bind loads it and init reuses it through
/// the bind data, so credentials and region resolve once per scan rather than
/// twice — but never across scans: a long-lived process that changes
/// `AWS_PROFILE` between queries must see the new profile, not a cached one.
fn load_aws_config(region: Option<&str>) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(Region::new(region.to_owned()));
    }
    crate::RUNTIME.block_on(loader.load())
}

/// S3 location of a finished query's result file, or `None` when the execution
/// exposes none (Athena-managed query results).
fn result_output_location(resp: &GetQueryExecutionOutput) -> Option<&str> {
    resp.query_execution()
        .and_then(|qe| qe.result_configuration())
        .and_then(|rc| rc.output_location())
        .filter(|loc| !loc.is_empty())
}

/// Opens the result CSV for streaming. `Ok(None)` means there is no S3 location
/// to read, which is a fallback rather than a failure.
fn open_result_csv(
    config: &aws_config::SdkConfig,
    resp: &GetQueryExecutionOutput,
) -> Result<Option<CsvRowStream>, String> {
    let Some(location) = result_output_location(resp) else {
        return Ok(None);
    };
    let (bucket, key) = parse_s3_uri(location)?;
    let s3 = aws_sdk_s3::Client::new(config);
    let object = crate::RUNTIME
        .block_on(s3.get_object().bucket(bucket).key(key).send())
        .map_err(|e| format!("reading {location}: {e}"))?;
    let content_length = object.content_length().and_then(|n| u64::try_from(n).ok());
    Ok(Some(CsvRowStream::new(object.body, content_length)))
}

/// The `GetQueryResults` paging fallback: one page per scan call.
fn paged_mode(client: &AthenaClient, query_execution_id: &str) -> ScanMode {
    let mut paginator = client
        .get_query_results()
        .query_execution_id(query_execution_id.to_owned())
        .into_paginator()
        .send();
    let next_page: PageFetcher = Box::new(move || {
        crate::RUNTIME
            .block_on(paginator.next())
            .map(|r| r.map_err(|e| e.to_string()))
    });
    ScanMode::Pages {
        next_page,
        first_page: true,
    }
}

/// # Safety
#[no_mangle]
unsafe extern "C" fn read_athena_init(info: duckdb_init_info) {
    unsafe {
        let bind_data = match FfiBindData::<ScanBindData>::get_from_init(info) {
            Some(d) => d,
            None => return,
        };

        let tablename = bind_data.tablename.clone();
        let database = bind_data.database.clone();
        let output_location = bind_data.output_location.clone();
        let workgroup = bind_data.workgroup.clone();
        let maxrows = bind_data.limit;
        let predicate = bind_data.predicate.as_deref();
        let timeout = bind_data.timeout;
        let result_reuse_minutes = bind_data.result_reuse_minutes;

        // Projection pushdown: DuckDB tells us which output columns it actually
        // needs, in order. Select only those from Athena so columnar formats
        // scan fewer bytes. Falls back to all columns when nothing is projected.
        let init = InitInfo::new(info);
        let projected: Vec<usize> = (0..init.projected_column_count())
            .map(|i| init.projected_column_index(i))
            .collect();
        let select_list =
            projected_select_list(&bind_data.columns, &bind_data.col_types, &projected);
        // Resolved types of exactly the projected columns, in projection order,
        // so the scan writes each Athena result column into its matching chunk
        // vector with the physical layout registered at bind. Empty for
        // `COUNT(*)` (no columns projected), where the chunk has no vectors.
        let projected_types: Vec<ColType> = projected
            .iter()
            .filter_map(|&i| bind_data.col_types.get(i).copied())
            .collect();

        // Loaded at bind for this scan; reusing it here avoids resolving
        // credentials a second time without outliving the scan.
        let config = bind_data.config.clone();
        let client = AthenaClient::new(&config);

        let query = build_athena_query(&select_list, &database, &tablename, predicate, maxrows);

        // Only send a client ResultConfiguration when an explicit
        // output_location was given. Otherwise Athena applies the workgroup's
        // own result configuration (location, encryption, ACL, managed results).
        let mut request = client
            .start_query_execution()
            .query_string(query)
            .work_group(workgroup.clone());
        if let Some(location) = output_location {
            request = request.result_configuration(
                ResultConfiguration::builder()
                    .output_location(location)
                    .build(),
            );
        }
        // Opt-in: Athena returns a previous identical query's result if it is
        // younger than this, scanning no data and charging nothing.
        if let Some(minutes) = result_reuse_minutes {
            request = request.result_reuse_configuration(
                ResultReuseConfiguration::builder()
                    .result_reuse_by_age_configuration(
                        ResultReuseByAgeConfiguration::builder()
                            .enabled(true)
                            .max_age_in_minutes(minutes)
                            .build(),
                    )
                    .build(),
            );
        }

        let start_resp = crate::RUNTIME.block_on(request.send());

        let query_execution_id = match start_resp {
            Ok(r) => match r.query_execution_id().filter(|id| !id.is_empty()) {
                Some(id) => id.to_string(),
                // Polling "" would loop against a nonexistent execution until
                // the timeout and then blame the timeout. Athena should never
                // do this, but unwrap_or_default() turned "should never" into
                // an hour-long wait for a wrong error.
                None => {
                    let msg = CString::new(
                        "Athena accepted the query but returned no execution id".to_string(),
                    )
                    .unwrap_or_default();
                    libduckdb_sys::duckdb_init_set_error(info, msg.as_ptr());
                    return;
                }
            },
            Err(e) => {
                // Include the workgroup: the usual cause is a workgroup with no
                // result configuration and no output_location given, and the
                // raw message does not say which workgroup was used.
                let msg = CString::new(format!(
                    "starting Athena query in workgroup \"{workgroup}\": {e}"
                ))
                .unwrap_or_default();
                libduckdb_sys::duckdb_init_set_error(info, msg.as_ptr());
                return;
            }
        };

        eprintln!("Running Athena query, execution id: {query_execution_id}");

        let poll_start = Instant::now();
        let mut last_report: Option<Instant> = None;
        let mut poll_delay = POLL_INITIAL;
        loop {
            let get_resp = crate::RUNTIME.block_on(
                client
                    .get_query_execution()
                    .query_execution_id(query_execution_id.clone())
                    .send(),
            );

            let resp = match get_resp {
                Ok(r) => r,
                Err(e) => {
                    let msg = CString::new(e.to_string()).unwrap_or_default();
                    libduckdb_sys::duckdb_init_set_error(info, msg.as_ptr());
                    return;
                }
            };

            let state = match status(&resp) {
                Some(s) => s,
                None => {
                    let msg = CString::new("Could not get query state").unwrap_or_default();
                    libduckdb_sys::duckdb_init_set_error(info, msg.as_ptr());
                    return;
                }
            };

            match state {
                Queued | Running => {
                    if poll_start.elapsed() >= timeout {
                        // Stop the query so Athena doesn't keep scanning (and
                        // billing) after we abandon it.
                        let stop = crate::RUNTIME.block_on(
                            client
                                .stop_query_execution()
                                .query_execution_id(query_execution_id.clone())
                                .send(),
                        );
                        let region = config.region().map(|r| r.to_string());
                        let msg = timeout_message(
                            &query_execution_id,
                            &format!("{state:?}"),
                            timeout.as_secs(),
                            region.as_deref(),
                            stop.err().map(|e| e.to_string()).as_deref(),
                        );
                        let c_msg = CString::new(msg).unwrap_or_default();
                        libduckdb_sys::duckdb_init_set_error(info, c_msg.as_ptr());
                        return;
                    }
                    // The one piece of feedback available: DuckDB's own
                    // progress bar is stuck at 0% for the whole wait (see
                    // progress_line), so without this a slow query looks hung.
                    if let Some(line) = progress_line(
                        poll_start.elapsed(),
                        last_report.map(|t: Instant| t.elapsed()),
                        &format!("{state:?}").to_uppercase(),
                        scanned_bytes(&resp),
                    ) {
                        eprintln!("{line}");
                        last_report = Some(Instant::now());
                    }
                    // Wake for whichever comes first: the next poll, the
                    // deadline, or the next heartbeat. Without the last one the
                    // backoff decides the cadence -- the poll after the first
                    // report lands 4s later, is too early to report, and the
                    // one after that is 9s later, so a "every five seconds"
                    // heartbeat goes quiet for nine.
                    let until_report = match last_report {
                        Some(t) => PROGRESS_EVERY.saturating_sub(t.elapsed()),
                        None => PROGRESS_AFTER.saturating_sub(poll_start.elapsed()),
                    };
                    thread::sleep(sleep_before_next_poll(
                        poll_delay,
                        poll_start.elapsed(),
                        timeout,
                        until_report,
                    ));
                    poll_delay = next_poll_delay(poll_delay, POLL_MAX);
                }
                Cancelled | Failed => {
                    let msg =
                        failure_message(&query_execution_id, &format!("{state:?}"), reason(&resp));
                    let c_msg = CString::new(msg).unwrap_or_default();
                    libduckdb_sys::duckdb_init_set_error(info, c_msg.as_ptr());
                    return;
                }
                _ => {
                    print_query_stats(&resp);

                    // Athena has already written the entire result set as one CSV
                    // object. Streaming it costs a single GetObject; paging the
                    // same rows through GetQueryResults costs one call per 1000
                    // rows (~135s for a million), so only fall back to paging when
                    // there is no readable S3 location.
                    let mode = match open_result_csv(&config, &resp) {
                        Ok(Some(rows)) => ScanMode::Csv {
                            rows: Box::new(rows),
                        },
                        Ok(None) => paged_mode(&client, &query_execution_id),
                        Err(e) => {
                            eprintln!("Falling back to GetQueryResults paging: {e}");
                            paged_mode(&client, &query_execution_id)
                        }
                    };

                    FfiInitData::<ScanInitData>::set(
                        info,
                        ScanInitData {
                            mode,
                            col_types: projected_types,
                            done: false,
                        },
                    );
                    break;
                }
            }
        }
    }
}

pub fn build_table_function_def() -> TableFunctionBuilder {
    TableFunctionBuilder::new("athena_scan")
        .param(TypeId::Varchar)
        .named_param("output_location", TypeId::Varchar)
        .named_param("workgroup", TypeId::Varchar)
        .named_param("maxrows", TypeId::Integer)
        .named_param("database", TypeId::Varchar)
        .named_param("predicate", TypeId::Varchar)
        .named_param("region", TypeId::Varchar)
        .named_param("timeout_seconds", TypeId::Integer)
        .named_param("result_reuse_minutes", TypeId::Integer)
        .projection_pushdown(true)
        .bind(read_athena_bind)
        .init(read_athena_init)
        .scan(read_athena)
}

#[cfg(test)]
mod tests {
    use super::{
        build_athena_query, datum_row_to_result_row, describe_target, failure_message,
        mask_string_literals, next_poll_delay, parse_optional_arg, parse_reuse_minutes,
        parse_timeout_seconds, progress_line, projected_select_list, qualified_table,
        result_output_location, result_row_cell, sleep_before_next_poll, timeout_message,
        validate_predicate, validate_predicate_columns, POLL_INITIAL, POLL_MAX,
    };
    use crate::types::ColType;
    use aws_sdk_athena::operation::get_query_execution::GetQueryExecutionOutput;
    use aws_sdk_athena::types::{Datum, QueryExecution, ResultConfiguration, Row};
    use quack_rs::types::TypeId;
    use std::time::Duration;

    #[test]
    fn next_poll_delay_doubles_then_caps() {
        assert_eq!(
            next_poll_delay(POLL_INITIAL, POLL_MAX),
            Duration::from_millis(500)
        );
        assert_eq!(next_poll_delay(Duration::from_secs(3), POLL_MAX), POLL_MAX); // 6s capped
        assert_eq!(next_poll_delay(POLL_MAX, POLL_MAX), POLL_MAX); // stays at cap
    }

    #[test]
    fn result_row_cell_null_fills_ragged_rows() {
        // A row with fewer cells than columns must null-fill the trailing
        // columns rather than leave them unwritten (stale from a prior chunk).
        let row = vec![Some("a".to_string())];
        assert_eq!(result_row_cell(&row, 0), Some("a"));
        assert_eq!(result_row_cell(&row, 1), None);
    }

    #[test]
    fn datum_row_keeps_nulls_distinct_from_values() {
        // A Datum with no var_char_value is Athena's SQL NULL, and must not
        // collapse into an empty string when the paging fallback converts it.
        let row = Row::builder()
            .data(Datum::builder().var_char_value("a").build())
            .data(Datum::builder().build())
            .data(Datum::builder().var_char_value("").build())
            .build();
        let converted = datum_row_to_result_row(&row);
        assert_eq!(result_row_cell(&converted, 0), Some("a"));
        assert_eq!(result_row_cell(&converted, 1), None);
        assert_eq!(result_row_cell(&converted, 2), Some(""));
    }

    /// A finished execution carrying the given result location, as Athena
    /// reports it from GetQueryExecution.
    fn execution_with_location(location: Option<&str>) -> GetQueryExecutionOutput {
        let mut rc = ResultConfiguration::builder();
        if let Some(loc) = location {
            rc = rc.output_location(loc);
        }
        GetQueryExecutionOutput::builder()
            .query_execution(
                QueryExecution::builder()
                    .result_configuration(rc.build())
                    .build(),
            )
            .build()
    }

    #[test]
    fn result_location_drives_the_s3_fast_path() {
        assert_eq!(
            result_output_location(&execution_with_location(Some("s3://bucket/key.csv"))),
            Some("s3://bucket/key.csv")
        );
    }

    #[test]
    fn missing_result_location_falls_back_to_paging() {
        // Athena-managed query results expose no S3 location; the scan must page
        // GetQueryResults rather than error or read nothing.
        assert_eq!(result_output_location(&execution_with_location(None)), None);
        assert_eq!(
            result_output_location(&execution_with_location(Some(""))),
            None
        );
        assert_eq!(
            result_output_location(&GetQueryExecutionOutput::builder().build()),
            None
        );
    }

    /// An SdkConfig with, or without, a resolved region. Built offline: the
    /// builder resolves nothing by itself.
    fn config_with_region(region: Option<&str>) -> aws_config::SdkConfig {
        let mut b = aws_config::SdkConfig::builder();
        if let Some(r) = region {
            b.set_region(Some(aws_config::Region::new(r.to_owned())));
        }
        b.build()
    }

    #[test]
    fn describe_target_prefers_the_explicit_region() {
        // The parameter beats the resolved config, because that is the region
        // the lookup actually used.
        assert_eq!(
            describe_target(
                "db",
                "t",
                Some("eu-west-1"),
                &config_with_region(Some("us-east-1"))
            ),
            "table \"db\".\"t\" in region eu-west-1"
        );
    }

    #[test]
    fn describe_target_falls_back_to_the_resolved_region() {
        assert_eq!(
            describe_target("db", "t", None, &config_with_region(Some("us-east-1"))),
            "table \"db\".\"t\" in region us-east-1"
        );
    }

    #[test]
    fn describe_target_says_so_when_no_region_resolved() {
        // Athena and Glue both fail confusingly without a region, so the message
        // has to distinguish "wrong region" from "no region at all".
        assert_eq!(
            describe_target("db", "t", None, &config_with_region(None)),
            "table \"db\".\"t\" in region <no region configured>"
        );
    }

    #[test]
    fn a_failed_query_carries_athenas_own_reason() {
        // Without StateChangeReason the error is "Query Failed: <id>", which
        // sends the reader to the Athena console to learn what this process
        // already had in hand.
        let with = failure_message(
            "abc",
            "FAILED",
            Some("SYNTAX_ERROR: line 1:8: Column 'x' cannot be resolved"),
        );
        assert!(with.contains("abc"), "{with}");
        assert!(with.contains("FAILED"), "{with}");
        assert!(with.contains("cannot be resolved"), "{with}");

        // And when Athena gives nothing, say that rather than implying silence
        // is the reason.
        let without = failure_message("abc", "CANCELLED", None);
        assert_eq!(
            without,
            "Athena query abc CANCELLED (Athena gave no reason)"
        );
    }

    #[test]
    fn a_refused_stop_is_reported_not_swallowed() {
        // timeout_seconds= promises to stop the query, not merely to stop
        // waiting for it. If the stop is refused -- an IAM policy without
        // athena:StopQueryExecution is the usual reason -- the query keeps
        // scanning and billing after DuckDB has given up, so the error has to
        // say so and name the missing permission.
        let quiet = timeout_message("abc", "Running", 60, Some("eu-west-1"), None);
        assert_eq!(quiet, "Athena query abc still Running after 60s; aborting");
        assert!(!quiet.contains("billing"));

        let noisy = timeout_message(
            "abc",
            "Running",
            60,
            Some("eu-west-1"),
            Some("AccessDeniedException"),
        );
        assert!(
            noisy.contains("may still be running and billing"),
            "{noisy}"
        );
        assert!(noisy.contains("athena:StopQueryExecution"), "{noisy}");
        assert!(noisy.contains("AccessDeniedException"), "{noisy}");
        assert!(noisy.contains("abc"), "{noisy}");
        // The command has to target the region the scan used, not whatever the
        // CLI defaults to -- a stop sent to the wrong region cancels nothing.
        assert!(noisy.contains("--region eu-west-1"), "{noisy}");

        // With no region resolved, ask for one rather than printing a command
        // that would silently target the wrong place.
        let vague = timeout_message("abc", "Running", 60, None, Some("boom"));
        assert!(
            vague.contains("--region <the region the scan used>"),
            "{vague}"
        );
    }

    #[test]
    fn a_short_query_says_nothing_extra() {
        // The heartbeat exists for waits long enough to look like a hang. A
        // two-second scan is not one, and a line per poll would be noise.
        assert_eq!(
            progress_line(Duration::from_secs(2), None, "RUNNING", None),
            None
        );
        // ... but the first poll past the threshold speaks, and with the real
        // backoff (0.25/0.75/1.75/3.75s) that is the poll at 3.75s. A
        // five-second threshold would not be observed until 7.75s, so a
        // six-second query -- exactly the kind that looks hung -- stayed silent.
        assert!(progress_line(Duration::from_millis(3750), None, "RUNNING", None).is_some());
    }

    #[test]
    fn a_long_query_reports_elapsed_and_spend() {
        // DuckDB's bar is pinned at 0% for the whole wait -- the C API has no
        // progress callback -- so this line is the only sign of life. Bytes go
        // in it because they are also the bill.
        let line = progress_line(
            Duration::from_secs(30),
            Some(Duration::from_secs(5)),
            "RUNNING",
            Some(1_243_974_270),
        )
        .expect("should speak after the threshold");
        assert!(line.contains("RUNNING"), "{line}");
        assert!(line.contains("30s elapsed"), "{line}");
        assert!(line.contains("1.16 GB scanned"), "{line}");
    }

    #[test]
    fn bytes_are_omitted_until_athena_publishes_them() {
        // Athena publishes no byte count while it plans, so the line has to
        // read correctly with nothing to report.
        let line = progress_line(
            Duration::from_secs(10),
            Some(Duration::from_secs(5)),
            "QUEUED",
            None,
        )
        .expect("should speak after the threshold");
        assert_eq!(line, "Athena query QUEUED, 10s elapsed");
        assert!(!line.contains("scanned"), "{line}");

        // A mid-flight zero means the same thing, and this is the form actually
        // observed: the SDK reported Some(0), not None, while the query was
        // still RUNNING. "0 bytes scanned" reads as a fact rather than an
        // absence, so it is suppressed too.
        let zero = progress_line(
            Duration::from_secs(10),
            Some(Duration::from_secs(5)),
            "RUNNING",
            Some(0),
        )
        .expect("should speak after the threshold");
        assert_eq!(zero, "Athena query RUNNING, 10s elapsed");
    }

    #[test]
    fn the_heartbeat_does_not_repeat_itself_every_poll() {
        // The backoff polls faster than the report interval near the start, so
        // without the since-last check one wait would print many lines.
        assert_eq!(
            progress_line(
                Duration::from_secs(30),
                Some(Duration::from_millis(800)),
                "RUNNING",
                Some(1)
            ),
            None
        );
    }

    #[test]
    fn polling_never_sleeps_past_the_deadline() {
        // The backoff caps at 5s, so an unclamped sleep can overshoot a short
        // timeout by nearly that much -- which would make timeout_seconds= fail
        // to bound the wait it promises to bound.
        let timeout = Duration::from_secs(1);
        let far = Duration::from_secs(3600);
        assert_eq!(
            sleep_before_next_poll(
                Duration::from_secs(5),
                Duration::from_millis(750),
                timeout,
                far
            ),
            Duration::from_millis(250)
        );
        // Deadline already passed: do not sleep at all.
        assert_eq!(
            sleep_before_next_poll(Duration::from_secs(5), Duration::from_secs(2), timeout, far),
            Duration::ZERO
        );
        // Far from every deadline the backoff is used unchanged.
        assert_eq!(
            sleep_before_next_poll(
                Duration::from_millis(250),
                Duration::ZERO,
                Duration::from_secs(60),
                far
            ),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn polling_wakes_up_in_time_for_the_next_heartbeat() {
        // The backoff, left alone, sets the reporting cadence: after the first
        // heartbeat at 3.75s the next poll is at 7.75s -- only 4s later, so it
        // reports nothing -- and the one after that is at 12.75s. A heartbeat
        // documented as "every five seconds" would go quiet for nine.
        //
        // So the sleep is also clamped to whatever is left of the heartbeat
        // interval: at 3.75s with 4s of backoff and 5s until the next report
        // due, wake at the report, not after it.
        assert_eq!(
            sleep_before_next_poll(
                Duration::from_secs(4),
                Duration::from_millis(3750),
                Duration::from_secs(3600),
                Duration::from_secs(5),
            ),
            Duration::from_secs(4),
            "backoff shorter than the report interval is left alone"
        );
        assert_eq!(
            sleep_before_next_poll(
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(3600),
                Duration::from_secs(2),
            ),
            Duration::from_secs(2),
            "a report due before the next poll pulls the wake-up in"
        );
    }

    #[test]
    fn timeout_seconds_must_be_positive() {
        assert_eq!(parse_timeout_seconds(30).unwrap(), Duration::from_secs(30));
        // 0 would abort instantly and a negative value cannot be a deadline;
        // both are typos rather than intent.
        assert!(parse_timeout_seconds(0).is_err());
        assert!(parse_timeout_seconds(-1).is_err());
    }

    #[test]
    fn reuse_minutes_bounded_by_athenas_seven_days() {
        assert_eq!(parse_reuse_minutes(60).unwrap(), 60);
        assert_eq!(parse_reuse_minutes(10080).unwrap(), 10080); // exactly 7 days
                                                                // Past the limit Athena rejects the query; fail at bind with a message
                                                                // that names the limit instead.
        let err = parse_reuse_minutes(10081).unwrap_err();
        assert!(err.contains("10080"), "{err}");
        assert!(parse_reuse_minutes(0).is_err());
    }

    #[test]
    fn parse_optional_arg_none_when_omitted() {
        assert_eq!(parse_optional_arg("output_location", None).unwrap(), None);
    }

    #[test]
    fn parse_optional_arg_trims_value() {
        assert_eq!(
            parse_optional_arg("workgroup", Some("  analytics  ")).unwrap(),
            Some("analytics".to_string())
        );
    }

    #[test]
    fn parse_optional_arg_rejects_explicit_empty() {
        assert!(parse_optional_arg("output_location", Some("")).is_err());
        assert!(parse_optional_arg("output_location", Some("   ")).is_err());
    }

    fn cols() -> Vec<String> {
        ["id", "name", "year"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn qualified_table_quotes_identifiers() {
        assert_eq!(
            qualified_table("analytics", "events"),
            "\"analytics\".\"events\""
        );
        assert_eq!(
            qualified_table("odd\"db", "odd\"table"),
            "\"odd\"\"db\".\"odd\"\"table\""
        );
    }

    /// Resolved types matching `cols()`: all simple, none complex.
    fn simple_types() -> Vec<ColType> {
        vec![ColType::Simple(TypeId::Varchar); 3]
    }

    #[test]
    fn build_query_includes_predicate_before_limit() {
        assert_eq!(
            build_athena_query("*", "analytics", "events", Some("year = 2024"), 100),
            "SELECT * FROM \"analytics\".\"events\" WHERE year = 2024 LIMIT 100"
        );
    }

    #[test]
    fn build_query_omits_limit_for_non_positive_limit() {
        assert_eq!(
            build_athena_query("*", "analytics", "events", Some("year = 2024"), 0),
            "SELECT * FROM \"analytics\".\"events\" WHERE year = 2024"
        );
    }

    #[test]
    fn build_query_uses_projected_select_list() {
        let select = projected_select_list(&cols(), &simple_types(), &[0, 2]);
        assert_eq!(
            build_athena_query(&select, "analytics", "events", None, 100),
            "SELECT \"id\", \"year\" FROM \"analytics\".\"events\" LIMIT 100"
        );
    }

    #[test]
    fn projected_select_list_casts_complex_columns_to_json() {
        // Athena's default rendering of an array/map/struct is ambiguous, so
        // complex columns are selected as JSON while the rest are untouched.
        let columns = vec!["id".to_string(), "tags".to_string()];
        let types = vec![ColType::Simple(TypeId::BigInt), ColType::Json];
        assert_eq!(
            projected_select_list(&columns, &types, &[0, 1]),
            "\"id\", CAST(\"tags\" AS JSON)"
        );
        // Quoting still applies inside the cast.
        let odd = vec!["od\"d".to_string()];
        assert_eq!(
            projected_select_list(&odd, &[ColType::Json], &[0]),
            "CAST(\"od\"\"d\" AS JSON)"
        );
    }

    #[test]
    fn projected_select_list_preserves_requested_order() {
        // DuckDB may request columns in a different order than the schema.
        assert_eq!(
            projected_select_list(&cols(), &simple_types(), &[2, 0]),
            "\"year\", \"id\""
        );
    }

    #[test]
    fn projected_select_list_quotes_identifiers() {
        let columns = vec!["od\"d".to_string()];
        assert_eq!(
            projected_select_list(&columns, &simple_types(), &[0]),
            "\"od\"\"d\""
        );
    }

    #[test]
    fn projected_select_list_empty_selects_constant() {
        // Defensive: DuckDB keeps a placeholder column even for COUNT(*), so it
        // never projects nothing -- but an empty list would be invalid SQL.
        assert_eq!(projected_select_list(&cols(), &simple_types(), &[]), "1");
    }

    #[test]
    fn projected_select_list_ignores_out_of_range_indices() {
        assert_eq!(
            projected_select_list(&cols(), &simple_types(), &[1, 99]),
            "\"name\""
        );
        // All out of range collapses to the safe constant.
        assert_eq!(projected_select_list(&cols(), &simple_types(), &[99]), "1");
    }

    #[test]
    fn predicate_columns_accepts_references_the_table_has() {
        let cols = cols();
        assert!(validate_predicate_columns("year = 2024 AND name IS NOT NULL", &cols).is_ok());
        // Athena lowercases unquoted identifiers, so matching is case-insensitive.
        assert!(validate_predicate_columns("YEAR > 2000 OR Name LIKE 'a%'", &cols).is_ok());
        // Quoted identifiers name columns too.
        assert!(validate_predicate_columns("\"year\" BETWEEN 2000 AND 2024", &cols).is_ok());
    }

    #[test]
    fn predicate_columns_rejects_a_column_the_table_lacks() {
        // The point of the check: a typo becomes a bind error naming the column
        // instead of an opaque Athena COLUMN_NOT_FOUND after the query starts.
        let err = validate_predicate_columns("yaer = 2024", &cols()).unwrap_err();
        assert!(err.contains("yaer"), "{err}");
        assert!(err.contains("year"), "should list the real columns: {err}");
        assert!(validate_predicate_columns("year = 2024 AND missing > 1", &cols()).is_err());
    }

    #[test]
    fn predicate_columns_ignores_words_inside_string_literals() {
        // 'New York' must not be read as a reference to a column named New.
        assert!(validate_predicate_columns("name = 'New York'", &cols()).is_ok());
        assert!(validate_predicate_columns("name = 'it''s year'", &cols()).is_ok());
    }

    #[test]
    fn predicate_columns_allows_functions_literals_and_types() {
        let cols = cols();
        // A word before "(" is a function name, not a column.
        assert!(validate_predicate_columns("lower(name) = 'x'", &cols).is_ok());
        assert!(validate_predicate_columns("year(  id ) = 2024", &cols).is_ok());
        // Typed literals, casts and keyword operands are not columns either.
        assert!(validate_predicate_columns("id > CAST('1' AS BIGINT)", &cols).is_ok());
        assert!(validate_predicate_columns("year >= DATE '2024-01-01'", &cols).is_ok());
        assert!(validate_predicate_columns("id IN (1, 2, 3) AND name IS NULL", &cols).is_ok());
        assert!(validate_predicate_columns("id = 1e6 OR id = 2.5", &cols).is_ok());
    }

    #[test]
    fn predicate_columns_allows_sql_grammar_inside_expressions() {
        // Regression: FROM inside EXTRACT was read as a column reference, so a
        // valid predicate was rejected at bind unless the table happened to have
        // a column named "from".
        let cols = vec!["event_time".to_string(), "id".to_string()];
        assert!(validate_predicate_columns("EXTRACT(YEAR FROM event_time) = 2024", &cols).is_ok());
        assert!(validate_predicate_columns("id > CAST(1 AS DOUBLE PRECISION)", &cols).is_ok());
        assert!(validate_predicate_columns(
            "event_time AT TIME ZONE 'UTC' > TIMESTAMP '2024-01-01 00:00:00'",
            &cols
        )
        .is_ok());
    }

    #[test]
    fn predicate_columns_checks_keyword_shaped_names_that_are_compared() {
        // YEAR is both an EXTRACT unit and a perfectly ordinary column name.
        // Standing before a comparison it is an operand, so it gets checked.
        let without = vec!["id".to_string()];
        assert!(validate_predicate_columns("year = 2024", &without).is_err());
        assert!(validate_predicate_columns("year>2024", &without).is_err());

        let with = vec!["year".to_string()];
        assert!(validate_predicate_columns("year = 2024", &with).is_ok());

        // ...but the same words in grammar positions are still not columns.
        assert!(validate_predicate_columns("id IS NOT NULL", &without).is_ok());
        assert!(validate_predicate_columns("id > DATE '2024-01-01'", &without).is_ok());
    }

    #[test]
    fn validate_predicate_accepts_simple_where_expression() {
        assert_eq!(
            validate_predicate(" year = 2024 AND event_type = 'click' ").unwrap(),
            "year = 2024 AND event_type = 'click'"
        );
    }

    #[test]
    fn validate_predicate_rejects_statement_separators_and_comments() {
        assert!(validate_predicate("year = 2024; DROP TABLE events").is_err());
        assert!(validate_predicate("year = 2024 -- comment").is_err());
        assert!(validate_predicate("year = 2024 /* comment */").is_err());
    }

    #[test]
    fn validate_predicate_rejects_full_sql_statements() {
        assert!(validate_predicate("SELECT * FROM events").is_err());
        assert!(validate_predicate("year = 2024 DELETE FROM events").is_err());
    }

    #[test]
    fn validate_predicate_accepts_keyword_inside_quoted_literal() {
        // A reserved word inside a string value is data, not SQL syntax.
        assert_eq!(
            validate_predicate("name = 'DROP everything'").unwrap(),
            "name = 'DROP everything'"
        );
        assert_eq!(
            validate_predicate("name = 'it''s a DELETE order'").unwrap(),
            "name = 'it''s a DELETE order'"
        );
    }

    #[test]
    fn validate_predicate_still_rejects_keyword_outside_literal() {
        assert!(validate_predicate("name = 'ok' OR DROP TABLE events").is_err());
    }

    #[test]
    fn mask_string_literals_blanks_contents_but_keeps_quotes_and_length() {
        assert_eq!(mask_string_literals("name = 'DROP'"), "name = '    '");
        // A doubled quote is SQL's escaped quote, not the literal's end, so
        // everything up to the real closing quote stays masked.
        assert_eq!(mask_string_literals("'it''s DROP'"), "'          '");
        assert_eq!(mask_string_literals("no literal here"), "no literal here");
    }
}
