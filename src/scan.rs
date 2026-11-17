use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider};
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
use quack_rs::types::TypeId;
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::results::{parse_s3_uri, CsvRowStream, ResultRow};
use crate::types::{map_type, ColType};

/// What a caller passed to `athena_scan`, before validation. Each engine API
/// (C API v1 through quack-rs, C API v2 through duckdb-neo) reads its own
/// argument handles into this, so everything after it is shared.
#[derive(Default)]
pub struct ScanArgs {
    pub tablename: String,
    pub output_location: Option<String>,
    pub workgroup: Option<String>,
    pub maxrows: Option<i32>,
    pub database: Option<String>,
    pub predicate: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub timeout_seconds: Option<i32>,
    pub result_reuse_minutes: Option<i32>,
}

pub struct ScanBindData {
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
    pub columns: Vec<String>,
    /// Resolved DuckDB type of each column in `columns` (same order). Carried
    /// from bind so the scan writes each value — decimals especially — with the
    /// exact physical width registered here, never re-derived from result metadata.
    pub col_types: Vec<ColType>,
    /// Whether a filter on each column may be sent to Athena (same order as
    /// `columns`). Only the C API v2 build pushes filters; see `predicate.rs`.
    #[cfg_attr(not(feature = "capi-v2"), allow(dead_code))]
    pub pushable: Vec<bool>,
    /// Expected row count for DuckDB's join ordering, when there is a basis
    /// for one. A hint, never a promise.
    pub cardinality: Option<u64>,
}

/// Lazily fetches the next Athena result page: `None` at end of stream, `Err`
/// on a page-fetch failure. Boxing keeps the concrete paginator type internal
/// (no smithy types to name here).
type PageFetcher = Box<dyn FnMut() -> Option<Result<GetQueryResultsOutput, String>> + Send>;

/// How the scan reads the finished query's results.
enum ScanMode {
    /// Stream the CSV Athena wrote to S3. One `GetObject`, no page limit.
    Csv { rows: Box<CsvRowStream> },
    /// Fallback when the query exposes no S3 location (for example a workgroup
    /// using Athena-managed query results) or S3 is unreadable: page
    /// `GetQueryResults` 1000 rows at a time.
    Pages {
        next_page: PageFetcher,
        /// The first Athena page's first row is the column header, skipped once.
        first_page: bool,
        /// Rows of a fetched page not handed out yet: a page is 1000 rows, and
        /// the caller's vector may hold fewer.
        pending: VecDeque<ResultRow>,
    },
}

/// The rows of a finished Athena query, handed out a vector at a time.
///
/// `athena_scan` never raises `max_threads` above DuckDB's default of 1, so
/// this is read single-threaded; callers wrap it in a `Mutex` only because the
/// engine APIs want shared state to be `Sync`.
pub struct RowSource {
    mode: ScanMode,
    done: bool,
    progress: Arc<ScanProgress>,
}

impl RowSource {
    /// Up to `capacity` rows; an empty vec means end of stream. Any fetch
    /// error surfaces here rather than when the query finished, so a scan can
    /// emit earlier rows before failing.
    pub fn next_rows(&mut self, capacity: usize) -> Result<Vec<ResultRow>, String> {
        if self.done {
            return Ok(Vec::new());
        }
        let rows = match self.fetch(capacity) {
            Ok(rows) => rows,
            Err(e) => {
                self.done = true;
                return Err(e);
            }
        };
        if rows.is_empty() {
            self.done = true;
            self.progress.finished.store(true, Ordering::Relaxed);
        }
        Ok(rows)
    }

    fn fetch(&mut self, capacity: usize) -> Result<Vec<ResultRow>, String> {
        match &mut self.mode {
            // Read the next vector's worth of rows straight from the result CSV.
            ScanMode::Csv { rows } => {
                let batch = rows.next_rows(capacity)?;
                self.progress
                    .fetched_bytes
                    .store(rows.bytes_read(), Ordering::Relaxed);
                Ok(batch)
            }
            ScanMode::Pages {
                next_page,
                first_page,
                pending,
            } => {
                while pending.is_empty() {
                    let Some(page) = next_page() else { break };
                    let page = page?;
                    let Some(rs) = page.result_set() else { break };
                    let rows = rs.rows();
                    // Athena returns the column header as the first row of
                    // the first page.
                    let skip = usize::from(*first_page && !rows.is_empty());
                    *first_page = false;
                    pending.extend(rows[skip..].iter().map(datum_row_to_result_row));
                }
                let take = capacity.min(pending.len());
                Ok(pending.drain(..take).collect())
            }
        }
    }
}

/// What a running scan can say about how far along it is, shared between the
/// thread doing the work and whoever asks. Only the C API v2 build has anyone
/// asking (its progress callback); the v1 build prints a heartbeat instead.
#[derive(Default)]
pub struct ScanProgress {
    /// When the wait on Athena began. `None` until the query is submitted.
    wait_started: Mutex<Option<Instant>>,
    fetching: AtomicBool,
    fetched_bytes: AtomicU64,
    /// `Content-Length` of the result object; 0 when unknown (paging).
    total_bytes: AtomicU64,
    finished: AtomicBool,
}

impl ScanProgress {
    /// Progress from 0.0 to 1.0, see [`progress_fraction`].
    #[cfg_attr(not(feature = "capi-v2"), allow(dead_code))]
    pub fn fraction(&self) -> f64 {
        if self.finished.load(Ordering::Relaxed) {
            return 1.0;
        }
        let waited = self
            .wait_started
            .lock()
            .ok()
            .and_then(|started| *started)
            .map(|started| started.elapsed());
        let fetch = self.fetching.load(Ordering::Relaxed).then(|| {
            (
                self.fetched_bytes.load(Ordering::Relaxed),
                self.total_bytes.load(Ordering::Relaxed),
            )
        });
        progress_fraction(waited, fetch)
    }
}

/// How long the Athena half of the bar takes to fill to ~63%.
#[cfg_attr(not(feature = "capi-v2"), allow(dead_code))]
const WAIT_HALF_SCALE: Duration = Duration::from_secs(20);

/// Maps what is known about a scan onto one bar.
///
/// Athena reports no total while a query runs -- bytes scanned so far, never
/// bytes to scan -- so the first half of the bar is time-based: it approaches
/// 50% asymptotically and says only "still working". The second half is real:
/// result bytes read over the result object's `Content-Length`. When that
/// length is unknown (the paging fallback) the bar waits at 50%.
#[cfg_attr(not(feature = "capi-v2"), allow(dead_code))]
fn progress_fraction(waited: Option<Duration>, fetch: Option<(u64, u64)>) -> f64 {
    match (fetch, waited) {
        (Some((_, 0)), _) => 0.5,
        (Some((read, total)), _) => 0.5 + 0.5 * (read as f64 / total as f64).min(1.0),
        (None, Some(waited)) => {
            0.5 * (1.0 - (-waited.as_secs_f64() / WAIT_HALF_SCALE.as_secs_f64()).exp())
        }
        (None, None) => 0.0,
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
pub fn result_row_cell(row: &ResultRow, col_idx: usize) -> Option<&str> {
    row.get(col_idx).and_then(|c| c.as_deref())
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
/// DuckDB's progress bar cannot move for a table function loaded through the
/// stable (v1) C API: the whole surface is bind/init/function plus projection
/// pushdown, with no progress callback (C++ table functions have
/// `table_scan_progress`, and the unstable C API v2 has one; this one does
/// not). So the bar renders at 0% for the entire wait, and a long
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

pub fn quote_identifier(identifier: &str) -> String {
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
    //
    // Then every non-word byte becomes a space, so the scan sees tokens rather
    // than spellings. Matching on " SELECT " alone missed `(SELECT` and
    // `SELECT\n`, which is enough to smuggle a subquery over another table
    // past this check -- and a subquery scans and bills a table the caller
    // never named in athena_scan.
    let uppercase: String = mask_string_literals(predicate)
        .to_ascii_uppercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect();
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

/// A named parameter, else the environment variable standing in for it as a
/// session default. The parameter's own rules apply (explicitly empty is an
/// error); an empty or blank variable counts as unset, the way `FOO=` is
/// commonly used to clear one.
fn arg_or_env(
    name: &str,
    raw: Option<&str>,
    env: Option<String>,
) -> Result<Option<String>, String> {
    Ok(parse_optional_arg(name, raw)?
        .or_else(|| env.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())))
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

/// Validates the arguments, reads the table's schema from Glue and resolves
/// everything the scan needs. Errors are in the order the parameters are
/// documented, so the first thing wrong is the first thing reported.
pub fn bind_scan(args: ScanArgs) -> Result<ScanBindData, String> {
    let tablename = args.tablename;

    // Optional explicit S3 results location. When omitted, no client
    // ResultConfiguration is sent and Athena applies the workgroup's own
    // config (location, encryption, ACL, managed results). An explicitly
    // empty value is an error, not a silent fallback.
    let output_location = arg_or_env(
        "output_location",
        args.output_location.as_deref(),
        std::env::var("ATHENA_OUTPUT_LOCATION").ok(),
    )?;

    // Workgroup defaults to ATHENA_WORKGROUP, else `primary`; an explicitly
    // empty value is an error.
    let workgroup = arg_or_env(
        "workgroup",
        args.workgroup.as_deref(),
        std::env::var("ATHENA_WORKGROUP").ok(),
    )?
    .unwrap_or_else(|| "primary".to_owned());

    // maxrows > 0 caps the Athena query with `LIMIT n`. Unset (null) or any
    // value <= 0 (e.g. maxrows=-1) means no limit: return all rows so
    // aggregates and joins over athena_scan see the full table.
    let maxrows = args.maxrows.unwrap_or(0);

    // Positive integer, in seconds, or the default ceiling. A query still
    // running when this elapses is stopped rather than abandoned, so the
    // parameter bounds cost as well as waiting.
    let timeout = match args.timeout_seconds {
        None => DEFAULT_POLL_WAIT,
        Some(seconds) => parse_timeout_seconds(seconds)?,
    };

    // Athena caps result reuse at 7 days; anything longer is a user error
    // rather than something to silently clamp.
    let result_reuse_minutes = args
        .result_reuse_minutes
        .map(parse_reuse_minutes)
        .transpose()?;

    // Database defaults to `default` when omitted; an explicitly empty value
    // is an error, not a silent fallback.
    let database = parse_optional_arg("database", args.database.as_deref())?
        .unwrap_or_else(|| "default".to_owned());

    // Overrides the region the AWS config chain would pick, so one session
    // can read tables in more than one region.
    let region = parse_optional_arg("region", args.region.as_deref())?;

    // Picks a named profile from ~/.aws/config for this scan only, so one
    // session can read across accounts without restarting DuckDB to change
    // AWS_PROFILE. A profile's own region still loses to `region=`, which is
    // set on the loader after the profile.
    let profile = parse_optional_arg("profile", args.profile.as_deref())?;

    let predicate = match args.predicate.as_deref() {
        Some(s) if !s.trim().is_empty() => Some(validate_predicate(s)?),
        _ => None,
    };

    let config = load_aws_config(region.as_deref(), profile.as_deref());
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
    let mut pushable: Vec<bool> = Vec::new();
    // Resolves a Glue column type to the DuckDB type the engine adapter will
    // register (native DECIMAL(width, scale) when applicable). Unmappable
    // types fall back to Varchar.
    let mut register = |name: &str, type_str: &str| {
        columns.push(name.to_string());
        col_types.push(map_type(type_str).unwrap_or(ColType::Simple(TypeId::Varchar)));
        pushable.push(crate::types::filter_pushable(type_str));
    };
    let record_count = match table_result {
        Ok(resp) => {
            let table = resp.table();
            if let Some(table) = table {
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
            table.and_then(|table| glue_record_count(table.parameters()))
        }
        Err(err) => {
            // Glue's own message is often just "Entity Not Found", which
            // says nothing about what was looked up. Name the table, the
            // database and the region, since a wrong region looks exactly
            // like a missing table.
            let where_ = describe_target(&database, &tablename, region.as_deref(), &config);
            return Err(format!("{}: {}", where_, error_chain(&err)));
        }
    };

    if columns.is_empty() {
        return Err(format!(
            "{} has no columns in the Glue catalog",
            describe_target(&database, &tablename, region.as_deref(), &config)
        ));
    }

    Ok(ScanBindData {
        tablename,
        database,
        output_location,
        workgroup,
        limit: maxrows,
        predicate,
        config,
        timeout,
        result_reuse_minutes,
        columns,
        col_types,
        pushable,
        // A hint for the join optimizer, never a promise: without it DuckDB
        // has no idea whether athena_scan is the small or the large side.
        cardinality: cardinality_estimate(maxrows, record_count),
    })
}

/// The row count Glue has on record for a table, if any. Crawlers and Athena
/// CTAS write `recordCount`; Hive and Spark write `numRows`. Both are
/// statistics from whenever they were last computed, and `-1` or `0` mean
/// "not computed" rather than "empty", so neither is passed on.
fn glue_record_count(parameters: Option<&HashMap<String, String>>) -> Option<u64> {
    let parameters = parameters?;
    ["recordCount", "numRows"]
        .iter()
        .filter_map(|key| parameters.get(*key))
        .filter_map(|raw| raw.trim().parse::<u64>().ok())
        .find(|rows| *rows > 0)
}

/// How many rows the scan is expected to return, for DuckDB's join ordering:
/// `maxrows` is a hard upper bound, the Glue statistic a stale approximation,
/// and with both the smaller wins. `None` leaves DuckDB to its own default. A
/// `predicate=` is not accounted for -- its selectivity is unknown here -- so
/// the estimate can only err high.
fn cardinality_estimate(maxrows: i32, record_count: Option<u64>) -> Option<u64> {
    let limit = u64::try_from(maxrows).ok().filter(|rows| *rows > 0);
    match (limit, record_count) {
        (Some(limit), Some(rows)) => Some(limit.min(rows)),
        (limit, rows) => limit.or(rows),
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

/// Every layer of an error, joined.
///
/// AWS SDK errors display only their outermost layer. For anything that fails
/// before the request is dispatched -- a `profile=` that does not exist, absent
/// credentials -- that layer is the literal string "unhandled error", and the
/// actual reason sits in `source()`. Walking the chain is the difference between
/// "unhandled error" and "profile `no_such_profile` was not defined".
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(e) = source {
        let text = e.to_string();
        // SDK layers repeat themselves; a message printed twice reads like two
        // separate failures.
        if !parts.iter().any(|p| p == &text) {
            parts.push(text);
        }
        source = e.source();
    }
    parts.join(": ")
}

/// Resolves AWS config for one scan. Bind loads it and init reuses it through
/// the bind data, so credentials and region resolve once per scan rather than
/// twice — but never across scans: a long-lived process that changes
/// `AWS_PROFILE` between queries must see the new profile, not a cached one.
fn load_aws_config(region: Option<&str>, profile: Option<&str>) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(profile) = profile {
        // An explicit credentials provider, not `loader.profile_name()`. That
        // only tells the *profile-file provider* which profile to read, and the
        // default chain consults environment credentials first -- so with
        // AWS_ACCESS_KEY_ID set (which the README suggests) `profile=prod` was
        // silently ignored and the query ran against the environment's account.
        // Measured before the fix: a profile that does not exist still returned
        // "The security token included in the request is invalid" from the env
        // credentials, rather than saying the profile was not defined.
        loader = loader.credentials_provider(
            ProfileFileCredentialsProvider::builder()
                .profile_name(profile)
                .build(),
        );
        // Same reasoning for the region: the profile's own region should apply
        // when the scan did not name one, and lose when it did. A chain, not a
        // bare provider -- setting the profile provider alone *replaces* the
        // region chain, so AWS_REGION stopped being a fallback and a profile
        // without a region reported "<no region configured>" despite the
        // environment having one.
        if region.is_none() {
            loader = loader.region(
                RegionProviderChain::first_try(
                    ProfileFileRegionProvider::builder()
                        .profile_name(profile)
                        .build(),
                )
                .or_default_provider(),
            );
        }
    }
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

/// The `GetQueryResults` paging fallback: one page per call.
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
        pending: VecDeque::new(),
    }
}

/// The user's `predicate=` and the filters pushed down by DuckDB, as one
/// `WHERE` body. Alone, either is sent as written; together each is
/// parenthesized, since either may contain a top-level `OR`.
fn combine_predicates(user: Option<&str>, pushed: Option<&str>) -> Option<String> {
    match (user, pushed) {
        (Some(user), Some(pushed)) => Some(format!("({user}) AND ({pushed})")),
        (Some(only), None) | (None, Some(only)) => Some(only.to_owned()),
        (None, None) => None,
    }
}

/// An Athena query that has been accepted and is queued or running.
pub struct SubmittedScan {
    client: AthenaClient,
    config: aws_config::SdkConfig,
    query_execution_id: String,
    timeout: Duration,
    pub progress: Arc<ScanProgress>,
}

/// Builds the query for the columns DuckDB projected and submits it.
///
/// `projected` indexes `ScanBindData::columns`, in the order DuckDB wants the
/// output; `pushed` is an extra predicate from filter pushdown (C API v2 only).
/// Returns as soon as Athena accepts the query -- [`SubmittedScan::wait`] does
/// the waiting.
pub fn submit_scan(
    bind_data: &ScanBindData,
    projected: &[usize],
    pushed: Option<&str>,
) -> Result<SubmittedScan, String> {
    // Projection pushdown: select only the columns DuckDB needs so columnar
    // formats scan fewer bytes. Falls back to all columns when nothing is
    // projected.
    let select_list = projected_select_list(&bind_data.columns, &bind_data.col_types, projected);

    // Loaded at bind for this scan; reusing it here avoids resolving
    // credentials a second time without outliving the scan.
    let config = bind_data.config.clone();
    let client = AthenaClient::new(&config);

    let predicate = combine_predicates(bind_data.predicate.as_deref(), pushed);
    let query = build_athena_query(
        &select_list,
        &bind_data.database,
        &bind_data.tablename,
        predicate.as_deref(),
        bind_data.limit,
    );

    // Only send a client ResultConfiguration when an explicit
    // output_location was given. Otherwise Athena applies the workgroup's
    // own result configuration (location, encryption, ACL, managed results).
    let mut request = client
        .start_query_execution()
        .query_string(query)
        .work_group(bind_data.workgroup.clone());
    if let Some(location) = bind_data.output_location.clone() {
        request = request.result_configuration(
            ResultConfiguration::builder()
                .output_location(location)
                .build(),
        );
    }
    // Opt-in: Athena returns a previous identical query's result if it is
    // younger than this, scanning no data and charging nothing.
    if let Some(minutes) = bind_data.result_reuse_minutes {
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

    let query_execution_id = match crate::RUNTIME.block_on(request.send()) {
        Ok(r) => match r.query_execution_id().filter(|id| !id.is_empty()) {
            Some(id) => id.to_string(),
            // Polling "" would loop against a nonexistent execution until
            // the timeout and then blame the timeout. Athena should never
            // do this, but unwrap_or_default() turned "should never" into
            // an hour-long wait for a wrong error.
            None => {
                return Err("Athena accepted the query but returned no execution id".to_string())
            }
        },
        Err(e) => {
            // Include the workgroup: the usual cause is a workgroup with no
            // result configuration and no output_location given, and the
            // raw message does not say which workgroup was used.
            return Err(format!(
                "starting Athena query in workgroup \"{}\": {e}",
                bind_data.workgroup
            ));
        }
    };

    eprintln!("Running Athena query, execution id: {query_execution_id}");

    let progress = Arc::new(ScanProgress::default());
    if let Ok(mut started) = progress.wait_started.lock() {
        *started = Some(Instant::now());
    }
    Ok(SubmittedScan {
        client,
        config,
        query_execution_id,
        timeout: bind_data.timeout,
        progress,
    })
}

impl SubmittedScan {
    /// Polls until the query reaches a terminal state, then opens its result.
    ///
    /// `heartbeat` prints a line to stderr while a slow query runs. The v1
    /// build needs it, because DuckDB's progress bar cannot move there; the v2
    /// build reports through [`ScanProgress`] instead and leaves stderr alone.
    pub fn wait(&self, heartbeat: bool) -> Result<RowSource, String> {
        let client = &self.client;
        let query_execution_id = &self.query_execution_id;
        let timeout = self.timeout;

        let poll_start = Instant::now();
        let mut last_report: Option<Instant> = None;
        let mut poll_delay = POLL_INITIAL;
        loop {
            let resp = crate::RUNTIME
                .block_on(
                    client
                        .get_query_execution()
                        .query_execution_id(query_execution_id.clone())
                        .send(),
                )
                .map_err(|e| e.to_string())?;

            let state = status(&resp).ok_or_else(|| "Could not get query state".to_string())?;

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
                        let region = self.config.region().map(|r| r.to_string());
                        return Err(timeout_message(
                            query_execution_id,
                            &format!("{state:?}"),
                            timeout.as_secs(),
                            region.as_deref(),
                            stop.err().map(|e| e.to_string()).as_deref(),
                        ));
                    }
                    // The one piece of feedback available on v1: DuckDB's own
                    // progress bar is stuck at 0% for the whole wait (see
                    // progress_line), so without this a slow query looks hung.
                    if heartbeat {
                        if let Some(line) = progress_line(
                            poll_start.elapsed(),
                            last_report.map(|t: Instant| t.elapsed()),
                            &format!("{state:?}").to_uppercase(),
                            scanned_bytes(&resp),
                        ) {
                            eprintln!("{line}");
                            last_report = Some(Instant::now());
                        }
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
                    return Err(failure_message(
                        query_execution_id,
                        &format!("{state:?}"),
                        reason(&resp),
                    ));
                }
                _ => {
                    print_query_stats(&resp);

                    // Athena has already written the entire result set as one CSV
                    // object. Streaming it costs a single GetObject; paging the
                    // same rows through GetQueryResults costs one call per 1000
                    // rows (~135s for a million), so only fall back to paging when
                    // there is no readable S3 location.
                    let mode = match open_result_csv(&self.config, &resp) {
                        Ok(Some(rows)) => {
                            self.progress
                                .total_bytes
                                .store(rows.content_length().unwrap_or(0), Ordering::Relaxed);
                            ScanMode::Csv {
                                rows: Box::new(rows),
                            }
                        }
                        Ok(None) => paged_mode(client, query_execution_id),
                        Err(e) => {
                            eprintln!("Falling back to GetQueryResults paging: {e}");
                            paged_mode(client, query_execution_id)
                        }
                    };
                    self.progress.fetching.store(true, Ordering::Relaxed);

                    return Ok(RowSource {
                        mode,
                        done: false,
                        progress: Arc::clone(&self.progress),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        arg_or_env, build_athena_query, cardinality_estimate, combine_predicates,
        datum_row_to_result_row, describe_target, error_chain, failure_message, glue_record_count,
        mask_string_literals, next_poll_delay, parse_optional_arg, parse_reuse_minutes,
        parse_timeout_seconds, progress_fraction, progress_line, projected_select_list,
        qualified_table, result_output_location, result_row_cell, sleep_before_next_poll,
        timeout_message, validate_predicate, POLL_INITIAL, POLL_MAX,
    };
    use crate::types::ColType;
    use aws_sdk_athena::operation::get_query_execution::GetQueryExecutionOutput;
    use aws_sdk_athena::types::{Datum, QueryExecution, ResultConfiguration, Row};
    use quack_rs::types::TypeId;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn glue_record_count_reads_either_spelling_and_skips_placeholders() {
        let params = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert_eq!(glue_record_count(None), None);
        assert_eq!(glue_record_count(Some(&params(&[]))), None);
        assert_eq!(
            glue_record_count(Some(&params(&[("recordCount", "167585")]))),
            Some(167_585)
        );
        assert_eq!(
            glue_record_count(Some(&params(&[("numRows", " 42 ")]))),
            Some(42)
        );
        // Hive writes -1 and 0 for "statistics not computed".
        assert_eq!(glue_record_count(Some(&params(&[("numRows", "-1")]))), None);
        assert_eq!(
            glue_record_count(Some(&params(&[("recordCount", "0"), ("numRows", "9")]))),
            Some(9)
        );
        assert_eq!(
            glue_record_count(Some(&params(&[("recordCount", "lots")]))),
            None
        );
    }

    #[test]
    fn cardinality_estimate_prefers_the_tighter_bound() {
        assert_eq!(cardinality_estimate(0, None), None);
        assert_eq!(cardinality_estimate(-1, None), None);
        assert_eq!(cardinality_estimate(100, None), Some(100));
        assert_eq!(cardinality_estimate(0, Some(5_000)), Some(5_000));
        assert_eq!(cardinality_estimate(-1, Some(5_000)), Some(5_000));
        assert_eq!(cardinality_estimate(100, Some(5_000)), Some(100));
        assert_eq!(cardinality_estimate(100, Some(7)), Some(7));
    }

    #[test]
    fn progress_is_time_based_until_the_result_has_a_length() {
        assert_eq!(progress_fraction(None, None), 0.0);
        let early = progress_fraction(Some(Duration::from_secs(1)), None);
        let late = progress_fraction(Some(Duration::from_secs(600)), None);
        // Waiting on Athena approaches half and never reaches it.
        assert!(early > 0.0 && early < late && late < 0.5);
        // Fetching is measured: bytes read over the object's length.
        assert_eq!(progress_fraction(None, Some((0, 100))), 0.5);
        assert_eq!(progress_fraction(None, Some((50, 100))), 0.75);
        assert_eq!(progress_fraction(None, Some((100, 100))), 1.0);
        assert_eq!(progress_fraction(None, Some((150, 100))), 1.0);
        // No length (the paging fallback): hold at half rather than guess.
        assert_eq!(progress_fraction(None, Some((4096, 0))), 0.5);
    }

    #[test]
    fn predicates_are_parenthesized_only_when_combined() {
        assert_eq!(combine_predicates(None, None), None);
        assert_eq!(
            combine_predicates(Some("a = 1 OR b = 2"), None).as_deref(),
            Some("a = 1 OR b = 2")
        );
        assert_eq!(
            combine_predicates(None, Some("\"c\" > 3")).as_deref(),
            Some("\"c\" > 3")
        );
        assert_eq!(
            combine_predicates(Some("a = 1 OR b = 2"), Some("\"c\" > 3")).as_deref(),
            Some("(a = 1 OR b = 2) AND (\"c\" > 3)")
        );
    }

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
    fn an_error_reports_every_layer_not_just_the_outermost() {
        // The failure this fixes: a bad `profile=` renders as the literal
        // "unhandled error" at the top level, with the real reason -- profile
        // not defined -- buried in source(). Nothing about that message tells
        // the reader what to change.
        #[derive(Debug)]
        struct Layer(&'static str, Option<Box<Layer>>);
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1
                    .as_deref()
                    .map(|e| e as &(dyn std::error::Error + 'static))
            }
        }

        let deep = Layer(
            "dispatch failure",
            Some(Box::new(Layer(
                "the credentials provider was not properly configured",
                Some(Box::new(Layer("profile `nope` was not defined", None))),
            ))),
        );
        assert_eq!(
            error_chain(&deep),
            "dispatch failure: the credentials provider was not properly configured: \
             profile `nope` was not defined"
        );

        // A single-layer error is unchanged, not decorated.
        assert_eq!(error_chain(&Layer("plain", None)), "plain");

        // SDK layers repeat themselves; the same text twice reads as two
        // separate failures, so it is said once.
        let repeated = Layer("same", Some(Box::new(Layer("same", None))));
        assert_eq!(error_chain(&repeated), "same");
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

    #[test]
    fn named_parameter_beats_the_environment() {
        assert_eq!(
            arg_or_env("workgroup", Some("adhoc"), Some("etl".into())).unwrap(),
            Some("adhoc".to_string())
        );
    }

    #[test]
    fn environment_fills_an_omitted_parameter() {
        assert_eq!(
            arg_or_env("workgroup", None, Some(" etl ".into())).unwrap(),
            Some("etl".to_string())
        );
    }

    #[test]
    fn blank_environment_variable_is_unset() {
        assert_eq!(
            arg_or_env("workgroup", None, Some("  ".into())).unwrap(),
            None
        );
        assert_eq!(arg_or_env("workgroup", None, None).unwrap(), None);
    }

    #[test]
    fn explicitly_empty_parameter_is_still_an_error_with_the_environment_set() {
        assert!(arg_or_env("workgroup", Some(""), Some("etl".into())).is_err());
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
    fn validate_predicate_accepts_simple_where_expression() {
        assert_eq!(
            validate_predicate(" year = 2024 AND event_type = 'click' ").unwrap(),
            "year = 2024 AND event_type = 'click'"
        );
    }

    #[test]
    fn a_subquery_cannot_hide_behind_punctuation_or_newlines() {
        // Removing the identifier check left this scan as the only thing
        // standing between `predicate=` and a subquery over a table the caller
        // never named -- which Athena would happily scan and bill. Matching on
        // the spelling " SELECT " missed every real way of writing it.
        for smuggled in [
            "EXISTS (SELECT 1 FROM other_table)",
            "EXISTS (SELECT\n1 FROM other_table)",
            "id IN(SELECT id FROM other_table)",
            "id IN\t(select id from other_table)",
            "EXISTS\r\n(SELECT\t1)",
        ] {
            assert!(
                validate_predicate(smuggled).is_err(),
                "should be rejected: {smuggled:?}"
            );
        }

        // Tokens, not substrings: a column whose name contains a keyword is
        // still an ordinary column.
        for fine in [
            "selected = 1",
            "col_select > 0",
            "insertion_id = 5",
            "name = 'SELECT 1 FROM t'",
        ] {
            assert!(
                validate_predicate(fine).is_ok(),
                "should be allowed: {fine:?}"
            );
        }
    }

    #[test]
    fn valid_sql_is_not_rejected_for_looking_unfamiliar() {
        // This is why the column checker is gone. It carried a 60-word grammar,
        // and every word missing from that list was valid SQL refused before
        // Athena ever saw it: `EXTRACT(year FROM col)` was rejected outright
        // until FROM/TO/AT/PRECISION/LOCAL were added, and the next gap would
        // have been found the same way -- by a user with a working query.
        //
        // Athena rejects an unknown column itself in ~555ms for 0 bytes, with
        // `COLUMN_NOT_FOUND: Column 'x' cannot be resolved`, and that reason is
        // surfaced verbatim. What remains here is only what Athena would not
        // catch: an expression shaped like a second statement.
        for ok in [
            "EXTRACT(year FROM ts) = 2024",
            "ts AT TIME ZONE 'UTC' > TIMESTAMP '2024-01-01 00:00:00'",
            "CAST(x AS DOUBLE PRECISION) > 1",
            "nosuchcolumn = 1",
        ] {
            assert!(validate_predicate(ok).is_ok(), "{ok} should reach Athena");
        }
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
