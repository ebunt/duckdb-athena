//! The C API v2 adapter: `athena_scan` registered through duckdb-neo.
//!
//! Published beside the standard build, but DuckDB 2.0's C API v2 is
//! unstable, so a build with
//! `--features capi-v2` loads into exactly one DuckDB version (the one
//! `libduckdb-sys` is pinned to) and nothing else. In exchange it gets what the
//! stable v1 API cannot offer:
//!
//! - **Filter pushdown.** A plain DuckDB `WHERE` reaches Athena, so partitions
//!   prune and less data is scanned. Filters are pushed but never *accepted*:
//!   DuckDB re-applies every one to the rows that come back. See
//!   `predicate.rs` for why that is the safe contract today.
//! - **A progress bar that moves**, instead of a heartbeat on stderr.
//!
//! Everything that is not about DuckDB's handles lives in `scan.rs`, shared
//! with the v1 adapter.

use std::sync::{Arc, Mutex};

use duckdb_neo::{
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::DataChunkRef,
    duckdb_entrypoint_c_api_v2,
    expression::{Expression, ExpressionType},
    extension::Extension,
    logical_type::LogicalType,
    signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, PushdownData,
        TableFilterPushdownCallbacks, TableFunctionBuilder, TableFunctionCallbacks,
    },
    types::{DateValue, HugeDecimal, LongDecimal, ShortDecimal, TimestampValue, WordDecimal},
    DuckDBType, Error, Result,
};
use quack_rs::types::TypeId;

use crate::predicate::{render_conjunction, Cmp, Filter, Literal};
use crate::results::ResultRow;
use crate::scan::{
    bind_scan, result_row_cell, submit_scan, RowSource, ScanArgs, ScanBindData, ScanProgress,
    SubmittedScan,
};
use crate::types::{parse_cell, Cell, ColType};

/// Named parameters, in the order they are declared and therefore the order
/// DuckDB hands their values to bind (after the positional table name).
const STRING_PARAMS: [&str; 6] = [
    "output_location",
    "workgroup",
    "database",
    "predicate",
    "region",
    "profile",
];
const INT_PARAMS: [&str; 3] = ["maxrows", "timeout_seconds", "result_reuse_minutes"];

/// What the optimizer's filter-pushdown callback left for the scan.
#[derive(Debug, Default, PartialEq, Eq)]
enum Pushed {
    /// The callback has not run, or offered nothing this extension pushes.
    #[default]
    Nothing,
    Predicate(String),
    /// The callback ran more than once and disagreed with itself. Bind data
    /// is supposed to belong to one scan; if it ever turns out to be shared,
    /// sending either predicate could drop rows the other scan needs, so
    /// neither is sent. Nothing is lost but the saving -- DuckDB filters the
    /// rows regardless.
    Conflict,
}

impl Pushed {
    fn offer(&mut self, predicate: Option<String>) {
        *self = match (std::mem::take(self), predicate) {
            (Self::Nothing, Some(new)) => Self::Predicate(new),
            (Self::Nothing, None) => Self::Nothing,
            (Self::Predicate(old), Some(new)) if old == new => Self::Predicate(old),
            _ => Self::Conflict,
        };
    }

    fn predicate(&self) -> Option<&str> {
        match self {
            Self::Predicate(predicate) => Some(predicate),
            Self::Nothing | Self::Conflict => None,
        }
    }
}

struct BindData {
    scan: ScanBindData,
    /// Written by the pushdown callback (optimizer), read at init (executor).
    pushed: Mutex<Pushed>,
}

struct GlobalState {
    submitted: SubmittedScan,
    progress: Arc<ScanProgress>,
    /// `None` until the first `exec` has waited for Athena. Waiting there
    /// rather than at init is what lets the progress callback run meanwhile:
    /// DuckDB only asks a scan for progress once its global state exists.
    source: Mutex<Option<RowSource>>,
    /// Resolved type of each projected column, in projection order.
    col_types: Vec<ColType>,
}

struct AthenaScan;

fn error(message: String) -> Error {
    Error::invalid_input(message)
}

fn optional_string(argument: &BindArgument) -> Result<Option<String>> {
    match &argument.value {
        Some(value) if !value.is_null()? => value.get::<String>(),
        _ => Ok(None),
    }
}

fn optional_i32(argument: &BindArgument) -> Result<Option<i32>> {
    match &argument.value {
        Some(value) if !value.is_null()? => value.get::<i32>(),
        _ => Ok(None),
    }
}

fn read_args(arguments: &[BindArgument]) -> Result<ScanArgs> {
    let expected = 1 + STRING_PARAMS.len() + INT_PARAMS.len();
    if arguments.len() != expected {
        return Err(error(format!(
            "athena_scan expected {expected} bound arguments, got {}",
            arguments.len()
        )));
    }
    let tablename = optional_string(&arguments[0])?
        .ok_or_else(|| error("athena_scan requires a constant table name".to_string()))?;
    let (strings, ints) = arguments[1..].split_at(STRING_PARAMS.len());
    Ok(ScanArgs {
        tablename,
        output_location: optional_string(&strings[0])?,
        workgroup: optional_string(&strings[1])?,
        database: optional_string(&strings[2])?,
        predicate: optional_string(&strings[3])?,
        region: optional_string(&strings[4])?,
        profile: optional_string(&strings[5])?,
        maxrows: optional_i32(&ints[0])?,
        timeout_seconds: optional_i32(&ints[1])?,
        result_reuse_minutes: optional_i32(&ints[2])?,
    })
}

fn duckdb_type(context: &Context, col_type: ColType) -> Result<LogicalType> {
    let name = match col_type {
        ColType::Decimal { width, scale } => format!("DECIMAL({width},{scale})"),
        // JSON text is a VARCHAR column to DuckDB; only the Athena-side
        // SELECT differs.
        ColType::Json => "VARCHAR".to_string(),
        ColType::Simple(type_id) => match type_id {
            TypeId::Boolean => "BOOLEAN",
            TypeId::TinyInt => "TINYINT",
            TypeId::SmallInt => "SMALLINT",
            TypeId::Integer => "INTEGER",
            TypeId::BigInt => "BIGINT",
            TypeId::Float => "FLOAT",
            TypeId::Double => "DOUBLE",
            TypeId::Date => "DATE",
            TypeId::Timestamp => "TIMESTAMP",
            _ => "VARCHAR",
        }
        .to_string(),
    };
    LogicalType::from_text(context, &name)
}

/// Projected column indexes, in the order DuckDB wants the output.
fn projected_columns(column_data: &InitColumnData<'_>) -> Result<Vec<usize>> {
    (0..column_data.count()?)
        .map(|i| column_data.get_column_index(i))
        .collect()
}

impl TableFunctionCallbacks for AthenaScan {
    type BindData = BindData;
    type LocalState = ();
    type GlobalState = GlobalState;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let scan = bind_scan(read_args(&arguments)?).map_err(error)?;
        for (name, col_type) in scan.columns.iter().zip(&scan.col_types) {
            bind_handle.add_result_column(name, &duckdb_type(context, *col_type)?)?;
        }
        if let Some(rows) = scan.cardinality {
            bind_handle.set_cardinality(usize::try_from(rows).unwrap_or(usize::MAX), false)?;
        }
        Ok(BindData {
            scan,
            pushed: Mutex::default(),
        })
    }

    fn init_global_state(
        &self,
        _context: &Context,
        bind_data: &Self::BindData,
        column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::GlobalState>> {
        let projected = projected_columns(&column_data)?;
        let col_types = projected
            .iter()
            .filter_map(|&i| bind_data.scan.col_types.get(i).copied())
            .collect();
        let pushed = bind_data
            .pushed
            .lock()
            .map_err(|_| error("athena_scan pushdown state was poisoned".to_string()))?;
        // Submitting is quick; the wait happens in `exec`.
        let submitted =
            submit_scan(&bind_data.scan, &projected, pushed.predicate()).map_err(error)?;
        Ok(Some(GlobalState {
            progress: Arc::clone(&submitted.progress),
            submitted,
            source: Mutex::new(None),
            col_types,
        }))
    }

    fn progress(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
    ) -> Result<Option<f64>> {
        Ok(global_state.map(|state| state.progress.fraction()))
    }

    fn exec(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        _local_state: Option<&mut Self::LocalState>,
        global_state: Option<&Self::GlobalState>,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let state =
            global_state.ok_or_else(|| error("athena_scan has no scan state".to_string()))?;
        let mut source = state
            .source
            .lock()
            .map_err(|_| error("athena_scan scan state was poisoned".to_string()))?;
        if source.is_none() {
            // No heartbeat: the progress callback speaks for the scan here.
            *source = Some(state.submitted.wait(false).map_err(error)?);
        }
        let rows = source
            .as_mut()
            .expect("set just above")
            .next_rows(output.capacity()?)
            .map_err(error)?;
        write_rows(&mut output, &state.col_types, &rows)
    }
}

/// Fills one typed vector from a column of parsed cells: a cell of the
/// expected shape is written, anything else (SQL NULL, or text that did not
/// parse as the column's type) is NULL.
macro_rules! write_column {
    ($output:expr, $col:expr, $rows:expr, $col_type:expr, $ty:ty, $pattern:pat => $value:expr) => {{
        let mut vector = $output.get_vector_at_mut::<$ty>($col)?;
        vector.set_size($rows.len())?;
        let mut writer = vector.writer()?;
        for (row_idx, row) in $rows.iter().enumerate() {
            match parse_cell(result_row_cell(row, $col), $col_type) {
                Some($pattern) => writer.set(row_idx, $value)?,
                _ => writer.set_null(row_idx)?,
            }
        }
    }};
}

fn write_rows(
    output: &mut DataChunkRef<'_>,
    col_types: &[ColType],
    rows: &[ResultRow],
) -> Result<()> {
    let col_count = col_types.len().min(output.col_count());
    for (col, &col_type) in col_types.iter().take(col_count).enumerate() {
        match col_type {
            // The storage DuckDB allocates for a DECIMAL depends on its width;
            // `parse_cell` has already checked the value fits it.
            ColType::Decimal { width, .. } => match width {
                1..=4 => write_column!(output, col, rows, col_type, ShortDecimal,
                    Cell::Decimal { unscaled, .. } => &ShortDecimal(unscaled as i16)),
                5..=9 => write_column!(output, col, rows, col_type, WordDecimal,
                    Cell::Decimal { unscaled, .. } => &WordDecimal(unscaled as i32)),
                10..=18 => write_column!(output, col, rows, col_type, LongDecimal,
                    Cell::Decimal { unscaled, .. } => &LongDecimal(unscaled as i64)),
                _ => write_column!(output, col, rows, col_type, HugeDecimal,
                    Cell::Decimal { unscaled, .. } => &HugeDecimal(unscaled)),
            },
            ColType::Simple(TypeId::Boolean) => {
                write_column!(output, col, rows, col_type, bool, Cell::Bool(v) => &v)
            }
            ColType::Simple(TypeId::TinyInt) => {
                write_column!(output, col, rows, col_type, i8, Cell::I8(v) => &v)
            }
            ColType::Simple(TypeId::SmallInt) => {
                write_column!(output, col, rows, col_type, i16, Cell::I16(v) => &v)
            }
            ColType::Simple(TypeId::Integer) => {
                write_column!(output, col, rows, col_type, i32, Cell::I32(v) => &v)
            }
            ColType::Simple(TypeId::BigInt) => {
                write_column!(output, col, rows, col_type, i64, Cell::I64(v) => &v)
            }
            ColType::Simple(TypeId::Float) => {
                write_column!(output, col, rows, col_type, f32, Cell::F32(v) => &v)
            }
            ColType::Simple(TypeId::Double) => {
                write_column!(output, col, rows, col_type, f64, Cell::F64(v) => &v)
            }
            ColType::Simple(TypeId::Date) => {
                write_column!(output, col, rows, col_type, DateValue, Cell::Date(v) => &DateValue(v))
            }
            ColType::Simple(TypeId::Timestamp) => write_column!(output, col, rows, col_type,
                TimestampValue, Cell::Timestamp(v) => &TimestampValue(v)),
            ColType::Simple(_) | ColType::Json => {
                write_column!(output, col, rows, col_type, String, Cell::Text(v) => v)
            }
        }
    }
    // With nothing projected DuckDB still keeps one placeholder column, so
    // there is always a vector 0 to carry the row count.
    if col_count == 0 {
        output
            .get_vector_at_mut::<duckdb_neo::vector::Unknown>(0)?
            .set_size(rows.len())?;
    }
    Ok(())
}

impl TableFilterPushdownCallbacks for AthenaScan {
    fn pushdown_filter(
        &self,
        context: &Context,
        bind_data: &Self::BindData,
        pushdown_data: PushdownData<'_>,
    ) -> Result<()> {
        let lowering = Lowering {
            context,
            scan: &bind_data.scan,
            pushdown_data: &pushdown_data,
            varchar: String::logical_type(context)?,
        };
        let mut filters = Vec::new();
        for index in 0..pushdown_data.filter_count()? {
            // A filter this extension cannot express is skipped, never an
            // error: DuckDB applies it to the rows that come back.
            if let Some(filter) = lowering.lower(&pushdown_data.filter(index)?)? {
                filters.push(filter);
            }
        }
        // Deliberately no `accept_pushdown`: see the module docs.
        bind_data
            .pushed
            .lock()
            .map_err(|_| error("athena_scan pushdown state was poisoned".to_string()))?
            .offer(render_conjunction(&filters));
        Ok(())
    }
}

/// Turns the bound expressions DuckDB offers into [`Filter`]s. Returns
/// `Ok(None)` for anything outside the supported subset.
struct Lowering<'a> {
    context: &'a Context,
    scan: &'a ScanBindData,
    pushdown_data: &'a PushdownData<'a>,
    varchar: LogicalType,
}

impl Lowering<'_> {
    fn lower(&self, expression: &Expression<'_>) -> Result<Option<Filter>> {
        let comparison = |op| self.comparison(expression, op);
        match expression.expression_type()? {
            ExpressionType::CompareEqual => comparison(Cmp::Eq),
            ExpressionType::CompareNotEqual => comparison(Cmp::Ne),
            ExpressionType::CompareLessThan => comparison(Cmp::Lt),
            ExpressionType::CompareLessThanOrEqualTo => comparison(Cmp::Le),
            ExpressionType::CompareGreaterThan => comparison(Cmp::Gt),
            ExpressionType::CompareGreaterThanOrEqualTo => comparison(Cmp::Ge),
            ExpressionType::CompareBetween => self.between(expression),
            ExpressionType::CompareIn => self.in_list(expression),
            ExpressionType::OperatorIsNull => self.null_check(expression, Filter::IsNull),
            ExpressionType::OperatorIsNotNull => self.null_check(expression, Filter::IsNotNull),
            ExpressionType::ConjunctionAnd => {
                // Any subset of an AND's operands is a weaker filter, so keep
                // the ones that lower and drop the rest.
                let mut parts = Vec::new();
                for child in expression.children()? {
                    parts.extend(self.lower(&child)?);
                }
                Ok(match parts.len() {
                    0 => None,
                    1 => parts.pop(),
                    _ => Some(Filter::And(parts)),
                })
            }
            ExpressionType::ConjunctionOr => {
                // Dropping an operand of an OR would make it stricter, so it
                // is all of them or nothing.
                let mut parts = Vec::new();
                for child in expression.children()? {
                    match self.lower(&child)? {
                        Some(part) => parts.push(part),
                        None => return Ok(None),
                    }
                }
                Ok((!parts.is_empty()).then_some(Filter::Or(parts)))
            }
            // NOT, casts, functions, IS DISTINCT FROM, NOT IN, ...: DuckDB's.
            _ => Ok(None),
        }
    }

    /// A direct reference to a column whose filters may be pushed: its Athena
    /// name and resolved type. A column wrapped in a cast or function is not
    /// one -- the comparison would then be on a different value.
    fn column(&self, expression: &Expression<'_>) -> Result<Option<(String, ColType)>> {
        if expression.expression_type()? != ExpressionType::BoundColumnRef {
            return Ok(None);
        }
        let index = self
            .pushdown_data
            .get_column_index(expression.reference_index()?)?;
        if !self.scan.pushable.get(index).copied().unwrap_or(false) {
            return Ok(None);
        }
        Ok(self
            .scan
            .columns
            .get(index)
            .zip(self.scan.col_types.get(index))
            .map(|(name, col_type)| (name.clone(), *col_type)))
    }

    /// A non-NULL constant, read for a column of `col_type`.
    fn literal(&self, expression: &Expression<'_>, col_type: ColType) -> Result<Option<Literal>> {
        if expression.expression_type()? != ExpressionType::ValueConstant {
            return Ok(None);
        }
        let value = expression.get_constant_value()?;
        if value.is_null()? {
            return Ok(None);
        }
        // DuckDB's own text for the value is the one form every type has;
        // `Literal::for_column` accepts only what it can vouch for.
        let text = value.cast(self.context, &self.varchar)?.get::<String>()?;
        Ok(text.and_then(|text| Literal::for_column(col_type, &text)))
    }

    fn comparison(&self, expression: &Expression<'_>, op: Cmp) -> Result<Option<Filter>> {
        if expression.child_count()? != 2 {
            return Ok(None);
        }
        let (left, right) = (expression.child(0)?, expression.child(1)?);
        // `col < 5` or `5 > col`.
        for (column, constant, op) in [(&left, &right, op), (&right, &left, op.flipped())] {
            let Some((column, col_type)) = self.column(column)? else {
                continue;
            };
            return Ok(self
                .literal(constant, col_type)?
                .map(|value| Filter::Compare { column, op, value }));
        }
        Ok(None)
    }

    fn between(&self, expression: &Expression<'_>) -> Result<Option<Filter>> {
        if expression.child_count()? != 3 {
            return Ok(None);
        }
        let Some((column, col_type)) = self.column(&expression.child(0)?)? else {
            return Ok(None);
        };
        let low = self.literal(&expression.child(1)?, col_type)?;
        let high = self.literal(&expression.child(2)?, col_type)?;
        Ok(low
            .zip(high)
            .map(|(low, high)| Filter::Between { column, low, high }))
    }

    fn in_list(&self, expression: &Expression<'_>) -> Result<Option<Filter>> {
        let children = expression.children()?;
        let Some((first, rest)) = children.split_first() else {
            return Ok(None);
        };
        let Some((column, col_type)) = self.column(first)? else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(rest.len());
        for child in rest {
            match self.literal(child, col_type)? {
                Some(value) => values.push(value),
                None => return Ok(None),
            }
        }
        Ok((!values.is_empty()).then_some(Filter::In { column, values }))
    }

    fn null_check(
        &self,
        expression: &Expression<'_>,
        build: fn(String) -> Filter,
    ) -> Result<Option<Filter>> {
        if expression.child_count()? != 1 {
            return Ok(None);
        }
        Ok(self
            .column(&expression.child(0)?)?
            .map(|(column, _)| build(column)))
    }
}

fn signature() -> SignatureBuilder {
    let named = |parameter: FunctionParameter| parameter.kind(FunctionParameterKind::Named);
    let mut signature = SignatureBuilder::new().param(
        FunctionParameter::new::<String>("tablename").kind(FunctionParameterKind::PositionalOnly),
    );
    // A NULL default makes "omitted" and "passed NULL" the same thing, which
    // is how the v1 build treats them.
    for name in STRING_PARAMS {
        signature = signature.param(named(FunctionParameter::with_default::<Option<String>>(
            name, None,
        )));
    }
    for name in INT_PARAMS {
        signature = signature.param(named(FunctionParameter::with_default::<Option<i32>>(
            name, None,
        )));
    }
    signature
}

#[duckdb_entrypoint_c_api_v2(extension_name = "athena", api_version = "v2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<()> {
    TableFunctionBuilder::new("athena_scan", signature(), AthenaScan)
        .with_projection_pushdown(true)
        .with_filter_pushdown()
        .with_progress()
        .register(extension)
}

#[cfg(test)]
mod tests {
    use super::Pushed;

    #[test]
    fn a_repeated_offer_is_kept_and_a_different_one_cancels_both() {
        let mut pushed = Pushed::default();
        assert_eq!(pushed.predicate(), None);
        pushed.offer(Some("a = 1".into()));
        pushed.offer(Some("a = 1".into()));
        assert_eq!(pushed.predicate(), Some("a = 1"));
        pushed.offer(Some("a = 2".into()));
        assert_eq!(pushed, Pushed::Conflict);
        // And it stays cancelled.
        pushed.offer(Some("a = 1".into()));
        assert_eq!(pushed.predicate(), None);
    }

    #[test]
    fn an_offer_of_nothing_after_a_predicate_is_a_conflict() {
        let mut pushed = Pushed::default();
        pushed.offer(None);
        assert_eq!(pushed, Pushed::Nothing);
        pushed.offer(Some("a = 1".into()));
        pushed.offer(None);
        assert_eq!(pushed, Pushed::Conflict);
    }
}
