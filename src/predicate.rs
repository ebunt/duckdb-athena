//! DuckDB filters, rendered as an Athena predicate.
//!
//! Used only by the C API v2 build, whose filter-pushdown callback is handed
//! bound expressions. The adapter lowers what it recognises into [`Filter`];
//! everything about what may be sent to Athena, and how it is spelled, is
//! decided here, where it can be tested without an engine.
//!
//! One rule governs all of it: **a pushed predicate may keep rows DuckDB will
//! drop, never drop rows DuckDB would keep.** The extension does not accept
//! filters -- DuckDB still evaluates every one on the rows that come back --
//! so pushdown only has to be a correct pre-filter, and anything unrecognised
//! is simply not pushed. That is also why `BETWEEN` can be pushed even though
//! the v2 API does not say whether its bounds are inclusive
//! (duckdb/duckdb#27729): the inclusive form is never the stricter one.

use crate::scan::quote_identifier;
use crate::types::{parse_date, parse_timestamp, ColType};
use quack_rs::types::TypeId;

/// A constant, already validated for the column it is compared with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    Int(i64),
    Bool(bool),
    Text(String),
    /// `YYYY-MM-DD`
    Date(String),
    /// `YYYY-MM-DD HH:MM:SS[.ffffff]`
    Timestamp(String),
    /// Digits with an optional sign and fraction.
    Decimal(String),
}

impl Literal {
    /// Reads a DuckDB constant, given as DuckDB prints it, for a column of
    /// `col_type`. `None` means "do not push a filter on this": the type is
    /// not one whose comparisons are known to agree between the two engines,
    /// or the text is not what that type prints as (an infinite date, say).
    pub fn for_column(col_type: ColType, text: &str) -> Option<Self> {
        match col_type {
            ColType::Simple(TypeId::Boolean) => match text {
                "true" => Some(Self::Bool(true)),
                "false" => Some(Self::Bool(false)),
                _ => None,
            },
            ColType::Simple(
                TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt,
            ) => text.parse().ok().map(Self::Int),
            ColType::Simple(TypeId::Varchar) => Some(Self::Text(text.to_owned())),
            ColType::Simple(TypeId::Date) => parse_date(text).map(|_| Self::Date(text.to_owned())),
            ColType::Simple(TypeId::Timestamp) => {
                parse_timestamp(text).map(|_| Self::Timestamp(text.to_owned()))
            }
            ColType::Decimal { .. } => {
                is_plain_decimal(text).then(|| Self::Decimal(text.to_owned()))
            }
            // Float and double are left to DuckDB: see `types::filter_pushable`.
            ColType::Simple(_) | ColType::Json => None,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::Bool(v) => v.to_string(),
            // Athena (Trino) string literals have one escape: a doubled quote.
            Self::Text(v) => format!("'{}'", v.replace('\'', "''")),
            Self::Date(v) => format!("DATE '{v}'"),
            Self::Timestamp(v) => format!("TIMESTAMP '{v}'"),
            Self::Decimal(v) => format!("DECIMAL '{v}'"),
        }
    }
}

/// `-12`, `3.50`, `0.125`: what DuckDB prints for a DECIMAL, and nothing else.
fn is_plain_decimal(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, "0"));
    !int_part.is_empty()
        && !frac_part.is_empty()
        && int_part.bytes().all(|b| b.is_ascii_digit())
        && frac_part.bytes().all(|b| b.is_ascii_digit())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    /// The operator for the same comparison with its operands swapped:
    /// `5 < col` is `col > 5`.
    pub fn flipped(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
            Self::Eq | Self::Ne => self,
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }
}

/// The subset of DuckDB filter expressions this extension sends to Athena.
/// Columns are Athena column names, unquoted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    Compare {
        column: String,
        op: Cmp,
        value: Literal,
    },
    /// Rendered with inclusive bounds, whatever the original's were.
    Between {
        column: String,
        low: Literal,
        high: Literal,
    },
    In {
        column: String,
        values: Vec<Literal>,
    },
    IsNull(String),
    IsNotNull(String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

impl Filter {
    pub fn render(&self) -> String {
        match self {
            Self::Compare { column, op, value } => {
                format!(
                    "{} {} {}",
                    quote_identifier(column),
                    op.sql(),
                    value.render()
                )
            }
            Self::Between { column, low, high } => {
                let column = quote_identifier(column);
                format!(
                    "{column} >= {} AND {column} <= {}",
                    low.render(),
                    high.render()
                )
            }
            Self::In { column, values } => {
                let values: Vec<String> = values.iter().map(Literal::render).collect();
                format!("{} IN ({})", quote_identifier(column), values.join(", "))
            }
            Self::IsNull(column) => format!("{} IS NULL", quote_identifier(column)),
            Self::IsNotNull(column) => format!("{} IS NOT NULL", quote_identifier(column)),
            Self::And(parts) => join(parts, " AND "),
            Self::Or(parts) => join(parts, " OR "),
        }
    }
}

/// Every operand parenthesized, so precedence never depends on what is inside.
fn join(parts: &[Filter], separator: &str) -> String {
    let parts: Vec<String> = parts.iter().map(|p| format!("({})", p.render())).collect();
    parts.join(separator)
}

/// All of a scan's pushed filters as one predicate, or `None` for none.
pub fn render_conjunction(filters: &[Filter]) -> Option<String> {
    match filters {
        [] => None,
        [only] => Some(only.render()),
        many => Some(join(many, " AND ")),
    }
}

#[cfg(test)]
mod tests {
    use super::{render_conjunction, Cmp, Filter, Literal};
    use crate::types::ColType;
    use quack_rs::types::TypeId;

    fn compare(column: &str, op: Cmp, value: Literal) -> Filter {
        Filter::Compare {
            column: column.to_owned(),
            op,
            value,
        }
    }

    #[test]
    fn literals_are_read_per_column_type() {
        let simple = |id| ColType::Simple(id);
        assert_eq!(
            Literal::for_column(simple(TypeId::BigInt), "-42"),
            Some(Literal::Int(-42))
        );
        assert_eq!(Literal::for_column(simple(TypeId::Integer), "4.2"), None);
        assert_eq!(
            Literal::for_column(simple(TypeId::Boolean), "true"),
            Some(Literal::Bool(true))
        );
        assert_eq!(Literal::for_column(simple(TypeId::Boolean), "1"), None);
        assert_eq!(
            Literal::for_column(simple(TypeId::Date), "2024-02-29"),
            Some(Literal::Date("2024-02-29".into()))
        );
        // DuckDB prints these for dates Athena has no literal for.
        assert_eq!(Literal::for_column(simple(TypeId::Date), "infinity"), None);
        assert_eq!(
            Literal::for_column(simple(TypeId::Date), "0044-03-15 (BC)"),
            None
        );
        assert_eq!(
            Literal::for_column(simple(TypeId::Timestamp), "2024-01-01 12:30:00.5"),
            Some(Literal::Timestamp("2024-01-01 12:30:00.5".into()))
        );
        let decimal = ColType::Decimal {
            width: 10,
            scale: 2,
        };
        assert_eq!(
            Literal::for_column(decimal, "-1.50"),
            Some(Literal::Decimal("-1.50".into()))
        );
        assert_eq!(Literal::for_column(decimal, "1e3"), None);
        assert_eq!(Literal::for_column(decimal, "1.50' OR '1'='1"), None);
    }

    #[test]
    fn floats_and_json_are_never_pushed() {
        assert_eq!(
            Literal::for_column(ColType::Simple(TypeId::Double), "0.1"),
            None
        );
        assert_eq!(
            Literal::for_column(ColType::Simple(TypeId::Float), "0.1"),
            None
        );
        assert_eq!(Literal::for_column(ColType::Json, "[1]"), None);
    }

    #[test]
    fn strings_cannot_break_out_of_their_quotes() {
        let filter = compare("name", Cmp::Eq, Literal::Text("O'Brien' OR 1=1 --".into()));
        assert_eq!(filter.render(), r#""name" = 'O''Brien'' OR 1=1 --'"#);
    }

    #[test]
    fn identifiers_are_quoted() {
        let filter = Filter::IsNull(r#"odd "name""#.into());
        assert_eq!(filter.render(), r#""odd ""name""" IS NULL"#);
    }

    #[test]
    fn each_shape_renders_as_athena_sql() {
        assert_eq!(
            compare("d", Cmp::Ge, Literal::Date("2024-01-01".into())).render(),
            r#""d" >= DATE '2024-01-01'"#
        );
        assert_eq!(
            compare(
                "ts",
                Cmp::Lt,
                Literal::Timestamp("2024-01-01 00:00:00".into())
            )
            .render(),
            r#""ts" < TIMESTAMP '2024-01-01 00:00:00'"#
        );
        assert_eq!(
            compare("amount", Cmp::Ne, Literal::Decimal("1.50".into())).render(),
            r#""amount" <> DECIMAL '1.50'"#
        );
        assert_eq!(
            Filter::Between {
                column: "qty".into(),
                low: Literal::Int(1),
                high: Literal::Int(9),
            }
            .render(),
            r#""qty" >= 1 AND "qty" <= 9"#
        );
        assert_eq!(
            Filter::In {
                column: "yr".into(),
                values: vec![Literal::Text("2023".into()), Literal::Text("2024".into())],
            }
            .render(),
            r#""yr" IN ('2023', '2024')"#
        );
        assert_eq!(Filter::IsNotNull("c".into()).render(), r#""c" IS NOT NULL"#);
    }

    #[test]
    fn nesting_is_always_parenthesized() {
        let either = Filter::Or(vec![
            compare("a", Cmp::Eq, Literal::Int(1)),
            Filter::And(vec![
                compare("b", Cmp::Gt, Literal::Int(2)),
                Filter::IsNull("c".into()),
            ]),
        ]);
        assert_eq!(
            either.render(),
            r#"("a" = 1) OR (("b" > 2) AND ("c" IS NULL))"#
        );
    }

    #[test]
    fn a_conjunction_of_one_needs_no_parentheses() {
        assert_eq!(render_conjunction(&[]), None);
        let one = compare("a", Cmp::Eq, Literal::Int(1));
        assert_eq!(
            render_conjunction(std::slice::from_ref(&one)).as_deref(),
            Some(r#""a" = 1"#)
        );
        let two = [one, Filter::IsNull("b".into())];
        assert_eq!(
            render_conjunction(&two).as_deref(),
            Some(r#"("a" = 1) AND ("b" IS NULL)"#)
        );
    }

    #[test]
    fn flipping_swaps_only_the_ordered_comparisons() {
        assert_eq!(Cmp::Lt.flipped(), Cmp::Gt);
        assert_eq!(Cmp::Ge.flipped(), Cmp::Le);
        assert_eq!(Cmp::Eq.flipped(), Cmp::Eq);
        assert_eq!(Cmp::Ne.flipped(), Cmp::Ne);
    }
}
