use libduckdb_sys::{duckdb_data_chunk, duckdb_data_chunk_get_vector, idx_t};
use quack_rs::{types::TypeId, vector::VectorWriter};

/// A DuckDB output type resolved from an Athena/Glue type string. Most types are
/// a plain [`TypeId`]; `Decimal` additionally carries the width/scale needed to
/// build the DuckDB `DECIMAL` logical type at bind and to encode each value as
/// its backing integer at scan. Resolving once (at bind) keeps a single source
/// of truth, so the physical width we write always matches the registered type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
    Simple(TypeId),
    Decimal {
        width: u8,
        scale: u8,
    },
    /// A complex Athena type (array, map, struct) selected as `CAST(col AS JSON)`
    /// and written as `VARCHAR`. Athena's default rendering of these is lossy —
    /// `array['a,b', 'c']` prints as `[a,b, c]`, where the comma inside the
    /// element cannot be told from the separator — so the value would be
    /// unparseable. JSON escapes properly and keeps struct field names, which
    /// makes DuckDB's json functions usable on the result.
    Json,
}

// Maps Athena/Glue data types to DuckDB types.
// Only returns non-Varchar types when populate_column can write them correctly.
// Supported types are listed here: https://docs.aws.amazon.com/athena/latest/ug/data-types.html
pub fn map_type(col_type: &str) -> Result<ColType, String> {
    let col_type = ColType::Simple(match col_type {
        "boolean" => TypeId::Boolean,
        "tinyint" => TypeId::TinyInt,
        "smallint" => TypeId::SmallInt,
        "int" | "integer" => TypeId::Integer,
        "bigint" => TypeId::BigInt,
        "double" => TypeId::Double,
        "float" => TypeId::Float,
        // Athena returns these as strings; populate_column parses them into
        // DuckDB's fixed-width DATE (days since epoch) / TIMESTAMP (micros since
        // epoch) representations.
        "date" => TypeId::Date,
        "timestamp" => TypeId::Timestamp,
        // Glue spells parameterized decimals as `decimal(p,s)` (bare `decimal`
        // is the Hive default DECIMAL(10,0)). Registered as a native DECIMAL so
        // values keep numeric typing instead of coming back as strings.
        s if s == "decimal" || s.starts_with("decimal(") => return parse_decimal(s),
        "string" | "varchar" | "char" => TypeId::Varchar,
        // Complex types are requested as JSON rather than Athena's ambiguous
        // default text. Glue spells them `array<...>`, `map<...>`, `struct<...>`.
        s if s.starts_with("array<") || s.starts_with("map<") || s.starts_with("struct<") => {
            return Ok(ColType::Json)
        }
        _ => {
            return Err(format!("Unsupported data type: {col_type}"));
        }
    });

    Ok(col_type)
}

/// Parses a Glue decimal type string (`decimal`, `decimal(p)`, or `decimal(p,s)`)
/// into a [`ColType::Decimal`]. Bare `decimal` is Hive's default DECIMAL(10,0).
/// DuckDB `DECIMAL` supports width 1..=38 with scale <= width; anything outside
/// that (or malformed) is an error, so the caller falls back to Varchar.
fn parse_decimal(col_type: &str) -> Result<ColType, String> {
    let malformed = || format!("malformed decimal type: {col_type}");
    let inner = col_type.strip_prefix("decimal").unwrap_or_default().trim();
    let (width, scale) = if inner.is_empty() {
        (10u8, 0u8) // Hive default DECIMAL(10,0)
    } else {
        let params = inner
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(malformed)?;
        let mut parts = params.split(',');
        let width: u8 = parts
            .next()
            .unwrap_or_default()
            .trim()
            .parse()
            .map_err(|_| malformed())?;
        let scale: u8 = match parts.next() {
            Some(s) => s.trim().parse().map_err(|_| malformed())?,
            None => 0,
        };
        if parts.next().is_some() {
            return Err(malformed());
        }
        (width, scale)
    };
    if width == 0 || width > 38 || scale > width {
        return Err(format!("unsupported decimal(width={width}, scale={scale})"));
    }
    Ok(ColType::Decimal { width, scale })
}

/// Parses an Athena decimal string (e.g. `"123.450"`, `"-0.5"`, `"42"`) into the
/// backing integer for a DECIMAL of the given `scale` (value * 10^scale), or
/// `None` if malformed. Fractional digits beyond `scale` are truncated, matching
/// DuckDB's own decimal cast; fewer are zero-padded. No rounding.
fn parse_decimal_value(value: &str, scale: u8) -> Option<i128> {
    let value = value.trim();
    let (neg, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    let scale = scale as usize;
    let mut unscaled = String::with_capacity(int_part.len() + scale);
    unscaled.push_str(int_part);
    if frac_part.len() >= scale {
        unscaled.push_str(&frac_part[..scale]);
    } else {
        unscaled.push_str(frac_part);
        unscaled.extend(std::iter::repeat_n('0', scale - frac_part.len()));
    }
    let mut n: i128 = unscaled.parse().ok()?;
    if neg {
        n = -n;
    }
    Some(n)
}

/// Returns whether `unscaled` fits within `width` decimal digits, i.e. is a
/// legal unscaled value for `DECIMAL(width, _)`.
fn decimal_fits(unscaled: i128, width: u8) -> bool {
    let limit = 10i128.pow(width as u32) - 1;
    unscaled <= limit && unscaled >= -limit
}

/// Writes an unscaled decimal integer using the physical width DuckDB allocates
/// for a `DECIMAL(width, _)`: int16 for 1..=4, int32 for 5..=9, int64 for
/// 10..=18, int128 for 19..=38. Must match the width registered at bind, or the
/// write lands at the wrong stride. A value that doesn't fit in `width` digits
/// (possible even for the narrower physical types, whose range exceeds what the
/// declared width allows) is written as NULL rather than truncated by the `as`
/// cast.
unsafe fn write_decimal(writer: &mut VectorWriter, row_idx: usize, width: u8, unscaled: i128) {
    unsafe {
        if !decimal_fits(unscaled, width) {
            writer.set_null(row_idx);
            return;
        }
        match width {
            1..=4 => writer.write_i16(row_idx, unscaled as i16),
            5..=9 => writer.write_i32(row_idx, unscaled as i32),
            10..=18 => writer.write_i64(row_idx, unscaled as i64),
            _ => writer.write_i128(row_idx, unscaled),
        }
    }
}

/// Julian day number of the Unix epoch (1970-01-01), used to turn a `time::Date`
/// into days-since-epoch as DuckDB's `DATE` expects.
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;

const DATE_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
    time::macros::format_description!("[year]-[month]-[day]");
const TIMESTAMP_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] = time::macros::format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second][optional [.[subsecond digits:1+]]]"
);

/// Parses an Athena `date` (`YYYY-MM-DD`) to days since epoch, or `None` if
/// malformed. `time::Date::parse` validates the day against the actual month
/// length (leap years included), so an impossible date like `2024-02-31`
/// becomes NULL rather than silently normalizing to a different valid day.
fn parse_date(value: &str) -> Option<i32> {
    let date = time::Date::parse(value, DATE_FORMAT).ok()?;
    Some(date.to_julian_day() - UNIX_EPOCH_JULIAN_DAY)
}

/// Parses an Athena `timestamp` (`YYYY-MM-DD HH:MM:SS[.ffffff]`) to microseconds
/// since epoch, or `None` if malformed. Fractional seconds are truncated (not
/// rounded) to microsecond precision, matching Athena/DuckDB behavior.
fn parse_timestamp(value: &str) -> Option<i64> {
    let dt = time::PrimitiveDateTime::parse(value, TIMESTAMP_FORMAT).ok()?;
    let days = (dt.date().to_julian_day() - UNIX_EPOCH_JULIAN_DAY) as i64;
    let secs = dt.hour() as i64 * 3_600 + dt.minute() as i64 * 60 + dt.second() as i64;
    let micros = dt.microsecond() as i64;
    Some((days * 86_400 + secs) * 1_000_000 + micros)
}

/// Writes one Athena cell into the DuckDB output vector.
///
/// `value` is `None` when Athena returned SQL NULL (the datum has no
/// `varCharValue`). A numeric value that fails to parse is also written as
/// NULL rather than silently left as a valid zero.
pub unsafe fn populate_column(
    value: Option<&str>,
    col_type: ColType,
    output: duckdb_data_chunk,
    row_idx: usize,
    col_idx: usize,
) {
    unsafe {
        let vector = duckdb_data_chunk_get_vector(output, col_idx as idx_t);
        let mut writer = VectorWriter::new(vector);

        let Some(value) = value else {
            writer.set_null(row_idx);
            return;
        };

        let type_id = match col_type {
            ColType::Decimal { width, scale } => {
                match parse_decimal_value(value, scale) {
                    Some(unscaled) => write_decimal(&mut writer, row_idx, width, unscaled),
                    None => writer.set_null(row_idx),
                }
                return;
            }
            // JSON arrives as text and is written verbatim; DuckDB's json
            // functions parse it on demand.
            ColType::Json => TypeId::Varchar,
            ColType::Simple(type_id) => type_id,
        };

        match type_id {
            TypeId::Boolean => writer.write_bool(row_idx, value.eq_ignore_ascii_case("true")),
            TypeId::BigInt => match value.parse::<i64>() {
                Ok(v) => writer.write_i64(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::Integer => match value.parse::<i32>() {
                Ok(v) => writer.write_i32(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::TinyInt => match value.parse::<i8>() {
                Ok(v) => writer.write_i8(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::SmallInt => match value.parse::<i16>() {
                Ok(v) => writer.write_i16(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::Float => match value.parse::<f32>() {
                Ok(v) => writer.write_f32(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::Double => match value.parse::<f64>() {
                Ok(v) => writer.write_f64(row_idx, v),
                Err(_) => writer.set_null(row_idx),
            },
            TypeId::Date => match parse_date(value) {
                Some(days) => writer.write_date(row_idx, days),
                None => writer.set_null(row_idx),
            },
            TypeId::Timestamp => match parse_timestamp(value) {
                Some(micros) => writer.write_timestamp(row_idx, micros),
                None => writer.set_null(row_idx),
            },
            // Varchar and any other type: write as string.
            // SAFETY: only reached for types registered as Varchar with DuckDB.
            _ => writer.write_varchar(row_idx, value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decimal_fits, map_type, parse_date, parse_decimal_value, parse_timestamp, ColType,
    };
    use quack_rs::types::TypeId;

    #[test]
    fn map_type_maps_temporal_natively() {
        assert_eq!(map_type("date").unwrap(), ColType::Simple(TypeId::Date));
        assert_eq!(
            map_type("timestamp").unwrap(),
            ColType::Simple(TypeId::Timestamp)
        );
    }

    #[test]
    fn map_type_flags_complex_types_as_json() {
        // These are read as CAST(col AS JSON); Athena's plain text rendering of
        // them cannot be parsed back (`array['a,b','c']` prints as `[a,b, c]`).
        assert_eq!(map_type("array<string>").unwrap(), ColType::Json);
        assert_eq!(map_type("map<string,int>").unwrap(), ColType::Json);
        assert_eq!(map_type("struct<a:int,b:string>").unwrap(), ColType::Json);
        assert_eq!(
            map_type("array<struct<a:int,b:array<string>>>").unwrap(),
            ColType::Json
        );
        // Not complex: a plain string column keeps its own mapping.
        assert_eq!(
            map_type("string").unwrap(),
            ColType::Simple(TypeId::Varchar)
        );
    }

    #[test]
    fn map_type_maps_decimal_with_width_and_scale() {
        // Bare `decimal` is Hive's default DECIMAL(10,0).
        assert_eq!(
            map_type("decimal").unwrap(),
            ColType::Decimal {
                width: 10,
                scale: 0
            }
        );
        assert_eq!(
            map_type("decimal(18,3)").unwrap(),
            ColType::Decimal {
                width: 18,
                scale: 3
            }
        );
        // Scale defaults to 0 when only the width is given.
        assert_eq!(
            map_type("decimal(9)").unwrap(),
            ColType::Decimal { width: 9, scale: 0 }
        );
        // Out-of-range / malformed decimals error so the caller falls back to
        // Varchar rather than registering an invalid DECIMAL width.
        assert!(map_type("decimal(0,0)").is_err()); // width 0
        assert!(map_type("decimal(39,2)").is_err()); // width > 38
        assert!(map_type("decimal(4,6)").is_err()); // scale > width
        assert!(map_type("decimal(4,").is_err()); // malformed
    }

    #[test]
    fn parse_decimal_value_scales_and_pads() {
        assert_eq!(parse_decimal_value("123.45", 2), Some(12345));
        assert_eq!(parse_decimal_value("123.4", 2), Some(12340)); // pad to scale
        assert_eq!(parse_decimal_value("42", 2), Some(4200)); // no fraction
        assert_eq!(parse_decimal_value("-0.5", 3), Some(-500));
        assert_eq!(parse_decimal_value("+7.000", 0), Some(7)); // leading +
                                                               // Digits beyond scale truncate (no rounding), matching DuckDB's cast.
        assert_eq!(parse_decimal_value("1.239", 2), Some(123));
        // Wide value that only fits in int128 (DECIMAL(38, ...)).
        assert_eq!(
            parse_decimal_value("12345678901234567890.12", 2),
            Some(1_234_567_890_123_456_789_012)
        );
    }

    #[test]
    fn decimal_fits_bounds_by_width() {
        assert!(decimal_fits(9999, 4));
        assert!(!decimal_fits(10000, 4));
        assert!(decimal_fits(-9999, 4));
        assert!(!decimal_fits(-10000, 4));
        // int32-backed width whose digit limit is narrower than i32's range.
        assert!(decimal_fits(999_999_999, 9));
        assert!(!decimal_fits(1_000_000_000, 9));
    }

    #[test]
    fn parse_decimal_value_rejects_garbage() {
        assert_eq!(parse_decimal_value("abc", 2), None);
        assert_eq!(parse_decimal_value("1.2.3", 2), None);
        assert_eq!(parse_decimal_value("", 2), None);
        assert_eq!(parse_decimal_value("1,234.5", 2), None); // thousands separators
    }

    #[test]
    fn parse_date_epoch_and_offsets() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("1970-01-02"), Some(1));
        assert_eq!(parse_date("1969-12-31"), Some(-1));
        assert_eq!(parse_date("2000-01-01"), Some(10957)); // known Y2K offset
        assert_eq!(parse_date("2020-02-29"), Some(18321)); // valid leap day
    }

    #[test]
    fn parse_date_rejects_garbage() {
        assert_eq!(parse_date("not-a-date"), None);
        assert_eq!(parse_date("2024-13-01"), None); // month out of range
        assert_eq!(parse_date("2024-01"), None); // missing day
        assert_eq!(parse_date(""), None);
    }

    #[test]
    fn parse_date_rejects_impossible_calendar_days() {
        assert_eq!(parse_date("2024-02-31"), None); // Feb has no 31st
        assert_eq!(parse_date("2023-02-29"), None); // 2023 not a leap year
        assert_eq!(parse_date("2024-02-29"), Some(19782)); // 2024 is a leap year
        assert_eq!(parse_date("2024-04-31"), None); // April has 30 days
        assert_eq!(parse_date("2024-00-10"), None); // month 0
        assert_eq!(parse_date("2024-04-00"), None); // day 0
    }

    #[test]
    fn parse_timestamp_seconds_and_fractions() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01 00:00:01"), Some(1_000_000));
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.123"), Some(123_000));
        // Sub-microsecond digits truncate, not round.
        assert_eq!(
            parse_timestamp("1970-01-01 00:00:00.1234567"),
            Some(123_456)
        );
        assert_eq!(
            parse_timestamp("2000-01-01 00:00:00"),
            Some(10957 * 86_400 * 1_000_000)
        );
    }

    #[test]
    fn parse_timestamp_rejects_garbage() {
        assert_eq!(parse_timestamp("2024-01-15"), None); // no time component
        assert_eq!(parse_timestamp("2024-01-15 25:00:00"), None); // hour out of range
        assert_eq!(parse_timestamp("2024-01-15 00:00:60"), None); // no leap seconds
        assert_eq!(parse_timestamp("2024-01-15 00:00:00.12x"), None); // non-digit frac
        assert_eq!(parse_timestamp("2024-01-15 00:00"), None); // missing seconds
                                                               // A synthetic multi-million-year input is out of `time`'s supported
                                                               // range and is rejected outright, not normalized into a huge value.
        assert_eq!(parse_date("5000000-01-01"), None);
        assert_eq!(parse_timestamp("5000000-01-01 00:00:00"), None);
    }
}
