//! Streaming reader for the CSV result file Athena writes to S3.
//!
//! `GetQueryResults` caps a page at 1000 rows, so a large result costs one API
//! round trip per 1000 rows (~135s for a million). Athena has already written
//! the whole result set as a single CSV object at the execution's
//! `ResultConfiguration.OutputLocation`, so one `GetObject` replaces all of
//! them; this module turns that byte stream into rows a chunk at a time.

use aws_sdk_s3::primitives::ByteStream;
use std::collections::VecDeque;

/// One result row as owned cells. `None` is SQL NULL: Athena's CSV writes NULL
/// as an unquoted empty field and an empty string as `""`, so quoting — not
/// emptiness — is what tells them apart.
pub type ResultRow = Vec<Option<String>>;

/// Splits `s3://bucket/key` into its parts.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), String> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| format!("result location is not an s3 URI: {uri}"))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| format!("result location has no key: {uri}"))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(format!("result location has an empty bucket or key: {uri}"));
    }
    Ok((bucket.to_string(), key.to_string()))
}

/// Incremental RFC 4180 parser for Athena's CSV dialect: every non-NULL value is
/// quoted (numbers included), NULL is an unquoted empty field, `""` inside a
/// quoted field is a literal quote, and a field may span newlines.
#[derive(Default)]
pub struct CsvParser {
    field: Vec<u8>,
    row: ResultRow,
    /// The field being read opened with a quote, so an empty result is `""`
    /// (an empty string) rather than NULL.
    quoted: bool,
    in_quotes: bool,
    /// Saw `"` inside a quoted field: either an escaped quote or the closing one,
    /// decided by the next byte.
    pending_quote: bool,
}

impl CsvParser {
    /// Feeds a chunk of bytes, appending every row it completes to `out`.
    pub fn push(&mut self, bytes: &[u8], out: &mut Vec<ResultRow>) -> Result<(), String> {
        for &b in bytes {
            if self.in_quotes {
                if self.pending_quote {
                    self.pending_quote = false;
                    if b == b'"' {
                        self.field.push(b'"');
                    } else {
                        self.in_quotes = false;
                        self.push_unquoted(b, out)?;
                    }
                } else if b == b'"' {
                    self.pending_quote = true;
                } else {
                    self.field.push(b);
                }
            } else {
                self.push_unquoted(b, out)?;
            }
        }
        Ok(())
    }

    fn push_unquoted(&mut self, b: u8, out: &mut Vec<ResultRow>) -> Result<(), String> {
        match b {
            b'"' => {
                self.in_quotes = true;
                self.quoted = true;
            }
            b',' => self.end_field()?,
            b'\n' => {
                self.end_field()?;
                out.push(std::mem::take(&mut self.row));
            }
            b'\r' => {}
            _ => self.field.push(b),
        }
        Ok(())
    }

    fn end_field(&mut self) -> Result<(), String> {
        let bytes = std::mem::take(&mut self.field);
        let cell = if self.quoted || !bytes.is_empty() {
            Some(
                String::from_utf8(bytes)
                    .map_err(|_| "Athena result CSV is not valid UTF-8".to_string())?,
            )
        } else {
            None
        };
        self.quoted = false;
        self.row.push(cell);
        Ok(())
    }

    /// Flushes a final row when the CSV does not end with a newline.
    pub fn finish(&mut self, out: &mut Vec<ResultRow>) -> Result<(), String> {
        if self.pending_quote {
            self.pending_quote = false;
            self.in_quotes = false;
        }
        if self.in_quotes {
            return Err("Athena result CSV ended inside a quoted field".to_string());
        }
        if !self.field.is_empty() || !self.row.is_empty() || self.quoted {
            self.end_field()?;
            out.push(std::mem::take(&mut self.row));
        }
        Ok(())
    }
}

/// Pulls rows from the result object, fetching more bytes only when the buffered
/// rows run out, so peak memory stays near one chunk rather than the whole file.
/// Fails when the body ended early. A stream cut on a row boundary parses
/// cleanly and would otherwise return a prefix of the result while reporting
/// success — silently wrong answers, so this is checked rather than assumed.
fn check_complete(bytes_read: u64, content_length: Option<u64>) -> Result<(), String> {
    match content_length {
        Some(expected) if bytes_read != expected => Err(format!(
            "Athena result truncated: read {bytes_read} of {expected} bytes"
        )),
        _ => Ok(()),
    }
}

pub struct CsvRowStream {
    body: ByteStream,
    parser: CsvParser,
    pending: VecDeque<ResultRow>,
    eof: bool,
    /// The CSV's first row is Athena's column header, dropped once.
    header_skipped: bool,
    bytes_read: u64,
    /// `Content-Length` of the result object, when S3 reported one.
    content_length: Option<u64>,
}

impl CsvRowStream {
    pub fn new(body: ByteStream, content_length: Option<u64>) -> Self {
        Self {
            body,
            parser: CsvParser::default(),
            pending: VecDeque::new(),
            eof: false,
            header_skipped: false,
            bytes_read: 0,
            content_length,
        }
    }

    /// Returns up to `max` rows; an empty vec means end of stream.
    pub fn next_rows(&mut self, max: usize) -> Result<Vec<ResultRow>, String> {
        let wanted = max + usize::from(!self.header_skipped);
        while !self.eof && self.pending.len() < wanted {
            let mut rows = Vec::new();
            match crate::RUNTIME
                .block_on(self.body.try_next())
                .map_err(|e| format!("reading Athena result from S3: {e}"))?
            {
                Some(chunk) => {
                    self.bytes_read += chunk.len() as u64;
                    self.parser.push(&chunk, &mut rows)?;
                }
                None => {
                    check_complete(self.bytes_read, self.content_length)?;
                    self.parser.finish(&mut rows)?;
                    self.eof = true;
                }
            }
            self.pending.extend(rows);
        }

        if !self.header_skipped && self.pending.pop_front().is_some() {
            self.header_skipped = true;
        }

        let take = max.min(self.pending.len());
        Ok(self.pending.drain(..take).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{check_complete, parse_s3_uri, CsvParser, ResultRow};

    fn parse(input: &str) -> Vec<ResultRow> {
        let mut parser = CsvParser::default();
        let mut out = Vec::new();
        parser.push(input.as_bytes(), &mut out).unwrap();
        parser.finish(&mut out).unwrap();
        out
    }

    fn cells(row: &ResultRow) -> Vec<Option<&str>> {
        row.iter().map(|c| c.as_deref()).collect()
    }

    #[test]
    fn parses_athenas_own_output_verbatim() {
        // Byte-for-byte what Athena wrote for
        //   SELECT 'a,b', '', CAST(NULL AS VARCHAR), 'he said "hi"', 'l1\nl2', 42, CAST(NULL AS INTEGER)
        let rows = parse(
            "\"c_comma\",\"c_empty\",\"c_null\",\"c_quote\",\"c_newline\",\"c_int\",\"c_nullint\"\n\
             \"a,b\",\"\",,\"he said \"\"hi\"\"\",\"l1\nl2\",\"42\",\n",
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(
            cells(&rows[1]),
            vec![
                Some("a,b"),            // comma inside a quoted field
                Some(""),               // "" is an empty string
                None,                   // unquoted empty is NULL
                Some("he said \"hi\""), // doubled quotes unescape
                Some("l1\nl2"),         // newline inside a quoted field
                Some("42"),             // numbers are quoted too
                None,                   // trailing NULL before the line end
            ]
        );
    }

    #[test]
    fn null_and_empty_string_stay_distinct() {
        // The whole reason the parser tracks quoting: collapsing these would
        // turn every empty string in a result into a NULL.
        let rows = parse("\"a\",\"b\"\n\"\",\n");
        assert_eq!(cells(&rows[1]), vec![Some(""), None]);
    }

    #[test]
    fn rows_split_across_chunk_boundaries_are_reassembled() {
        // S3 hands us arbitrary byte chunks, so every parser state must survive
        // being cut in half -- here inside a quoted field, on the escape pair,
        // and between the value and its delimiter.
        let whole = "\"h1\",\"h2\"\n\"a\"\"b\",\"c,d\"\n";
        for split in 1..whole.len() {
            let mut parser = CsvParser::default();
            let mut out = Vec::new();
            parser.push(&whole.as_bytes()[..split], &mut out).unwrap();
            parser.push(&whole.as_bytes()[split..], &mut out).unwrap();
            parser.finish(&mut out).unwrap();
            assert_eq!(
                cells(&out[1]),
                vec![Some("a\"b"), Some("c,d")],
                "split at {split}"
            );
        }
    }

    #[test]
    fn final_row_without_trailing_newline_is_emitted() {
        let rows = parse("\"h\"\n\"last\"");
        assert_eq!(cells(&rows[1]), vec![Some("last")]);
    }

    #[test]
    fn crlf_line_endings_do_not_leak_into_values() {
        let rows = parse("\"h\"\r\n\"v\"\r\n");
        assert_eq!(cells(&rows[1]), vec![Some("v")]);
    }

    #[test]
    fn unterminated_quoted_field_is_an_error() {
        // Truncated download must fail loudly, not silently drop the tail.
        let mut parser = CsvParser::default();
        let mut out = Vec::new();
        parser.push(b"\"h\"\n\"unfinished", &mut out).unwrap();
        assert!(parser.finish(&mut out).is_err());
    }

    #[test]
    fn short_read_is_an_error_not_a_partial_result() {
        // The dangerous case: a stream cut between rows parses cleanly, so
        // without this the scan would report success on a prefix of the data.
        assert!(check_complete(4_000_000, Some(4_281_056)).is_err());
        assert!(check_complete(0, Some(1)).is_err());
    }

    #[test]
    fn complete_or_unknown_length_reads_are_accepted() {
        assert!(check_complete(4_281_056, Some(4_281_056)).is_ok());
        // S3 not reporting a length must not fail every scan.
        assert!(check_complete(4_281_056, None).is_ok());
    }

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        assert_eq!(
            parse_s3_uri("s3://bucket/path/to/file.csv").unwrap(),
            ("bucket".to_string(), "path/to/file.csv".to_string())
        );
    }

    #[test]
    fn parse_s3_uri_rejects_malformed_locations() {
        assert!(parse_s3_uri("https://bucket/key").is_err());
        assert!(parse_s3_uri("s3://bucket-only").is_err());
        assert!(parse_s3_uri("s3:///key").is_err());
    }
}
