pub(crate) enum SelectList {
    All,
    RowsOnly,
    Columns(Vec<String>),
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified_table(database: &str, tablename: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(database),
        quote_identifier(tablename)
    )
}

fn render_select_list(select: &SelectList) -> String {
    match select {
        SelectList::All => "*".to_owned(),
        SelectList::RowsOnly => "1".to_owned(),
        SelectList::Columns(columns) => columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

pub(crate) fn build_athena_query(
    database: &str,
    tablename: &str,
    select: &SelectList,
    predicate: Option<&str>,
    maxrows: i32,
) -> String {
    let mut query = format!(
        "SELECT {} FROM {}",
        render_select_list(select),
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

pub(crate) fn validate_predicate(predicate: &str) -> anyhow::Result<String> {
    let predicate = predicate.trim();
    if predicate.is_empty() {
        anyhow::bail!("predicate must not be empty");
    }
    if predicate.contains('\0') {
        anyhow::bail!("predicate must not contain NUL bytes");
    }
    if predicate.contains(';') {
        anyhow::bail!("predicate must be a single WHERE expression without semicolons");
    }
    if predicate.contains("--") || predicate.contains("/*") || predicate.contains("*/") {
        anyhow::bail!("predicate must not contain SQL comments");
    }

    let uppercase = predicate.to_ascii_uppercase();
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
            anyhow::bail!("predicate must be a WHERE expression, not a full SQL statement");
        }
    }

    Ok(predicate.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{build_athena_query, qualified_table, validate_predicate, SelectList};

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

    #[test]
    fn build_query_includes_predicate_before_limit() {
        assert_eq!(
            build_athena_query(
                "analytics",
                "events",
                &SelectList::All,
                Some("year = 2024"),
                100
            ),
            "SELECT * FROM \"analytics\".\"events\" WHERE year = 2024 LIMIT 100"
        );
    }

    #[test]
    fn build_query_omits_limit_for_non_positive_limit() {
        assert_eq!(
            build_athena_query(
                "analytics",
                "events",
                &SelectList::All,
                Some("year = 2024"),
                0
            ),
            "SELECT * FROM \"analytics\".\"events\" WHERE year = 2024"
        );
    }

    #[test]
    fn build_query_supports_projection() {
        assert_eq!(
            build_athena_query(
                "analytics",
                "events",
                &SelectList::Columns(vec!["event_type".to_owned(), "odd\"col".to_owned()]),
                None,
                10,
            ),
            "SELECT \"event_type\", \"odd\"\"col\" FROM \"analytics\".\"events\" LIMIT 10"
        );
    }

    #[test]
    fn build_query_supports_rows_only_projection() {
        assert_eq!(
            build_athena_query("analytics", "events", &SelectList::RowsOnly, None, 10),
            "SELECT 1 FROM \"analytics\".\"events\" LIMIT 10"
        );
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
}
