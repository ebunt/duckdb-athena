use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
    process::Command,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct AwsIntegration {
    database: String,
    table: String,
    output_location: String,
    workgroup: Option<String>,
    extension_path: PathBuf,
}

impl AwsIntegration {
    fn from_env() -> TestResult<Option<Self>> {
        let Some(database) = optional_env("ATHENA_TEST_DATABASE") else {
            skip();
            return Ok(None);
        };
        let Some(table) = optional_env("ATHENA_TEST_TABLE") else {
            skip();
            return Ok(None);
        };
        let Some(output_location) = optional_env("ATHENA_TEST_OUTPUT") else {
            skip();
            return Ok(None);
        };

        let extension_path = env::var_os("ATHENA_EXTENSION_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(default_extension_path);
        if !extension_path.exists() {
            return Err(format!(
                "extension not found at {}; run `make` first or set ATHENA_EXTENSION_PATH",
                extension_path.display()
            )
            .into());
        }

        Ok(Some(Self {
            database,
            table,
            output_location,
            workgroup: optional_env("ATHENA_TEST_WORKGROUP"),
            extension_path,
        }))
    }

    fn scan_args(&self, extra_args: &[String]) -> String {
        let mut args = vec![
            sql_string(&self.table),
            sql_string(&self.output_location),
            format!("database={}", sql_string(&self.database)),
        ];
        if let Some(workgroup) = &self.workgroup {
            args.push(format!("workgroup={}", sql_string(workgroup)));
        }
        args.extend(extra_args.iter().cloned());
        args.join(", ")
    }

    fn run_duckdb(&self, sql: &str) -> TestResult<String> {
        run_duckdb(&self.extension_path, sql)
    }
}

#[test]
fn aws_basic_scan_limit() -> TestResult {
    let Some(ctx) = AwsIntegration::from_env()? else {
        return Ok(());
    };

    let sql = format!(
        "SELECT COUNT(*) FROM athena_scan({});",
        ctx.scan_args(&["maxrows=1".to_owned()])
    );
    let output = ctx.run_duckdb(&sql)?;

    assert!(
        output.trim().parse::<i64>().is_ok(),
        "expected COUNT(*) output, got {output:?}"
    );
    Ok(())
}

#[test]
fn aws_projection_scan() -> TestResult {
    let Some(ctx) = AwsIntegration::from_env()? else {
        return Ok(());
    };
    let Some(column) = optional_env("ATHENA_TEST_PROJECTION_COLUMN") else {
        eprintln!(
            "skipping AWS projection integration test: ATHENA_TEST_PROJECTION_COLUMN not set"
        );
        return Ok(());
    };

    let sql = format!(
        "SELECT {} FROM athena_scan({}) LIMIT 1;",
        quote_identifier(&column),
        ctx.scan_args(&["maxrows=1".to_owned()])
    );
    ctx.run_duckdb(&sql)?;

    Ok(())
}

#[test]
fn aws_manual_predicate_scan() -> TestResult {
    let Some(ctx) = AwsIntegration::from_env()? else {
        return Ok(());
    };
    let Some(predicate) = optional_env("ATHENA_TEST_PREDICATE") else {
        eprintln!("skipping AWS predicate integration test: ATHENA_TEST_PREDICATE not set");
        return Ok(());
    };

    let sql = format!(
        "SELECT COUNT(*) FROM athena_scan({});",
        ctx.scan_args(&[
            "maxrows=10".to_owned(),
            format!("predicate={}", sql_string(&predicate)),
        ])
    );
    let output = ctx.run_duckdb(&sql)?;

    assert!(
        output.trim().parse::<i64>().is_ok(),
        "expected COUNT(*) output, got {output:?}"
    );
    Ok(())
}

#[test]
fn aws_paginated_scan() -> TestResult {
    let Some(ctx) = AwsIntegration::from_env()? else {
        return Ok(());
    };
    let Some(rows) = optional_env("ATHENA_TEST_PAGINATION_ROWS") else {
        eprintln!("skipping AWS pagination integration test: ATHENA_TEST_PAGINATION_ROWS not set");
        return Ok(());
    };
    let rows = rows.parse::<i64>()?;
    if rows <= 1000 {
        return Err("ATHENA_TEST_PAGINATION_ROWS must be greater than 1000".into());
    }

    let sql = format!(
        "SELECT COUNT(*) FROM athena_scan({});",
        ctx.scan_args(&[format!("maxrows={rows}")])
    );
    let output = ctx.run_duckdb(&sql)?;
    let observed_rows = output.trim().parse::<i64>()?;

    assert_eq!(
        observed_rows, rows,
        "pagination test table must contain at least ATHENA_TEST_PAGINATION_ROWS rows"
    );
    Ok(())
}

fn run_duckdb(extension_path: &Path, sql: &str) -> TestResult<String> {
    let full_sql = format!("LOAD '{}'; {sql}", path_sql_string(extension_path));
    let output = Command::new("duckdb")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["-unsigned", "-csv", "-noheader", "-c", &full_sql])
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "duckdb failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(String::from_utf8(output.stdout)?)
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn default_extension_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("release")
        .join("duckdb_athena.duckdb_extension")
}

fn skip() {
    eprintln!(
        "skipping AWS integration tests: set ATHENA_TEST_DATABASE, ATHENA_TEST_TABLE, and ATHENA_TEST_OUTPUT"
    );
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn path_sql_string(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
