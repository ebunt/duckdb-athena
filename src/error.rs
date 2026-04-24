#[derive(Debug)]
pub enum Error {
    DuckDB(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (catalog, message) = match self {
            Self::DuckDB(s) => ("DuckDB", s.as_str()),
        };
        write!(f, "Athena({catalog}): {message}")
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Box<dyn std::error::Error>> for Error {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        Self::DuckDB(value.to_string())
    }
}
