use std::{
    env, fs, io,
    path::{Path, PathBuf},
    process::ExitCode,
};

const FIELD_SIZE: usize = 32;
const NUM_FIELDS: usize = 8;
const SIGNATURE_SIZE: usize = 256;
const FOOTER_SIZE: usize = FIELD_SIZE * NUM_FIELDS + SIGNATURE_SIZE;

const MAGIC_VALUE: &[u8] = b"4";
const ABI_TYPE: &[u8] = b"C_STRUCT";
const DUCKDB_CAPI_VERSION: &[u8] = b"v1.2.0";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 3 {
        return Err(format!(
            "usage: {} <shared-library> <duckdb-extension>",
            args.first()
                .map(PathBuf::from)
                .as_deref()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .unwrap_or("package_extension")
        ));
    }

    let source = PathBuf::from(&args[1]);
    let destination = PathBuf::from(&args[2]);
    package_extension(&source, &destination).map_err(|err| {
        format!(
            "failed to package {} as {}: {err}",
            source.display(),
            destination.display()
        )
    })?;

    println!("Extension ready: {}", destination.display());
    println!(
        "  platform:    {}",
        String::from_utf8_lossy(&duckdb_platform())
    );
    println!("  ABI type:    {}", String::from_utf8_lossy(ABI_TYPE));
    println!(
        "  C API ver:   {}",
        String::from_utf8_lossy(DUCKDB_CAPI_VERSION)
    );

    Ok(())
}

fn package_extension(source: &Path, destination: &Path) -> io::Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut extension = fs::read(source)?;
    extension.extend_from_slice(&build_footer());
    fs::write(destination, extension)
}

fn build_footer() -> [u8; FOOTER_SIZE] {
    let extension_version = env::var("DUCKDB_EXTENSION_VERSION")
        .unwrap_or_else(|_| format!("v{}", env!("CARGO_PKG_VERSION")));
    let platform = duckdb_platform();

    let fields_read_order = [
        make_field(MAGIC_VALUE),
        make_field(&platform),
        make_field(DUCKDB_CAPI_VERSION),
        make_field(extension_version.as_bytes()),
        make_field(ABI_TYPE),
        make_field(b""),
        make_field(b""),
        make_field(b""),
    ];

    let mut footer = [0; FOOTER_SIZE];
    for (idx, field) in fields_read_order.iter().rev().enumerate() {
        let start = idx * FIELD_SIZE;
        footer[start..start + FIELD_SIZE].copy_from_slice(field);
    }

    footer
}

fn make_field(value: &[u8]) -> [u8; FIELD_SIZE] {
    assert!(
        value.len() <= FIELD_SIZE,
        "DuckDB extension footer field is longer than {FIELD_SIZE} bytes: {}",
        String::from_utf8_lossy(value)
    );

    let mut field = [0; FIELD_SIZE];
    field[..value.len()].copy_from_slice(value);
    field
}

fn duckdb_platform() -> Vec<u8> {
    if let Ok(platform) = env::var("DUCKDB_PLATFORM") {
        return platform.into_bytes();
    }

    let os = match env::consts::OS {
        "macos" => "osx",
        "windows" => "windows",
        _ => "linux",
    };
    let arch = match env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        _ => "amd64",
    };

    format!("{os}_{arch}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::{
        build_footer, make_field, ABI_TYPE, DUCKDB_CAPI_VERSION, FIELD_SIZE, FOOTER_SIZE,
        MAGIC_VALUE,
    };

    #[test]
    fn field_is_zero_padded() {
        let field = make_field(b"osx_arm64");

        assert_eq!(&field[..9], b"osx_arm64");
        assert!(field[9..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn footer_has_expected_size_and_reversed_metadata_fields() {
        let footer = build_footer();

        assert_eq!(footer.len(), FOOTER_SIZE);
        assert_eq!(field_at(&footer, 3), ABI_TYPE);
        assert_eq!(field_at(&footer, 5), DUCKDB_CAPI_VERSION);
        assert_eq!(field_at(&footer, 7), MAGIC_VALUE);
        assert!(footer[FIELD_SIZE * 8..].iter().all(|byte| *byte == 0));
    }

    fn field_at(footer: &[u8], index: usize) -> &[u8] {
        let start = index * FIELD_SIZE;
        footer[start..start + FIELD_SIZE]
            .split(|byte| *byte == 0)
            .next()
            .unwrap()
    }
}
