#!/usr/bin/env python3
"""
Appends the DuckDB extension metadata footer to a .duckdb_extension file.

DuckDB requires a 512-byte footer at the end of every loadable extension:
  - 256 bytes of metadata: 8 fields × 32 bytes, stored in REVERSE order
  - 256 bytes of signature: zeros (skipped for unsigned/local extensions)

Field layout as stored in the file (reverse of how DuckDB reads them):
  [  0- 31] unused (zeros)
  [ 32- 63] unused (zeros)
  [ 64- 95] unused (zeros)
  [ 96-127] ABI type:        "C_STRUCT"
  [128-159] extension version (e.g. "v0.1.0")
  [160-191] DuckDB C API version: "v1.2.0"
  [192-223] platform (e.g. "osx_arm64")
  [224-255] magic value:     "4" followed by 31 zero bytes

Usage:
    python3 scripts/append_extension_footer.py <extension_file>
"""

import sys
import platform
import os


FIELD_SIZE = 32
NUM_FIELDS = 8
SIGNATURE_SIZE = 256
FOOTER_SIZE = FIELD_SIZE * NUM_FIELDS + SIGNATURE_SIZE  # 512

MAGIC_VALUE = b"4"
ABI_TYPE = b"C_STRUCT"
DUCKDB_CAPI_VERSION = b"v1.2.0"
EXTENSION_VERSION = os.environ.get("DUCKDB_EXTENSION_VERSION", "v0.1.0").encode()


def get_duckdb_platform() -> bytes:
    if configured_platform := os.environ.get("DUCKDB_PLATFORM"):
        return configured_platform.encode()

    system = platform.system()
    machine = platform.machine()

    if system == "Darwin":
        os_name = "osx"
    elif system == "Windows":
        os_name = "windows"
    else:
        os_name = "linux"

    if machine in ("arm64", "aarch64"):
        arch = "arm64"
    elif machine in ("x86_64", "AMD64"):
        arch = "amd64"
    else:
        arch = "amd64"

    return f"{os_name}_{arch}".encode()


def make_field(value: bytes) -> bytes:
    """Pad or truncate value to exactly FIELD_SIZE bytes."""
    assert len(value) <= FIELD_SIZE, f"Field value too long: {value!r}"
    return value + b"\x00" * (FIELD_SIZE - len(value))


def build_footer() -> bytes:
    platform_bytes = get_duckdb_platform()

    # Fields in READ order (index 0 = magic, index 7 = unused).
    # DuckDB reverses them before reading, so we must store them reversed in the file.
    fields_read_order = [
        make_field(MAGIC_VALUE),           # [0] magic
        make_field(platform_bytes),        # [1] platform
        make_field(DUCKDB_CAPI_VERSION),   # [2] C API version
        make_field(EXTENSION_VERSION),     # [3] extension version
        make_field(ABI_TYPE),              # [4] ABI type
        make_field(b""),                   # [5] unused
        make_field(b""),                   # [6] unused
        make_field(b""),                   # [7] unused
    ]

    # Store in REVERSE order as DuckDB reverses them when parsing
    fields_file_order = list(reversed(fields_read_order))
    metadata = b"".join(fields_file_order)
    assert len(metadata) == FIELD_SIZE * NUM_FIELDS

    signature = b"\x00" * SIGNATURE_SIZE
    return metadata + signature


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <extension_file>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    footer = build_footer()
    assert len(footer) == FOOTER_SIZE

    with open(path, "ab") as f:
        f.write(footer)

    print(f"Appended {FOOTER_SIZE}-byte DuckDB extension footer to {path}")
    print(f"  platform:    {get_duckdb_platform().decode()}")
    print(f"  ABI type:    {ABI_TYPE.decode()}")
    print(f"  C API ver:   {DUCKDB_CAPI_VERSION.decode()}")


if __name__ == "__main__":
    main()
