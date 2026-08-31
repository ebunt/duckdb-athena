# Builds the extension through DuckDB's own C-API extension makefiles, which is
# what duckdb/community-extensions runs: `make release` must leave the packaged
# extension at build/release/extension/$(EXTENSION_NAME)/.
#
# Those makefiles also append the metadata footer (platform, version, ABI), so
# nothing here does that by hand.
.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Also the name of the cdylib and of the init symbol: DuckDB derives
# athena_init_c_api from it, so src/lib.rs must match.
EXTENSION_NAME=athena
# ?= so the environment wins: a plain make stamps the crate version, while
# release.yml passes the tag name (or the commit sha off a tag). A plain `=`
# here would override the environment, which is the opposite of what Make
# does for command-line variables and quietly loses build provenance.
EXTENSION_VERSION ?= $(shell awk -F'"' '/^version = /{print "v"$$2; exit}' Cargo.toml)

# The C API version this is built against, not the DuckDB version it runs on.
# Deliberately no USE_UNSTABLE_C_API: quack-rs targets the stable C_STRUCT ABI,
# so one build loads across DuckDB 1.x. The official Rust template sets that
# flag only because duckdb-rs needs unstable C API functions.
TARGET_DUCKDB_VERSION=v1.2.0

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_release
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
