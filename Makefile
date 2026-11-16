# Builds the extension through DuckDB's own C-API extension makefiles, which is
# what duckdb/community-extensions runs: `make release` must leave the packaged
# extension at build/release/extension/$(EXTENSION_NAME)/.
#
# Those makefiles also append the metadata footer (platform, version, ABI), so
# nothing here does that by hand.
.PHONY: help clean clean_all release_v2 e2e e2e_v2
.DEFAULT_GOAL := help

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
# so one build loads across DuckDB 1.4 through 2.0. The official Rust template sets that
# flag only because duckdb-rs needs unstable C API functions.
TARGET_DUCKDB_VERSION=v1.2.0

all: configure release

help:
	@echo "Targets:"
	@echo "  configure   one-off: python venv + platform/version detection"
	@echo "  all         configure + release"
	@echo "  release     build build/release/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension"
	@echo "  debug       debug build under build/debug/"
	@echo "  release_v2  C API v2 build under build/v2/ (loads in DuckDB $(V2_DUCKDB_VERSION) only)"
	@echo "  e2e         end-to-end tests against a fake AWS (E2E_DUCKDB_VERSION=$(E2E_DUCKDB_VERSION))"
	@echo "  e2e_v2      end-to-end tests for the v2 build"
	@echo "  test        upstream SQL tests against the release build"
	@echo "  clean       cargo clean + build/"
	@echo "  clean_all   clean + configure/"

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

# The upstream makefiles copy the packaged extension over the previous one in
# place, which keeps its inode. macOS caches a loaded file's code signature per
# inode, so once DuckDB has loaded a build, the next build at the same path gets
# every process that loads it killed (SIGKILL, exit 137, nothing on stderr).
# Unlinking first gives each build a fresh inode.
unlink_packaged_%:
	rm -f build/$*/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension

debug: unlink_packaged_debug build_extension_library_debug build_extension_with_metadata_debug
release: unlink_packaged_release build_extension_library_release build_extension_with_metadata_release

# The same extension built on DuckDB 2.0's C API v2, which adds filter
# pushdown and a moving progress bar. That API is unstable, so this binary is
# stamped C_STRUCT_UNSTABLE and loads into exactly V2_DUCKDB_VERSION -- the
# release libduckdb-sys is pinned to -- and nothing else. release.yml publishes
# it beside the stable build, named for that version; community-extensions only
# ever builds `make release`. release.yml reads this line with sed, so keep
# it a plain `V2_DUCKDB_VERSION=vX.Y.Z`.
V2_DUCKDB_VERSION=v2.0.0
V2_PACKAGED=build/v2/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension

# Built the way rust.Makefile builds `release`: same flags, and the same
# --target when DUCKDB_PLATFORM names one (osx_arm64 does, which moves the
# output under target/<triple>/). Without it the library lands in
# target/release/ and the copy below looks in the wrong place.
release_v2: check_configure
	cargo build $(CARGO_OVERRIDE_DUCKDB_RS_FLAG) --release $(TARGET_INFO) --features capi-v2
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(dir $(V2_PACKAGED))').mkdir(parents=True, exist_ok=True)"
	rm -f $(V2_PACKAGED)
	$(PYTHON_VENV_BIN) extension-ci-tools/scripts/append_extension_metadata.py \
			-l $(TARGET_PATH)/release$(IS_EXAMPLE)/$(RUST_LIBNAME) \
			-o $(V2_PACKAGED) \
			-n $(EXTENSION_NAME) \
			-dv $(V2_DUCKDB_VERSION) \
			-evf configure/extension_version.txt \
			-pf configure/platform.txt --abi-type C_STRUCT_UNSTABLE

# End-to-end tests against a fake AWS (tests/e2e): no credentials needed.
# E2E_DUCKDB_VERSION picks the DuckDB the v1 build is loaded into, and
# E2E_EXTENSION which build of it (the release one unless told otherwise).
E2E_DUCKDB_VERSION ?= 2.0.0
E2E_PYTEST=uv run --no-project --with pytest --with duckdb==

E2E_EXTENSION ?= build/release/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension

e2e:
	$(E2E_PYTEST)$(E2E_DUCKDB_VERSION) pytest tests/e2e --extension $(E2E_EXTENSION)

e2e_v2:
	$(E2E_PYTEST)$(V2_DUCKDB_VERSION:v%=%) pytest tests/e2e --extension $(V2_PACKAGED)

test: test_release
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
