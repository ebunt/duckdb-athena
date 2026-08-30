# Determine the OS and set the library extension and prefix accordingly
OS := $(shell uname -s)
ARCH := $(shell uname -m)

ifeq ($(OS),Darwin)
	EXT = dylib
	PREFIX = lib
	PLATFORM_OS = osx
else ifeq ($(OS),Linux)
	EXT = so
	PREFIX = lib
	PLATFORM_OS = linux
else
	# Assume Windows
	EXT = dll
	PREFIX =
	PLATFORM_OS = windows
	EXE = .exe
endif

ifneq ($(filter arm64 aarch64,$(ARCH)),)
	PLATFORM_ARCH = arm64
else
	PLATFORM_ARCH = amd64
endif

LIB_NAME = duckdb_athena
TARGET_DIR = target/release
BUILT_LIB = $(TARGET_DIR)/$(PREFIX)$(LIB_NAME).$(EXT)
EXTENSION = $(TARGET_DIR)/$(LIB_NAME).duckdb_extension
QUACK_RS_VERSION = 0.11.0
APPEND_METADATA = target/tools/quack-rs-$(QUACK_RS_VERSION)/bin/append_metadata$(EXE)
DUCKDB_ABI_TYPE ?= C_STRUCT
DUCKDB_CAPI_VERSION ?= v1.2.0
# Footer version reported by duckdb_extensions(). Derived from the crate version
# (which tracks the release tag) so a local build never claims a stale one;
# release.yml overrides it from the environment with the tag name.
CRATE_VERSION = $(shell awk -F'"' '/^version = /{print $$2; exit}' Cargo.toml)
DUCKDB_EXTENSION_VERSION ?= v$(CRATE_VERSION)
DUCKDB_PLATFORM ?= $(PLATFORM_OS)_$(PLATFORM_ARCH)

.PHONY: all build clean metadata-tool package

all: build

build:
	cargo build --release
	$(MAKE) package
	@echo "Extension ready: $(EXTENSION)"

metadata-tool: $(APPEND_METADATA)

$(APPEND_METADATA):
	cargo install --locked --root target/tools/quack-rs-$(QUACK_RS_VERSION) \
		--version $(QUACK_RS_VERSION) quack-rs --bin append_metadata

package: $(APPEND_METADATA)
	@test -n "$(DUCKDB_EXTENSION_VERSION)" -a "$(DUCKDB_EXTENSION_VERSION)" != "v" \
		|| { echo "could not read version from Cargo.toml; set DUCKDB_EXTENSION_VERSION"; exit 1; }
	$(APPEND_METADATA) "$(BUILT_LIB)" "$(EXTENSION)" \
		--abi-type "$(DUCKDB_ABI_TYPE)" \
		--extension-version "$(DUCKDB_EXTENSION_VERSION)" \
		--duckdb-version "$(DUCKDB_CAPI_VERSION)" \
		--platform "$(DUCKDB_PLATFORM)"

clean:
	cargo clean
