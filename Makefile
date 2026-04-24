# Determine the OS and set the library extension and prefix accordingly
OS := $(shell uname -s)

ifeq ($(OS),Darwin)
	EXT = dylib
	PREFIX = lib
else ifeq ($(OS),Linux)
	EXT = so
	PREFIX = lib
else
	# Assume Windows
	EXT = dll
	PREFIX = 
endif

LIB_NAME = duckdb_athena
TARGET_DIR = target/release
BUILT_LIB = $(TARGET_DIR)/$(PREFIX)$(LIB_NAME).$(EXT)
EXTENSION = $(TARGET_DIR)/$(LIB_NAME).duckdb_extension

.PHONY: all build clean

all: build

build:
	cargo build --release
	cp $(BUILT_LIB) $(EXTENSION)
	python3 scripts/append_extension_footer.py $(EXTENSION)
	@echo "Extension ready: $(EXTENSION)"

clean:
	cargo clean
