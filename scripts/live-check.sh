#!/usr/bin/env bash
#
# Checks the extension against live Athena. CI cannot do this (no credentials),
# and the unit tests cannot either: every bug this catches lived in the gap
# between "the code compiles" and "the rows are right".
#
# Each check compares the extension's answer against Athena computing the same
# thing natively, so a wrong answer fails rather than merely looking plausible.
# The count regression that shipped in v0.2.2 was invisible to value-only
# checking -- the value was correct, the query behind it was not -- so the
# checks that can assert on the SQL Athena received do.
#
# Usage:
#   scripts/live-check.sh [path/to/athena.duckdb_extension]
#
# Requires: AWS credentials, duckdb and aws CLIs, and read access to the
# sampledb and nyctaxi databases in us-east-1.

set -uo pipefail

EXTENSION="${1:-build/release/extension/athena/athena.duckdb_extension}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
WORKGROUP="${ATHENA_WORKGROUP:-primary}"

if [ ! -f "$EXTENSION" ]; then
    echo "no extension at $EXTENSION -- run make first" >&2
    exit 1
fi
EXTENSION="$(cd "$(dirname "$EXTENSION")" && pwd)/$(basename "$EXTENSION")"

pass=0
fail=0

# Reports one comparison. Keeping expected/actual in the output means a failure
# says what diverged, not just that something did.
check() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        printf 'ok   %-42s %s\n' "$name" "$actual"
        pass=$((pass + 1))
    else
        printf 'FAIL %-42s expected %s, got %s\n' "$name" "$expected" "$actual"
        fail=$((fail + 1))
    fi
}

# Runs SQL through DuckDB with the extension loaded, one bare value out.
duck() {
    duckdb -unsigned -noheader -list -c "LOAD '$EXTENSION'; $1" 2>/dev/null
}

# Runs SQL directly in Athena and returns the first data cell, so the
# comparison is against Athena's own arithmetic rather than a hardcoded number.
athena() {
    local qid
    qid=$(aws athena start-query-execution --query-string "$1" \
        --work-group "$WORKGROUP" --query QueryExecutionId --output text) || return 1
    local state
    while :; do
        state=$(aws athena get-query-execution --query-execution-id "$qid" \
            --query 'QueryExecution.Status.State' --output text)
        case "$state" in
            SUCCEEDED) break ;;
            FAILED | CANCELLED)
                echo "athena query $state: $qid" >&2
                return 1
                ;;
        esac
        sleep 2
    done
    aws athena get-query-results --query-execution-id "$qid" \
        --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
}

echo "extension: $EXTENSION"
echo "region:    $AWS_REGION"
echo

echo "-- value parity: extension vs Athena computing the same aggregate"
for expr in "COUNT(*)" "COUNT(url)" "SUM(sentbytes)" "MIN(timestamp)" "MAX(timestamp)" \
    "COUNT(DISTINCT elbresponsecode)"; do
    check "elb_logs $expr" \
        "$(athena "SELECT CAST($expr AS VARCHAR) FROM sampledb.elb_logs")" \
        "$(duck "SELECT CAST($expr AS VARCHAR) FROM athena_scan('elb_logs', database='sampledb');")"
done

echo
echo "-- a full scan finishes, and reads the result from S3 rather than paging"
start=$(date +%s)
rows=$(duck "SELECT COUNT(*) FROM athena_scan('data', database='nyctaxi');")
elapsed=$(($(date +%s) - start))
check "nyctaxi COUNT(*)" "$(athena "SELECT CAST(COUNT(*) AS VARCHAR) FROM nyctaxi.data")" "$rows"
# Paging 1.07M rows took ~135s; reading the result object takes seconds. A large
# regression here means the S3 path silently stopped being used.
if [ "$elapsed" -lt 60 ]; then
    printf 'ok   %-42s %ss\n' "nyctaxi COUNT(*) under 60s" "$elapsed"
    pass=$((pass + 1))
else
    printf 'FAIL %-42s %ss -- likely fell back to GetQueryResults paging\n' \
        "nyctaxi COUNT(*) under 60s" "$elapsed"
    fail=$((fail + 1))
fi

echo
echo "-- pushdown and bounds reach Athena, not just DuckDB"
check "maxrows caps the Athena query" "137" \
    "$(duck "SELECT COUNT(*) FROM athena_scan('elb_logs', database='sampledb', maxrows=137);")"
check "predicate filters in Athena" \
    "$(athena "SELECT CAST(COUNT(*) AS VARCHAR) FROM sampledb.elb_logs WHERE elbresponsecode = '404'")" \
    "$(duck "SELECT COUNT(*) FROM athena_scan('elb_logs', database='sampledb', predicate='elbresponsecode = ''404''');")"

# Projection pushdown is only observable in the SQL Athena received: the row
# count is identical either way. Checking the value alone is what let a broken
# COUNT(*) look verified in v0.2.2.
duck "SELECT COUNT(url) FROM athena_scan('elb_logs', database='sampledb');" > /dev/null
# --max-items makes the CLI append its pagination token as a second line, so
# keep only the id.
sent=$(aws athena list-query-executions --work-group "$WORKGROUP" --max-items 1 \
    --query 'QueryExecutionIds[0]' --output text | head -1)
sql=$(aws athena get-query-execution --query-execution-id "$sent" \
    --query 'QueryExecution.Query' --output text)
if [ "$sql" = 'SELECT "url" FROM "sampledb"."elb_logs"' ]; then
    printf 'ok   %-42s %s\n' "projection pushdown in sent SQL" "$sql"
    pass=$((pass + 1))
else
    printf 'FAIL %-42s got: %s\n' "projection pushdown in sent SQL" "$sql"
    fail=$((fail + 1))
fi

echo
echo "-- partitioned tables: column order, metadata-only reads, pruning"
# Glue keeps partition keys separate from data columns, and Athena returns them
# last in SELECT *; bind has to register them in that order or every value lands
# in the wrong column. Needs a partitioned fixture, so it is skipped when absent.
PART_TABLE="${ATHENA_TEST_PARTITIONED_TABLE:-default.claude_part_test}"
part_db="${PART_TABLE%%.*}"
part_tbl="${PART_TABLE##*.}"
if aws glue get-table --database-name "$part_db" --name "$part_tbl" > /dev/null 2>&1; then
    glue_names() {
        aws glue get-table --database-name "$part_db" --name "$part_tbl" \
            --query "join(\`,\`, $1[].Name)" --output text
    }
    data_cols=$(glue_names 'Table.StorageDescriptor.Columns')
    part_cols=$(glue_names 'Table.PartitionKeys')
    expected="$data_cols,$part_cols"

    # DESCRIBE only binds the table function, so on its own it proves the
    # declared schema and nothing about the rows. Check it, then check the rows.
    actual=$(duck "SELECT string_agg(column_name, ',') FROM (
            DESCRIBE SELECT * FROM athena_scan('$part_tbl', database='$part_db')
        );")
    check "partition columns come last" "$expected" "$actual"

    # Every column of every row, concatenated in schema order and compared with
    # Athena's own answer: this is what catches values landing in the wrong
    # column, which is the failure a schema-only check sails past.
    row_expr=""
    for c in $(echo "$expected" | tr ',' ' '); do
        [ -n "$row_expr" ] && row_expr="$row_expr || '|' || "
        row_expr="${row_expr}COALESCE(CAST(\"$c\" AS VARCHAR), '')"
    done
    check "partition table rows match Athena" \
        "$(athena "SELECT array_join(array_agg($row_expr ORDER BY $row_expr), ';') FROM $part_db.$part_tbl")" \
        "$(duck "SELECT string_agg($row_expr, ';' ORDER BY $row_expr) FROM athena_scan('$part_tbl', database='$part_db');")"

    # Selecting only a partition column should not read the data files: the
    # values live in the catalog. Compared against a full scan rather than
    # asserted as zero, since a non-columnar fixture may still read something.
    part_one=$(echo "$part_cols" | cut -d, -f1)
    if ! duck "SELECT COUNT(DISTINCT \"$part_one\") FROM athena_scan('$part_tbl', database='$part_db');" > /dev/null; then
        printf 'FAIL %-42s scan of the partition column failed\n' "partition-only scan reads less"
        fail=$((fail + 1))
    else
        last_bytes() {
            local id
            id=$(aws athena list-query-executions --work-group "$WORKGROUP" --max-items 1 \
                --query 'QueryExecutionIds[0]' --output text | head -1)
            aws athena get-query-execution --query-execution-id "$id" \
                --query 'QueryExecution.Statistics.DataScannedInBytes' --output text
        }
        part_bytes=$(last_bytes)
        duck "SELECT COUNT(*) FROM athena_scan('$part_tbl', database='$part_db');" > /dev/null
        full_bytes=$(last_bytes)
        if [ "$part_bytes" -lt "$full_bytes" ] || [ "$part_bytes" -eq 0 ]; then
            printf 'ok   %-42s %s vs %s bytes for a full scan\n' \
                "partition-only scan reads less" "$part_bytes" "$full_bytes"
            pass=$((pass + 1))
        else
            printf 'FAIL %-42s %s bytes, full scan %s\n' \
                "partition-only scan reads less" "$part_bytes" "$full_bytes"
            fail=$((fail + 1))
        fi
    fi
else
    printf 'skip %-42s no partitioned fixture (%s)\n' "partitioned table checks" "$PART_TABLE"
fi

echo
echo "-- predicate validation rejects unknown columns at bind"
if duckdb -unsigned -c "LOAD '$EXTENSION';
    SELECT 1 FROM athena_scan('elb_logs', database='sampledb', predicate='nosuchcol = 1');" \
    > /dev/null 2>&1; then
    printf 'FAIL %-42s bind accepted an unknown column\n' "unknown predicate column rejected"
    fail=$((fail + 1))
else
    printf 'ok   %-42s rejected at bind\n' "unknown predicate column rejected"
    pass=$((pass + 1))
fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
