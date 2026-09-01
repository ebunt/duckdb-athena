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
# the demo database project/bootstrap.py creates. Build it first with
#     uv run project/bootstrap.py create

set -uo pipefail

EXTENSION="${1:-build/release/extension/athena/athena.duckdb_extension}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
WORKGROUP="${ATHENA_WORKGROUP:-primary}"
# The database project/bootstrap.py creates. Override to point the checks at a
# differently-named copy.
DB="${ATHENA_DEMO_DATABASE:-duckdb_athena_demo}"

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
    local name="$1" expected="$2" actual="$3" summary="${4:-}"
    if [ "$expected" = "$actual" ]; then
        # A fourth argument replaces the value in the success line: some checks
        # compare kilobytes of concatenated rows, and printing that on success
        # buries every other result.
        printf 'ok   %-42s %s\n' "$name" "${summary:-$actual}"
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
# SUM(total_decimal) is the one that earns its place twice over: DECIMAL is the
# only type with a two-part registration (width and scale) and a hand-written
# value path, and a wrong scale is silent -- the number just comes back off by a
# factor of ten. Deliberately no CAST in the DuckDB side: a cast would hide a
# decimal that arrived as VARCHAR, which is the failure being checked for.
for expr in "COUNT(*)" "COUNT(trip_distance)" "SUM(trip_distance)" "CAST(MIN(pickup_datetime) AS DATE)" "CAST(MAX(pickup_datetime) AS DATE)" \
    "COUNT(DISTINCT payment_type)" "SUM(total_decimal)" "COUNT(*) FILTER (WHERE is_disputed)"; do
    # Athena has no FILTER clause; count_if is its equivalent.
    athena_expr="${expr/COUNT(*) FILTER (WHERE is_disputed)/COUNT_IF(is_disputed)}"
    check "trips $expr" \
        "$(athena "SELECT CAST($athena_expr AS VARCHAR) FROM $DB.trips")" \
        "$(duck "SELECT CAST($expr AS VARCHAR) FROM athena_scan('trips', database='$DB');")"
done

echo
echo "-- a full scan finishes, and reads the result from S3 rather than paging"
start=$(date +%s)
rows=$(duck "SELECT COUNT(*) FROM athena_scan('trips', database='$DB');")
elapsed=$(($(date +%s) - start))
check "trips COUNT(*)" "$(athena "SELECT CAST(COUNT(*) AS VARCHAR) FROM $DB.trips")" "$rows"
# Paging 1.07M rows took ~135s; reading the result object takes seconds. A large
# regression here means the S3 path silently stopped being used.
if [ "$elapsed" -lt 60 ]; then
    printf 'ok   %-42s %ss\n' "trips COUNT(*) under 60s" "$elapsed"
    pass=$((pass + 1))
else
    printf 'FAIL %-42s %ss -- likely fell back to GetQueryResults paging\n' \
        "trips COUNT(*) under 60s" "$elapsed"
    fail=$((fail + 1))
fi

echo
echo "-- pushdown and bounds reach Athena, not just DuckDB"
check "maxrows caps the Athena query" "137" \
    "$(duck "SELECT COUNT(*) FROM athena_scan('trips', database='$DB', maxrows=137);")"
check "predicate filters in Athena" \
    "$(athena "SELECT CAST(COUNT(*) AS VARCHAR) FROM $DB.trips WHERE passenger_count = 6")" \
    "$(duck "SELECT COUNT(*) FROM athena_scan('trips', database='$DB', predicate='passenger_count = 6');")"

# Projection pushdown is only observable in the SQL Athena received: the row
# count is identical either way. Checking the value alone is what let a broken
# COUNT(*) look verified in v0.2.2.
duck "SELECT COUNT(trip_distance) FROM athena_scan('trips', database='$DB');" > /dev/null
# --max-items makes the CLI append its pagination token as a second line, so
# keep only the id.
sent=$(aws athena list-query-executions --work-group "$WORKGROUP" --max-items 1 \
    --query 'QueryExecutionIds[0]' --output text | head -1)
sql=$(aws athena get-query-execution --query-execution-id "$sent" \
    --query 'QueryExecution.Query' --output text)
if [ "$sql" = "SELECT \"trip_distance\" FROM \"$DB\".\"trips\"" ]; then
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
PART_TABLE="${ATHENA_TEST_PARTITIONED_TABLE:-$DB.trips_by_month}"
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

    # Every column of a bounded sample of rows, concatenated in schema order and
    # compared with Athena's own answer: this is what catches values landing in
    # the wrong column, which a schema-only check sails past. Bounded because
    # the comparison string is the whole sample -- over a full table it is
    # megabytes of shell variable and unreadable when it fails.
    row_expr=""
    for c in $(echo "$expected" | tr ',' ' '); do
        [ -n "$row_expr" ] && row_expr="$row_expr || '|' || "
        row_expr="${row_expr}COALESCE(CAST(\"$c\" AS VARCHAR), '')"
    done
    sample="ORDER BY $row_expr LIMIT 20"
    # Athena renders a TIMESTAMP with a millisecond field, DuckDB without, so
    # the same instant prints two ways. Strip a trailing .000 only where it ends
    # a field, which no other column here can produce -- the DECIMAL is 2dp.
    same_instant() { sed -E 's/\.000(\||$)/\1/g'; }
    check "partition table rows match Athena" \
        "$(athena "SELECT array_join(array_agg(r ORDER BY r), ';') FROM (SELECT $row_expr AS r FROM $part_db.$part_tbl $sample)" | same_instant)" \
        "$(duck "SELECT string_agg(r, ';' ORDER BY r) FROM (SELECT $row_expr AS r FROM athena_scan('$part_tbl', database='$part_db') $sample);" | same_instant)" \
        "20 sampled rows identical, every column"

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
echo "-- region= and result reuse"
# region= must beat the ambient region, so run with a deliberately wrong
# AWS_REGION and let the parameter correct it.
check "region= overrides AWS_REGION" \
    "$(athena "SELECT CAST(COUNT(*) AS VARCHAR) FROM $DB.trips")" \
    "$(AWS_REGION=eu-west-1 duckdb -unsigned -noheader -list -c "LOAD '$EXTENSION';
        SELECT COUNT(*) FROM athena_scan('trips', database='$DB', region='$AWS_REGION');" 2>/dev/null)"

# Reuse is only observable in the execution record: the answer is identical
# either way, so assert on ReusedPreviousResult and bytes scanned instead.
reuse_run() {
    duck "SELECT COUNT(trip_distance) FROM athena_scan('trips', database='$DB', result_reuse_minutes=60);" > /dev/null
    aws athena get-query-execution --query-execution-id \
        "$(aws athena list-query-executions --work-group "$WORKGROUP" --max-items 1 \
            --query 'QueryExecutionIds[0]' --output text | head -1)" \
        --query 'QueryExecution.Statistics.[ResultReuseInformation.ReusedPreviousResult,DataScannedInBytes]' \
        --output text
}
reuse_run > /dev/null            # prime the cache; this run may or may not reuse
second=$(reuse_run)
check "second identical query is reused" "True	0" "$second"

echo
echo "-- an unknown predicate column fails, with Athena's own reason"
# The extension no longer pre-parses the predicate to check column names: Athena
# rejects an unknown column itself in ~0.5s for 0 bytes scanned, and its message
# names the column. What matters is that the reason reaches the user rather than
# an opaque "Query Failed", so this asserts on the text, not just the failure.
err=$(duckdb -unsigned -c "LOAD '$EXTENSION';
    SELECT 1 FROM athena_scan('trips', database='$DB', predicate='nosuchcol = 1');" 2>&1 || true)
case "$err" in
    *COLUMN_NOT_FOUND*nosuchcol*)
        printf 'ok   %-42s %s\n' "unknown predicate column" "Athena reason surfaced"
        pass=$((pass + 1)) ;;
    *)
        printf 'FAIL %-42s %s\n' "unknown predicate column" "$(printf '%s' "$err" | tail -1)"
        fail=$((fail + 1)) ;;
esac

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
