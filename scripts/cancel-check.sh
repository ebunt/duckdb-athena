#!/usr/bin/env bash
#
# Answers one question against live Athena: when a scan is interrupted while it
# waits on Athena, does the Athena query stop, or keep running (and billing)?
#
# It starts a scan, sends SIGINT to DuckDB as soon as the execution id appears
# -- what Ctrl-C does -- and then reads the query's final state from Athena:
#   CANCELLED  the extension stopped it
#   SUCCEEDED  it ran to completion with nobody reading the result
#
# Usage:
#   scripts/cancel-check.sh [path/to/athena.duckdb_extension]
#
# Requires what live-check.sh requires. A fast query can finish before the
# interrupt lands; the script says so rather than guessing, and
# ATHENA_CANCEL_TABLE can point it at a larger table.

set -uo pipefail

EXTENSION="${1:-build/release/extension/athena/athena.duckdb_extension}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
DB="${ATHENA_DEMO_DATABASE:-duckdb_athena_demo}"
TABLE="${ATHENA_CANCEL_TABLE:-trips}"

if [ ! -f "$EXTENSION" ]; then
    echo "no extension at $EXTENSION -- run make first" >&2
    exit 1
fi
EXTENSION="$(cd "$(dirname "$EXTENSION")" && pwd)/$(basename "$EXTENSION")"

state_of() {
    aws athena get-query-execution --query-execution-id "$1" \
        --query 'QueryExecution.Status.State' --output text
}

log=$(mktemp)
trap 'rm -f "$log"' EXIT

duckdb -unsigned -noheader -list -c "LOAD '$EXTENSION';
    SELECT COUNT(*) FROM athena_scan('$TABLE', database='$DB');" >/dev/null 2>"$log" &
pid=$!

# The extension prints the execution id the moment Athena accepts the query.
qid=""
for _ in $(seq 1 300); do
    qid=$(sed -n 's/^Running Athena query, execution id: //p' "$log" | head -1)
    [ -n "$qid" ] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
done
if [ -z "$qid" ]; then
    echo "the scan never reached Athena:" >&2
    cat "$log" >&2
    exit 1
fi

kill -INT "$pid"
at_interrupt=$(state_of "$qid")
started=$(date +%s)
wait "$pid"
duckdb_status=$?
waited=$(( $(date +%s) - started ))

final=$(state_of "$qid")
while [ "$final" = "QUEUED" ] || [ "$final" = "RUNNING" ]; do
    sleep 1
    final=$(state_of "$qid")
done

echo "execution id:            $qid"
echo "state at interrupt:      $at_interrupt"
echo "duckdb exit status:      $duckdb_status (after ${waited}s)"
echo "final Athena state:      $final"

case "$at_interrupt:$final" in
    SUCCEEDED:*)
        echo "inconclusive: the query finished before the interrupt landed"
        exit 2 ;;
    *:CANCELLED)
        echo "ok: the interrupt stopped the Athena query" ;;
    *)
        echo "LEAK: the Athena query outlived the interrupt"
        exit 1 ;;
esac
