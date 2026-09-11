#!/usr/bin/env bash
# verify_example.sh — Verify the wordcount transactions example
#
# What it tests:
#   Builds and runs the wordcount_transactions_example which demonstrates
#   Kafka transactions by reading sentences, splitting them into words,
#   counting each word, and producing the counts atomically to an output
#   topic. It verifies that all 10 unique words are counted correctly
#   using isolation.level=read_committed.
#
# Expected output:
#   The script prints each step as it progresses and ends with:
#     SUCCESS
#     === PASSED ===
#
# Prerequisites:
#   - Go installed
#   - A Kafka broker running on localhost:9092

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BROKER="${1:-localhost:9092}"

echo "=== Step 1: Verify broker is reachable ==="
if ! echo "quit" | nc -w 2 localhost 9092 >/dev/null 2>&1; then
  echo "FAIL: Kafka broker not reachable at $BROKER"
  exit 1
fi
echo "Broker is reachable at $BROKER"

echo ""
echo "=== Step 2: Build the wordcount_transactions_example ==="
cd "$SCRIPT_DIR"
if ! go build -o /tmp/wordcount_transactions_example .; then
  echo "FAIL: wordcount_transactions_example failed to build"
  exit 1
fi
echo "wordcount_transactions_example builds successfully"

echo ""
echo "=== Step 3: Run the wordcount_transactions_example ==="
OUTPUT=$(go run . "$BROKER" 2>&1)
EXIT_CODE=$?

echo "$OUTPUT"

if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "FAIL: wordcount_transactions_example exited with code $EXIT_CODE"
  exit 1
fi

if echo "$OUTPUT" | grep -q "SUCCESS"; then
  echo ""
  echo "=== PASSED ==="
else
  echo ""
  echo "FAIL: Output did not contain SUCCESS"
  exit 1
fi
