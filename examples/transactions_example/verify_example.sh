#!/usr/bin/env bash
# verify_example.sh — Verify the simplified transactions example
#
# What it tests:
#   Builds and runs the simple_transactions_example which demonstrates the
#   KIP-447 single-producer transactional consume-transform-produce pattern.
#   It seeds 20 messages to an input topic, transactionally transforms them
#   to an output topic using a single producer, commits consumer offsets
#   atomically, and verifies all 20 messages appear on the output topic
#   with isolation.level=read_committed.
#
# Expected output:
#   The script prints each step as it progresses and ends with:
#     Result: 20/20 messages verified on output topic.
#     SUCCESS
#
# Prerequisites:
#   - Go installed
#   - A Kafka broker running on localhost:9092

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXAMPLE_DIR="$SCRIPT_DIR/../simple_transactions_example"
BROKER="${1:-localhost:9092}"

echo "=== Step 1: Verify broker is reachable ==="
if ! echo "quit" | nc -w 2 localhost 9092 >/dev/null 2>&1; then
  echo "FAIL: Kafka broker not reachable at $BROKER"
  exit 1
fi
echo "Broker is reachable at $BROKER"

echo ""
echo "=== Step 2: Build the transactions example ==="
cd "$SCRIPT_DIR"
if ! go build ./...; then
  echo "FAIL: transactions_example failed to build"
  exit 1
fi
echo "transactions_example builds successfully"

echo ""
echo "=== Step 3: Build the simple_transactions_example ==="
cd "$EXAMPLE_DIR"
if ! go build ./...; then
  echo "FAIL: simple_transactions_example failed to build"
  exit 1
fi
echo "simple_transactions_example builds successfully"

echo ""
echo "=== Step 4: Run the simple_transactions_example ==="
OUTPUT=$(go run . "$BROKER" 2>&1)
EXIT_CODE=$?

echo "$OUTPUT"

if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "FAIL: simple_transactions_example exited with code $EXIT_CODE"
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
