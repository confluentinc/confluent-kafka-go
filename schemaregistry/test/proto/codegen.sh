#!/usr/bin/env bash
# Regenerates the test protos that reference the confluent value types. Run from the repo root.
#
# The paths and package names come from --go_opt=M rather than the `option go_package="../test"`
# these files carry, because a relative go_package makes protoc refuse the output path
# ("Output file names must never have a relative path"). The confluent/type/... mappings must
# match schemaregistry/confluent/codegen.sh, or these files silently resolve Decimal through the
# deprecated confluent/types alias instead of the canonical package.
set -euo pipefail

MOD=github.com/confluentinc/confluent-kafka-go/v2
SR=schemaregistry

protoc -I"$SR/test/proto" -I"$SR" --go_out=. --go_opt=module="$MOD" \
  --go_opt="Mconfluent/type/decimal.proto=$MOD/$SR/confluent/type;typepb" \
  --go_opt="Mconfluent/type/variant.proto=$MOD/$SR/confluent/type;typepb" \
  --go_opt="Mconfluent/meta.proto=$MOD/$SR/confluent;confluent" \
  --go_opt="Mnested_decimal.proto=$MOD/$SR/test;test" \
  --go_opt="Mvalue_types.proto=$MOD/$SR/test;test" \
  --go_opt="Mvalue_type_rules.proto=$MOD/$SR/test;test" \
  nested_decimal.proto value_types.proto value_type_rules.proto
