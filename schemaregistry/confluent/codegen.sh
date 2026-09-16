#!/usr/bin/env bash
# Regenerates the Go bindings for the vendored confluent value types. Run from the repo root.
#
# The canonical files live at confluent/type/... - the path the Java client registers and what
# ProtobufSchema declares - and generate into `package typepb`, because `type` is a Go keyword and
# cannot name a package. confluent/types/decimal.proto is a public-import stub for the path these
# types used to occupy; it generates `type Decimal = typepb.Decimal` plus the descriptor variable
# under its old name, so code and descriptors built against the old path keep working.
#
# The path and package come from --go_opt=M rather than an `option go_package`, because a relative
# go_package makes protoc refuse the output path ("Output file names must never have a relative
# path"). Requires protoc-gen-go v1.36.5 to match the checked-in headers.
set -euo pipefail

MOD=github.com/confluentinc/confluent-kafka-go/v2
SR=schemaregistry

protoc -I"$SR" --go_out=. --go_opt=module="$MOD" \
  --go_opt="Mconfluent/type/decimal.proto=$MOD/$SR/confluent/type;typepb" \
  --go_opt="Mconfluent/type/variant.proto=$MOD/$SR/confluent/type;typepb" \
  --go_opt="Mconfluent/types/decimal.proto=$MOD/$SR/confluent/types;types" \
  confluent/type/decimal.proto confluent/type/variant.proto confluent/types/decimal.proto
