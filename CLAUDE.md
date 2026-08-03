# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

confluent-kafka-go is Confluent's Go client for Apache Kafka. It is a CGo wrapper around [librdkafka](https://github.com/confluentinc/librdkafka), a high-performance C library. The module path is `github.com/confluentinc/confluent-kafka-go/v2`.

## Build & Test Commands

### Building
```bash
# Default build (uses statically linked vendored librdkafka)
go build ./...

# Build with dynamically linked librdkafka (requires librdkafka installed via pkg-config)
go build -tags dynamic ./...
```

### Testing
```bash
# Run unit tests for the kafka package (no broker required)
go test ./kafka/...

# Run a single test
go test ./kafka/ -run TestLibraryVersion

# Run unit tests for schema registry
go test ./schemaregistry/...

# Integration tests live in a separate sub-module (kafka/integration/) so the
# testcontainers + testify deps don't bleed into the main kafka module's go.mod.
# Run them from within that module. Tests use docker-compose via testcontainers.
# Use these flags:
(cd kafka/integration && go test ./... -docker.needed)    # auto-starts Docker containers
(cd kafka/integration && go test ./... -docker.exists)    # uses already-running containers

# Run all tests across all packages
make -f mk/Makefile "go test"

# Vet all packages
make -f mk/Makefile "go vet"
```

### Code Generation
```bash
# Regenerate error codes from librdkafka (produces kafka/generated_errors.go)
make -f mk/Makefile generr
```

## Architecture

### Two main packages

**`kafka/`** — Core Kafka client (Producer, Consumer, AdminClient)
- CGo bindings wrapping librdkafka via `#include "select_rdkafka.h"`
- librdkafka is vendored as static libraries in `kafka/librdkafka_vendor/` with per-platform `.a` files and build-tag-gated `build_*.go` files
- Use build tag `dynamic` to link against system librdkafka instead of vendored static libs
- Key types: `Producer`, `Consumer`, `AdminClient`, `ConfigMap`, `Message`, `Event`, `Handle` (interface shared by Producer/Consumer)
- Configuration uses `ConfigMap` (a `map[string]ConfigValue`) matching librdkafka config property names; Go-specific properties are prefixed with `go.`
- Events are delivered through channels or `.Poll()` — the `Event` interface is implemented by `*Message`, `Error`, `AssignedPartitions`, `RevokedPartitions`, etc.
- `kafka/generated_errors.go` is auto-generated from librdkafka error codes — do not edit manually

**`schemaregistry/`** — Confluent Schema Registry client and serialization/deserialization (serde)
- `schemaregistry_client.go` — REST client for Schema Registry API (register/lookup schemas, compatibility checks)
- `serde/` — Serializer/Deserializer framework with format-specific implementations:
  - `serde/avro/`, `serde/avrov2/`, and `serde/avrov3/` — Avro (gogen-avro, hamba/avro, and confluentinc/confluent-avro-go respectively)
  - `serde/jsonschema/` — JSON Schema
  - `serde/protobuf/` — Protocol Buffers
- `rules/` — Data contract rules engine (CEL, JSONata, field-level encryption)
- `cache/` — Schema caching (LRU and map-based)

### Test Configuration

Integration tests for `kafka/` live in the `kafka/integration/` **sub-module** (own `go.mod`, own `testresources/`) — they use a `testconf` struct populated from `testconf.json` (if present) or defaults to `localhost:9092`, and can spin up Docker containers via testcontainers-go when `-docker.needed` is set. The sub-module isolates the testcontainers + testify dependency surface from the main `kafka` module.

Schema registry integration tests live under `schemaregistry/test/` and use `testcontainers-go/modules/compose`.

## Git

- Always use `git push-external` instead of `git push` (proprietary code check required).
