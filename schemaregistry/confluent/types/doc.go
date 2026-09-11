// Package types held the generated confluent.type.Decimal bindings until they moved to the
// canonical confluent/type path. What remains is generated from confluent/types/decimal.proto, a
// stub that declares nothing and publicly imports the canonical file, so Decimal and the
// descriptor variable stay available under their old names. Nothing inside the client uses this
// package; compat_test.go is what keeps it honest.
//
// Deprecated: use github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type
// instead, imported as typepb.
package types
