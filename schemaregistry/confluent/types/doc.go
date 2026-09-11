// Deprecated: use github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type
// instead, imported as `typepb`.
//
// This package held the generated confluent.type.Decimal bindings until they moved to the
// canonical confluent/type path - the one the Java client registers and ProtobufSchema declares.
// What remains is generated from confluent/types/decimal.proto, a stub that declares nothing and
// publicly imports the canonical file, so Decimal stays available under its old name and a
// descriptor built against the old import path still resolves. Nothing inside the client uses
// this package; see compat_test.go, which is what keeps it honest.
package types
