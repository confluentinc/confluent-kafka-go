package cel

// A CEL_FIELD rule over the *null* branch of an Avro ["null", T] union must be evaluated, not
// skipped.
//
// Avro's null is a first-class value, and the reference binds it as CEL null so a rule can guard
// with `value == null`. Skipping the field instead removes that capability and is *silent*: a
// rule that never ran and a rule that ran and passed produce the same result, so nothing in a
// positive-only test can tell them apart.
//
// Three things had to change together. The executor's own `fieldValue == nil` guard is gone - the
// reference has none, and leaves the decision to each format's walk. The Avro walk no longer
// returns early on a nil pointer, so the union resolves to its "null" branch and the leaf reaches
// the rule with an untyped nil. And the adapter binds a nil pointer as CEL null, without which
// `value == null` answers false.
//
// The protobuf walk still skips an unset field, which is correct there: a field with presence
// that is unset has no value, and writing one back would materialise it.

import (
	"math/big"
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov3"
)

const nullFieldSchema = `{
  "type": "record",
  "name": "Nullable",
  "fields": [
    {"name": "amount",
     "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}],
     "confluent:tags": ["AMOUNT"]},
    {"name": "note", "type": ["null", "string"]},
    {"name": "plain", "type": "string"}
  ]
}`

type nullFieldRec struct {
	Amount *big.Rat `avro:"amount"`
	Note   *string  `avro:"note"`
	Plain  string   `avro:"plain"`
}

// runNullField serializes a record whose decimal field is nil under one CEL_FIELD condition,
// and returns the error text ("" when the rule passed).
func runNullField(t *testing.T, subject, expr string, amount *big.Rat) string {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{"AMOUNT"}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: nullFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := nullFieldRec{Amount: amount, Plain: "hi"}
	if _, err := ser.Serialize(subject, &obj); err != nil {
		return err.Error()
	}
	return ""
}

func TestNullAvroFieldReachesTheRule(t *testing.T) {
	// `value == null` can only pass if the null was bound and the rule ran.
	if got := runNullField(t, "nf1", "value == null", nil); got != "" {
		t.Fatalf("expected the guard to pass, got %q", got)
	}
}

func TestNullAvroFieldWasNotMerelySkipped(t *testing.T) {
	// The discriminator: `value != null` is false on a null, so it must FAIL. Without it the
	// test above is satisfied by a rule that never ran - a skipped field reports nothing either.
	got := runNullField(t, "nf2", "value != null", nil)
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected a condition failure, got %q", got)
	}
}

func TestUnguardedRuleOnANullAvroFieldFailsLoudly(t *testing.T) {
	got := runNullField(t, "nf3", `decimals.gt(decimal(value), decimal("10.00"))`, nil)
	if !strings.Contains(got, "cannot convert null") {
		t.Fatalf("expected a loud conversion failure, got %q", got)
	}
}

// The present-value pair runs against a *non-nullable* decimal field. A CEL_FIELD rule over a
// present value inside a ["null", T] union breaks serialization with "avro: unable to resolve
// type big.Rat" - a pre-existing defect in this client, unrelated to the null handling above:
// it reproduces with the expression `true` and with the walk unmodified. Using a plain field
// keeps this pair measuring what it is for, which is that removing the skip did not disturb the
// ordinary case.
const plainFieldSchema = `{
  "type": "record",
  "name": "Plain",
  "fields": [
    {"name": "amount",
     "type": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2},
     "confluent:tags": ["AMOUNT"]},
    {"name": "plain", "type": "string"}
  ]
}`

type plainFieldRec struct {
	Amount *big.Rat `avro:"amount"`
	Plain  string   `avro:"plain"`
}

func runPlainField(t *testing.T, subject, expr string) string {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{"AMOUNT"}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: plainFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := plainFieldRec{Amount: new(big.Rat).SetFrac64(1234, 100), Plain: "hi"}
	if _, err := ser.Serialize(subject, &obj); err != nil {
		return err.Error()
	}
	return ""
}

func TestPresentAvroFieldStillEvaluatesNormally(t *testing.T) {
	// The must-pass / must-fail pair: removing the skip must not disturb the ordinary case.
	if got := runPlainField(t, "nf4",
		`decimals.gt(decimal(value), decimal("10.00"))`); got != "" {
		t.Fatalf("expected 12.34 > 10.00 to pass, got %q", got)
	}
	got := runPlainField(t, "nf5", `decimals.gt(decimal(value), decimal("100.00"))`)
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected 12.34 > 100.00 to fail, got %q", got)
	}
}

// ---- writing a CEL null back into a ["null", T] union ---------------------------------------
//
// A message-level CEL transform hands its result back as a map, and a null in that map is
// cel-go's structpb.NullValue - a protobuf enum hamba has never seen, so it failed with
// "avro: unable to resolve type structpb.NullValue". That hit the two forms a rule author is
// most likely to write: an identity pass-through over a nullable field, and the
// `has(x) ? x : null` guard that is the only way to preserve absence.
//
// Asserted on the *deserialized* record: "the serializer did not throw" cannot tell a preserved
// null from a materialised zero, since writing either into a union encodes fine.

func runMsgTransform(t *testing.T, subject, expr string) (*nullFieldRec, string) {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL", Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: nullFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := nullFieldRec{Plain: "hi"} // amount nil
	bytes, err := ser.Serialize(subject, &obj)
	if err != nil {
		return nil, err.Error()
	}
	deser, err := avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var got nullFieldRec
	if err := deser.DeserializeInto(subject, bytes, &got); err != nil {
		return nil, "DESERR: " + err.Error()
	}
	return &got, ""
}

func TestPassThroughPreservesANullAvroField(t *testing.T) {
	got, errText := runMsgTransform(t, "mt1",
		`{"amount": message.Amount, "note": message.Note, "plain": message.Plain}`)
	if errText != "" {
		t.Fatalf("pass-through failed: %s", errText)
	}
	if got.Amount != nil {
		t.Errorf("amount = %v, want nil - the null must survive, not become a zero", got.Amount)
	}
	if got.Plain != "hi" {
		t.Errorf("plain = %q, want hi", got.Plain)
	}
}

func TestGuardedTransformPreservesANullAvroField(t *testing.T) {
	got, errText := runMsgTransform(t, "mt2",
		`{"amount": has(message.Amount) ? message.Amount : null, ` +
			`"note": message.Note, "plain": message.Plain}`)
	if errText != "" {
		t.Fatalf("guarded transform failed: %s", errText)
	}
	if got.Amount != nil {
		t.Errorf("amount = %v, want nil", got.Amount)
	}
}

// The twin: "the null survived" must not be "nothing was written at all". A transform that
// computes a real value for the same field has to set it.
// The twin: "the null survived" must not be "nothing was written at all". A transform that
// computes a real value for a nullable field has to set it.
//
// It uses the nullable *string*, not the nullable decimal: writing a computed decimal into a
// ["null", T] union yields a zero in this client, pre-existing and unrelated to the null
// handling here - it reproduces with avro_result_writer.go unmodified. See the doc's Go notes.
func TestTransformCanStillSetTheNullableField(t *testing.T) {
	got, errText := runMsgTransform(t, "mt3",
		`{"amount": message.Amount, "note": "written", "plain": message.Plain}`)
	if errText != "" {
		t.Fatalf("computed transform failed: %s", errText)
	}
	if got.Note == nil || *got.Note != "written" {
		t.Errorf("note = %v, want \"written\" - a real value must still reach the union", got.Note)
	}
	if got.Amount != nil {
		t.Errorf("amount = %v, want nil", got.Amount)
	}
}

// ---- the same four behaviours through avrov3 --------------------------------------------------
//
// avrov2 and avrov3 are kept in sync: their transform walks differ only in which avro library
// they import and in avrov2's extra RefSchema case. The null handling above was applied to both,
// so it is asserted through both - a fix to one walk and not the other is exactly the kind of
// drift this pairing exists to prevent.

func runNullFieldV3(t *testing.T, subject, expr string, amount *big.Rat) string {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{"AMOUNT"}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: nullFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov3.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov3.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := nullFieldRec{Plain: "hi"}
	if amount != nil {
		obj.Amount = amount
	}
	if _, err := ser.Serialize(subject, &obj); err != nil {
		return err.Error()
	}
	return ""
}

func TestNullAvroFieldReachesTheRuleInV3(t *testing.T) {
	if got := runNullFieldV3(t, "v3a", "value == null", nil); got != "" {
		t.Fatalf("expected the guard to pass, got %q", got)
	}
	// The discriminator, same as for avrov2: a skipped field would report nothing here.
	got := runNullFieldV3(t, "v3b", "value != null", nil)
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected a condition failure, got %q", got)
	}
	// And an expression that cannot handle a null still fails loudly.
	got = runNullFieldV3(t, "v3c", `decimals.gt(decimal(value), decimal("10.00"))`, nil)
	if !strings.Contains(got, "cannot convert null") {
		t.Fatalf("expected a loud conversion failure, got %q", got)
	}
}
