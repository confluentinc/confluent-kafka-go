package cel

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
)

// Two result entries naming the same slot. Applying both left the outcome to the order they
// were visited in, which for a Go map is not even stable between runs: {one_id, other_id} kept
// whichever came out first, because setting a oneof member clears its siblings. JsonFormat -
// the reference's write-back path - refuses both shapes, with opposite null handling, measured
// against protobuf-java:
//
//	{"oneofMessage":{..},"oneofString":"s"}   Cannot set field ...oneof_string because another
//	                                         field ...oneof_message belonging to the same
//	                                         oneof has already been set
//	{"oneofString":"s","oneofMessage":null}  OK - a null does not occupy the oneof
//	{"total_amount":{..},"totalAmount":{..}} Field ...total_amount has already been set.
//	{"total_amount":{..},"totalAmount":null} Field ...total_amount has already been set.
//	{"total_amount":null,"totalAmount":{..}} OK - a null did not set it
//
// Python, C++ and Rust already carried these two checks; Go, C# and JavaScript did not.
func TestResultNamingTwoMembersOfOneOneofIsReported(t *testing.T) {
	out := (&test.ComplexType{}).ProtoReflect().New()
	err := fillMessage(out, map[string]interface{}{
		"one_id":   "x",
		"other_id": int64(7),
	})
	if err == nil {
		t.Fatal("expected an error for two members of one oneof")
	}
	// Sorted, so the message does not vary with the map iteration order that produced it.
	if !strings.Contains(err.Error(), "more than one member of oneof") ||
		!strings.Contains(err.Error(), "one_id and other_id") {
		t.Errorf("error should name both members in a stable order, got %v", err)
	}
}

// A null does not occupy the oneof, so this is the reference's OK case.
func TestANullAlongsideAOneofMemberIsAccepted(t *testing.T) {
	out := (&test.ComplexType{}).ProtoReflect().New()
	if err := fillMessage(out, map[string]interface{}{
		"one_id":   "x",
		"other_id": nil,
	}); err != nil {
		t.Fatalf("a null sibling should be accepted: %v", err)
	}
	got := out.Interface().(*test.ComplexType)
	if got.GetOneId() != "x" {
		t.Errorf("one_id = %q, want x", got.GetOneId())
	}
}

// The must-fail twin: a single member still writes.
func TestASingleOneofMemberStillWrites(t *testing.T) {
	out := (&test.ComplexType{}).ProtoReflect().New()
	if err := fillMessage(out, map[string]interface{}{"other_id": int64(7)}); err != nil {
		t.Fatalf("a single member should write: %v", err)
	}
	got := out.Interface().(*test.ComplexType)
	if got.GetOtherId() != 7 {
		t.Errorf("other_id = %d, want 7", got.GetOtherId())
	}
	if !proto.Equal(got, &test.ComplexType{SomeVal: &test.ComplexType_OtherId{OtherId: 7}}) {
		t.Errorf("unexpected message: %v", got)
	}
}

// findField accepts a field's declared name and its JSON name, so these are one field.
func TestResultNamingOneFieldUnderBothSpellingsIsReported(t *testing.T) {
	out := (&test.ComplexType{}).ProtoReflect().New()
	err := fillMessage(out, map[string]interface{}{
		"is_active": true,
		"isActive":  false,
	})
	if err == nil {
		t.Fatal("expected an error for one field named twice")
	}
	if !strings.Contains(err.Error(), "twice") ||
		!strings.Contains(err.Error(), "isActive and is_active") {
		t.Errorf("error should name both spellings in a stable order, got %v", err)
	}
}

// A field named once, with a null, clears it - the ordinary way a rule preserves an absent
// value across a transform that echoes it.
func TestALoneNullClearsTheField(t *testing.T) {
	out := (&test.ComplexType{}).ProtoReflect().New()
	if err := fillMessage(out, map[string]interface{}{"is_active": nil}); err != nil {
		t.Fatalf("a lone null should clear rather than fail: %v", err)
	}
	if out.Interface().(*test.ComplexType).GetIsActive() {
		t.Error("is_active should have been cleared")
	}
}

// A field named twice is refused whichever entry carries the null, and refused *every* time.
// Before the fix this pair was accepted ~87% of the time, so the repetition is the assertion:
// a single run proves nothing about an order-dependent verdict.
func TestAFieldNamedTwiceIsRefusedWhicheverEntryIsNull(t *testing.T) {
	for name, values := range map[string]map[string]interface{}{
		"null and value": {"is_active": nil, "isActive": true},
		"value and null": {"is_active": true, "isActive": nil},
		"two values":     {"is_active": true, "isActive": true},
	} {
		for i := 0; i < 1000; i++ {
			out := (&test.ComplexType{}).ProtoReflect().New()
			err := fillMessage(out, values)
			if err == nil {
				t.Fatalf("%s: accepted on run %d; the verdict must not vary", name, i)
			}
			if !strings.Contains(err.Error(), "twice") ||
				!strings.Contains(err.Error(), "isActive and is_active") {
				t.Fatalf("%s: error should name both spellings in a stable order, got %v",
					name, err)
			}
		}
	}
}
