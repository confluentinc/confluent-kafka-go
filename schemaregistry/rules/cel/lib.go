/**
 * Copyright 2024 Confluent Inc.
 * Copyright 2023-2024 Buf Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cel

import (
	"github.com/google/uuid"
	"net"
	"net/mail"
	"net/url"
	"reflect"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/common/env"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/overloads"
	"cel.dev/cel-go/ext"
)

// DefaultEnv produces a cel.Env with the necessary cel.EnvOption and
// cel.ProgramOption values preconfigured for usage throughout the
// module.
func DefaultEnv() (*cel.Env, error) {
	// NewCustomEnv rather than NewEnv, so the standard library can be subsetted: NewEnv extends a
	// cached, fully-populated standard environment, which leaves no way to take a standard
	// function over. The three excluded here are re-declared by decimalOptions with decimal-aware
	// implementations that delegate to the standard behaviour for everything else - see
	// decimalEqualityOptions. This mirrors the Java client's
	// setStandardFunctions().excludeFunctions(EQUALS, NOT_EQUALS, IN).
	base, err := cel.NewCustomEnv(
		cel.StdLib(cel.StdLibSubset(&env.LibrarySubset{
			// Only @in is excluded. == and != cannot be taken over from the registry at all:
			// cel-go's planner switches on the function name before it consults the dispatcher
			// (interpreter/planner.go, planCallEqual/planCallNotEqual) and emits an interpretable
			// that calls a.Equal(b) directly, so a re-declared binding is never reached. @in is
			// not in that switch, so it is subsettable. Equality is handled on the value side
			// instead, by the adapter below.
			//
			// string(timestamp) is excluded at the *overload* level (a subset entry need only
			// name the overload id). cel-go's builtin renders with time.RFC3339Nano, which
			// strips trailing zeros from the fraction - `.1Z` where Java, C++ and JS give
			// `.100Z`. Re-declared by timestampStringOption below to emit whole 3/6/9-digit
			// groups like protobuf's Timestamps.toString.
			ExcludeFunctions: []*env.Function{
				{Name: operators.In},
				{Name: overloads.TypeConvertString, Overloads: []*env.Overload{
					{ID: overloads.TimestampToString},
				}},
			},
		})),
		cel.Lib(lib{}),
	)
	return base, err
}

// decimalAdapter presents a decimal-shaped native value as this package's CEL decimal, so that
// its Equal is numeric rather than a structural comparison of the protobuf encoding. This is what
// makes `this.amount == other` numeric for a decimal reached by *selection*, which no boundary
// conversion can see: cel-go's planner routes == to the value's own Equal.
//
// Installed by buildProgram rather than here, and deliberately: it wraps the adapter of the env
// it is installed on, and the per-program env is extended with cel.Types(...) first. Wrapping the
// base env's adapter instead would capture a registry that never learns those descriptors, and
// every message would fail to adapt with "unknown type".
type decimalAdapter struct {
	inner types.Adapter
	// The same field-name mapping buildProgram gave ext.NativeTypes, so nullAwareObj resolves
	// a field by the name the *declaration* used - Go names for a domain rule, schema names
	// for a validation rule.
	fieldName func(reflect.StructField) string
}

func (a decimalAdapter) NativeToValue(value any) ref.Val {
	// An Avro union's null branch reaches CEL as a nil pointer: hamba models a nullable
	// field as *T, so the struct field exists but holds no value. CEL's representation of
	// that is null, and binding it as anything else makes `value == null` - the guard the
	// reference recommends for exactly this case - answer false.
	//
	// This has to precede the decimal arm: a nil *big.Rat satisfies it and was read as a
	// *zero* decimal, so `decimals.gt(message.amount, decimal("10.00"))` came back a quiet
	// false where the reference raises. Deliberately limited to pointers and interfaces -
	// a nil map or slice is an *empty* collection, not an absent one, and the reference
	// binds those as empty rather than null.
	if isNilPointer(value) {
		return types.NullValue
	}
	if dv, ok := decimalBoundaryValue(value); ok {
		return dv
	}
	inner := a.inner.NativeToValue(value)
	// A Go struct that cel-go modelled as an object gets the null-aware view. A big.Rat is
	// caught above, and a time.Time converts to a CEL timestamp, which is not an Indexer -
	// so the trait check is what keeps this to records rather than every struct.
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Struct {
		if _, ok := inner.(traits.Indexer); ok {
			return nullAwareObj{Val: inner, value: rv, fieldName: a.fieldName, adapter: a}
		}
	}
	return inner
}

// nullAwareObj presents a Go struct to CEL with its nil pointer fields bound as CEL null.
//
// cel-go substitutes a freshly allocated *zero* for a nil pointer field before any adapter is
// consulted (common/types/native.go, getFieldValue), so a nullable Avro field modelled as `*T`
// reaches a rule as a pointer to a zero T. `message.amount == null` then answers false while
// `has(message.amount)` answers false too - the two disagree - and
// `decimals.gt(message.amount, decimal("10.00"))` compares against zero instead of failing.
// Routing field selection through here makes an absent value what CEL says it is.
//
// Everything but Get is delegated, Type() included: the checker's declarations come from
// ext.NativeTypes and the runtime type has to be the same one, or comparisons and `type()`
// disagree with the plan the checker produced.
type nullAwareObj struct {
	ref.Val                                    // the wrapped nativeObj: Type, Equal, ConvertTo*
	value     reflect.Value                    // the struct itself, for field lookup
	fieldName func(reflect.StructField) string // the same mapping the declaration used
	adapter   types.Adapter                    // converts a field value, recursively
}

// Get resolves a field the way the declaration named it, and reports a nil pointer as null.
func (o nullAwareObj) Get(index ref.Val) ref.Val {
	name, ok := index.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(index)
	}
	field, found := o.lookup(string(name))
	if !found {
		// Delegate rather than invent an error, so the message stays cel-go's own
		// ("no such field: x") and unknown-field behaviour is unchanged.
		return o.delegateGet(index)
	}
	if field.Kind() == reflect.Pointer && field.IsNil() {
		return types.NullValue
	}
	if !field.CanInterface() {
		return o.delegateGet(index)
	}
	return o.adapter.NativeToValue(field.Interface())
}

func (o nullAwareObj) delegateGet(index ref.Val) ref.Val {
	if idx, ok := o.Val.(traits.Indexer); ok {
		return idx.Get(index)
	}
	return types.MaybeNoSuchOverloadErr(index)
}

// IsSet has to be declared explicitly. `ref.Val` does not carry it, so embedding does not
// promote it, and without it this type stops satisfying traits.FieldTester - which would make
// every `has()` over an Avro record fail.
func (o nullAwareObj) IsSet(field ref.Val) ref.Val {
	if ft, ok := o.Val.(traits.FieldTester); ok {
		return ft.IsSet(field)
	}
	return types.MaybeNoSuchOverloadErr(field)
}

// lookup mirrors cel-go's own field naming: the handler when one was installed, the Go field
// name otherwise (common/types/native.go, fieldName). It is duplicated rather than reused
// because cel-go exposes no name-to-field resolution, and it is fed the *same* function object
// buildProgram passed to ext.NativeTypes so the two cannot drift.
func (o nullAwareObj) lookup(name string) (reflect.Value, bool) {
	t := o.value.Type()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		n := sf.Name
		if o.fieldName != nil {
			n = o.fieldName(sf)
		}
		if n == name {
			return o.value.Field(i), true
		}
	}
	return reflect.Value{}, false
}


// isNilPointer reports whether value is a nil pointer or nil interface, the two shapes an
// absent Avro union branch takes in a Go struct.
func isNilPointer(value any) bool {
	if value == nil {
		return true
	}
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

type lib struct {
}

func (l lib) CompileOptions() []cel.EnvOption {
	opts := []cel.EnvOption{
		cel.CrossTypeNumericComparisons(true),
		cel.EagerlyValidateDeclarations(true),
		ext.Strings(ext.StringsValidateFormatCalls(true)),
		ext.Math(),
		cel.Function("isHostname",
			cel.MemberOverload(
				"string_is_hostname_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					host, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(l.validateHostname(host))
				}),
			),
		),
		cel.Function("isEmail",
			cel.MemberOverload(
				"string_is_email_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(l.validateEmail(addr))
				}),
			),
		),
		cel.Function("isIpv4",
			cel.MemberOverload(
				"string_is_ipv4_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(l.validateIP(addr, 4))
				}),
			),
		),
		cel.Function("isIpv6",
			cel.MemberOverload(
				"string_is_ipv6_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(l.validateIP(addr, 6))
				}),
			),
		),
		cel.Function("isUri",
			cel.MemberOverload(
				"string_is_uri_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					uri, err := url.Parse(s)
					return types.Bool(err == nil && uri.IsAbs())
				}),
			),
		),
		cel.Function("isUriRef",
			cel.MemberOverload(
				"string_is_uri_ref_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					_, err := url.Parse(s)
					return types.Bool(err == nil)
				}),
			),
		),
		cel.Function("isUuid",
			cel.MemberOverload(
				"string_is_uuid_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					_, err := uuid.Parse(s)
					return types.Bool(err == nil)
				}),
			),
		),
	}
	opts = append(opts, decimalOptions()...)
	opts = append(opts, decimalEqualityOptions()...)
	opts = append(opts, timestampOptions()...)
	opts = append(opts, variantOptions()...)
	return opts
}

func (l lib) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{
		cel.EvalOptions(
			cel.OptOptimize,
		),
	}
}

func (l lib) validateEmail(addr string) bool {
	a, err := mail.ParseAddress(addr)
	if err != nil || strings.ContainsRune(addr, '<') || a.Address != addr {
		return false
	}

	addr = a.Address
	if len(addr) > 254 {
		return false
	}

	parts := strings.SplitN(addr, "@", 2)
	return len(parts[0]) <= 64 && l.validateHostname(parts[1])
}

func (l lib) validateHostname(host string) bool {
	if len(host) > 253 {
		return false
	}

	s := strings.ToLower(strings.TrimSuffix(host, "."))
	allDigits := false
	// split hostname on '.' and validate each part
	for _, part := range strings.Split(s, ".") {
		allDigits = true
		// if part is empty, longer than 63 chars, or starts/ends with '-', it is invalid
		if l := len(part); l == 0 || l > 63 || part[0] == '-' || part[l-1] == '-' {
			return false
		}
		// for each character in part
		for _, ch := range part {
			// if the character is not a-z, 0-9, or '-', it is invalid
			if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
				return false
			}
			allDigits = allDigits && ch >= '0' && ch <= '9'
		}
	}

	// the last part cannot be all numbers
	return !allDigits
}

func (l lib) validateIP(addr string, ver int64) bool {
	address := net.ParseIP(addr)
	if address == nil {
		return false
	}
	switch ver {
	case 0:
		return true
	case 4:
		return address.To4() != nil
	case 6:
		return address.To4() == nil
	default:
		return false
	}
}
