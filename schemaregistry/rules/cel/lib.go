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
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/env"
	"cel.dev/cel-go/common/operators"
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
			ExcludeFunctions: []*env.Function{{Name: operators.In}},
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
}

func (a decimalAdapter) NativeToValue(value any) ref.Val {
	if dv, ok := decimalBoundaryValue(value); ok {
		return dv
	}
	return a.inner.NativeToValue(value)
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
