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

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
)

// DefaultEnv produces a cel.Env with the necessary cel.EnvOption and
// cel.ProgramOption values preconfigured for usage throughout the
// module.
func DefaultEnv() (*cel.Env, error) {
	return cel.NewEnv(cel.Lib(lib{}))
}

type lib struct {
}

func (l lib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.CrossTypeNumericComparisons(true),
		cel.EagerlyValidateDeclarations(true),
		ext.Strings(ext.StringsValidateFormatCalls(true)),
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
