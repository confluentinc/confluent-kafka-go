/**
 * Copyright 2026 Confluent Inc.
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

package serde

import (
	"fmt"
	"strings"
)

// ValidationRulesExecution determines when inline validation rules run, relative to
// domain rule transformations.
type ValidationRulesExecution string

const (
	// ValidationRulesDisabled means inline validation rules are not evaluated
	ValidationRulesDisabled ValidationRulesExecution = "DISABLED"
	// ValidationRulesBeforeDomainRules evaluates inline validation rules on the original
	// message, before domain rule transformations
	ValidationRulesBeforeDomainRules ValidationRulesExecution = "BEFORE_DOMAIN_RULES"
	// ValidationRulesAfterDomainRules evaluates inline validation rules on the
	// transformed message, after domain rules
	ValidationRulesAfterDomainRules ValidationRulesExecution = "AFTER_DOMAIN_RULES"
)

// ValidationRulesProp is the schema property (Avro) / keyword (JSON Schema) that holds
// inline validation rules.
const ValidationRulesProp = "confluent:rules"

// ValidationRule is an inline validation rule (a CHECK constraint) declared on a schema,
// either on a record/message/object or on one of its fields.
type ValidationRule struct {
	Name string
	Doc  string
	Expr string
	SQL  string
}

// ValidationRuleError is a single inline validation rule failure, located at FieldPath
// within the message that was validated.
type ValidationRuleError struct {
	Rule      ValidationRule
	FieldPath string
	// Message is an optional dynamic error message returned by the rule itself — set when
	// the rule expression returned a non-empty string explaining the failure (e.g.
	// "x > 0 ? '' : 'x must be positive'"). Empty when the failure was a plain false or a
	// CEL evaluation error.
	Message string
	Cause   error
}

// Error implements the error interface
func (e ValidationRuleError) Error() string {
	path := e.FieldPath
	if path == "" {
		path = "<root>"
	}
	name := e.Rule.Name
	if name == "" {
		name = "unnamed"
	}
	// Prefer the dynamic message returned by the rule itself; fall back to the rule's
	// authored doc / SQL / CEL expression in that order.
	var detail string
	switch {
	case e.Message != "":
		detail = e.Message
	case e.Rule.Doc != "":
		detail = e.Rule.Doc
	case e.Rule.SQL != "":
		detail = e.Rule.SQL
	default:
		detail = e.Rule.Expr
	}
	result := fmt.Sprintf("%s: %s: %s", path, name, detail)
	if e.Cause != nil {
		result += fmt.Sprintf(" (caused by: %s)", e.Cause.Error())
	}
	return result
}

// Unwrap returns the underlying cause, if any
func (e ValidationRuleError) Unwrap() error {
	return e.Cause
}

// ValidationRulesFailedErr aggregates every inline validation rule failure found while
// walking a message.
type ValidationRulesFailedErr struct {
	Violations []ValidationRuleError
}

// Error implements the error interface
func (e ValidationRulesFailedErr) Error() string {
	count := len(e.Violations)
	if count == 0 {
		return "Validation rule failed (no detail)"
	}
	plural := "s"
	if count == 1 {
		plural = ""
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "Validation rule failed (%d violation%s):", count, plural)
	for _, violation := range e.Violations {
		fmt.Fprintf(&sb, "\n  - %s", violation.Error())
	}
	return sb.String()
}

// ValidationRuleExecutor evaluates a single inline validation rule against a value.
//
// Implementations return either a bool (false meaning the rule failed) or a string
// (non-empty meaning the rule failed, with that string as the failure message).
type ValidationRuleExecutor interface {
	Execute(rule ValidationRule, schema interface{}, msg interface{}) (interface{}, error)
}

// ValidationEnabled returns true when inline validation rules should run at the given
// phase.
//
// Pass an empty phase when there is a single validation point — a serialization path that
// applies no domain rules has nothing to run before or after, so any enabled mode
// validates there.
func (s *BaseSerializer) ValidationEnabled(phase ValidationRulesExecution) bool {
	if s.Conf == nil {
		return false
	}
	execution := s.Conf.ValidationRulesExecution
	if execution == "" {
		execution = ValidationRulesDisabled
	}
	if phase == "" {
		return execution != ValidationRulesDisabled
	}
	return execution == phase
}

// ValidationExecutor returns the executor used to evaluate inline validation rules: the
// one configured on the serializer if set, otherwise the globally registered default.
func (s *BaseSerializer) ValidationExecutor() (ValidationRuleExecutor, error) {
	if s.Conf != nil && s.Conf.ValidationRuleExecutor != nil {
		return s.Conf.ValidationRuleExecutor, nil
	}
	executor := GetValidationRuleExecutor()
	if executor == nil {
		return nil, fmt.Errorf("no validation rule executor registered; import " +
			"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel or set " +
			"ValidationRuleExecutor on the serializer config")
	}
	return executor, nil
}

// ValidationRulesFailed returns a single error aggregating every violation found, or nil
// when there are none.
func ValidationRulesFailed(violations []ValidationRuleError) error {
	if len(violations) == 0 {
		return nil
	}
	return ValidationRulesFailedErr{Violations: violations}
}

// ParseValidationRules parses a "confluent:rules" property value — a list of maps with
// name/doc/expr/sql keys. Missing or malformed entries are ignored, yielding an empty
// slice.
func ParseValidationRules(prop interface{}) []ValidationRule {
	entries, ok := prop.([]interface{})
	if !ok {
		return nil
	}
	var rules []ValidationRule
	for _, entry := range entries {
		m, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		rules = append(rules, ValidationRule{
			Name: stringProp(m, "name"),
			Doc:  stringProp(m, "doc"),
			Expr: stringProp(m, "expr"),
			SQL:  stringProp(m, "sql"),
		})
	}
	return rules
}

func stringProp(m map[string]interface{}, key string) string {
	value, ok := m[key].(string)
	if !ok {
		return ""
	}
	return value
}

// EvaluateValidationRule evaluates one inline validation rule, appending a
// ValidationRuleError to out when it fails. A rule that returns an error is itself
// recorded as a violation so the walk can continue; a rule resolving to something other
// than a bool or string is a programming error and is returned.
func EvaluateValidationRule(executor ValidationRuleExecutor, rule ValidationRule, schema interface{},
	value interface{}, path string, out *[]ValidationRuleError) error {
	result, err := executor.Execute(rule, schema, value)
	if err != nil {
		*out = append(*out, ValidationRuleError{Rule: rule, FieldPath: path, Cause: err})
		return nil
	}
	switch typed := result.(type) {
	case bool:
		if !typed {
			*out = append(*out, ValidationRuleError{Rule: rule, FieldPath: path})
		}
	case string:
		if typed != "" {
			*out = append(*out, ValidationRuleError{Rule: rule, FieldPath: path, Message: typed})
		}
	default:
		name := rule.Name
		if name == "" {
			name = "unnamed"
		}
		return fmt.Errorf("validation rule '%s' resolved to an unexpected type: %T", name, result)
	}
	return nil
}
