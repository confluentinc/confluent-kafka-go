/**
 * Copyright 2024 Confluent Inc.
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
	"sync"
)

var (
	globalInstance = NewRuleRegistry()
)

// RuleOverride represents a rule override
type RuleOverride struct {
	// Rule type
	Type string
	// Rule action on success
	OnSuccess *string
	// Rule action on failure
	OnFailure *string
	// Whether the rule is disabled
	Disabled *bool
}

// RuleRegistry is used to store all registered rule executors and actions.
type RuleRegistry struct {
	ruleExecutorsMu sync.RWMutex
	ruleExecutors   map[string]RuleExecutor
	ruleActionsMu   sync.RWMutex
	ruleActions     map[string]RuleAction
	ruleOverridesMu sync.RWMutex
	ruleOverrides   map[string]*RuleOverride
}

// NewRuleRegistry creates a Rule Registry instance.
func NewRuleRegistry() RuleRegistry {
	return RuleRegistry{
		ruleExecutors: make(map[string]RuleExecutor),
		ruleActions:   make(map[string]RuleAction),
		ruleOverrides: make(map[string]*RuleOverride),
	}
}

// RegisterExecutor is used to register a new rule executor.
func (r *RuleRegistry) RegisterExecutor(ruleExecutor RuleExecutor) {
	r.ruleExecutorsMu.Lock()
	defer r.ruleExecutorsMu.Unlock()
	r.ruleExecutors[ruleExecutor.Type()] = ruleExecutor
}

// GetExecutor fetches a rule executor by a given name.
func (r *RuleRegistry) GetExecutor(name string) RuleExecutor {
	r.ruleExecutorsMu.RLock()
	defer r.ruleExecutorsMu.RUnlock()
	return r.ruleExecutors[name]
}

// GetExecutors fetches all rule executors
func (r *RuleRegistry) GetExecutors() []RuleExecutor {
	r.ruleExecutorsMu.RLock()
	defer r.ruleExecutorsMu.RUnlock()
	var result []RuleExecutor
	for _, v := range r.ruleExecutors {
		result = append(result, v)
	}
	return result
}

// RegisterAction is used to register a new global rule action.
func (r *RuleRegistry) RegisterAction(ruleAction RuleAction) {
	r.ruleActionsMu.Lock()
	defer r.ruleActionsMu.Unlock()
	r.ruleActions[ruleAction.Type()] = ruleAction
}

// GetAction fetches a rule action by a given name.
func (r *RuleRegistry) GetAction(name string) RuleAction {
	r.ruleActionsMu.RLock()
	defer r.ruleActionsMu.RUnlock()
	return r.ruleActions[name]
}

// GetActions fetches all rule actions
func (r *RuleRegistry) GetActions() []RuleAction {
	r.ruleActionsMu.RLock()
	defer r.ruleActionsMu.RUnlock()
	var result []RuleAction
	for _, v := range r.ruleActions {
		result = append(result, v)
	}
	return result
}

// RegisterOverride is used to register a new global rule override.
func (r *RuleRegistry) RegisterOverride(ruleOverride *RuleOverride) {
	r.ruleOverridesMu.Lock()
	defer r.ruleOverridesMu.Unlock()
	r.ruleOverrides[ruleOverride.Type] = ruleOverride
}

// GetOverride fetches a rule override by a given name.
func (r *RuleRegistry) GetOverride(name string) *RuleOverride {
	r.ruleOverridesMu.RLock()
	defer r.ruleOverridesMu.RUnlock()
	return r.ruleOverrides[name]
}

// GetOverrides fetches all rule overrides
func (r *RuleRegistry) GetOverrides() []*RuleOverride {
	r.ruleOverridesMu.RLock()
	defer r.ruleOverridesMu.RUnlock()
	var result []*RuleOverride
	for _, v := range r.ruleOverrides {
		result = append(result, v)
	}
	return result
}

// Clear clears all registered rules
func (r *RuleRegistry) Clear() {
	r.ruleOverridesMu.Lock()
	defer r.ruleOverridesMu.Unlock()
	for k := range r.ruleOverrides {
		delete(r.ruleOverrides, k)
	}
	r.ruleActionsMu.Lock()
	defer r.ruleActionsMu.Unlock()
	for k, v := range r.ruleActions {
		_ = v.Close()
		delete(r.ruleActions, k)
	}
	r.ruleExecutorsMu.Lock()
	defer r.ruleExecutorsMu.Unlock()
	for k, v := range r.ruleExecutors {
		_ = v.Close()
		delete(r.ruleExecutors, k)
	}
}

// GlobalRuleRegistry returns the global rule registry.
func GlobalRuleRegistry() *RuleRegistry {
	return &globalInstance
}

// RegisterRuleExecutor is used to register a new global rule executor.
func RegisterRuleExecutor(ruleExecutor RuleExecutor) {
	globalInstance.RegisterExecutor(ruleExecutor)
}

// RegisterRuleAction is used to register a new global rule action.
func RegisterRuleAction(ruleAction RuleAction) {
	globalInstance.RegisterAction(ruleAction)
}

// RegisterRuleOverride is used to register a new global rule override.
func RegisterRuleOverride(ruleOverride *RuleOverride) {
	globalInstance.RegisterOverride(ruleOverride)
}
