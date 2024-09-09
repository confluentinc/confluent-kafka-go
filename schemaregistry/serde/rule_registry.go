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
	globalInstance = RuleRegistry{
		ruleExecutors: make(map[string]RuleExecutor),
		ruleActions:   make(map[string]RuleAction),
	}
)

// RuleRegistry is used to store all registered rule executors and actions.
type RuleRegistry struct {
	ruleExecutorsMu sync.RWMutex
	ruleExecutors   map[string]RuleExecutor
	ruleActionsMu   sync.RWMutex
	ruleActions     map[string]RuleAction
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

// Clear clears all registered rules
func (r *RuleRegistry) Clear() {
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
