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
	ruleExecutorsMu sync.RWMutex
	ruleExecutors   = map[string]RuleExecutor{}
	ruleActionsMu   sync.RWMutex
	ruleActions     = map[string]RuleAction{}
)

// RegisterRuleExecutor is used to register a new rule executor.
func RegisterRuleExecutor(ruleExecutor RuleExecutor) {
	ruleExecutorsMu.Lock()
	defer ruleExecutorsMu.Unlock()
	ruleExecutors[ruleExecutor.Type()] = ruleExecutor
}

// GetRuleExecutor fetches a rule executor by a given name.
func GetRuleExecutor(name string) RuleExecutor {
	ruleExecutorsMu.RLock()
	defer ruleExecutorsMu.RUnlock()
	return ruleExecutors[name]
}

// GetRuleExecutors fetches all rule executors
func GetRuleExecutors() []RuleExecutor {
	ruleExecutorsMu.RLock()
	defer ruleExecutorsMu.RUnlock()
	var result []RuleExecutor
	for _, v := range ruleExecutors {
		result = append(result, v)
	}
	return result
}

// RegisterRuleAction is used to register a new rule action.
func RegisterRuleAction(ruleAction RuleAction) {
	ruleActionsMu.Lock()
	defer ruleActionsMu.Unlock()
	ruleActions[ruleAction.Type()] = ruleAction
}

// GetRuleAction fetches a rule action by a given name.
func GetRuleAction(name string) RuleAction {
	ruleActionsMu.RLock()
	defer ruleActionsMu.RUnlock()
	return ruleActions[name]
}

// GetRuleActions fetches all rule actions
func GetRuleActions() []RuleAction {
	ruleActionsMu.RLock()
	defer ruleActionsMu.RUnlock()
	var result []RuleAction
	for _, v := range ruleActions {
		result = append(result, v)
	}
	return result
}

// ClearRules clears all registered rules
func ClearRules() {
	ruleActionsMu.Lock()
	defer ruleActionsMu.Unlock()
	for k, v := range ruleActions {
		_ = v.Close()
		delete(ruleActions, k)
	}
	ruleExecutorsMu.Lock()
	defer ruleExecutorsMu.Unlock()
	for k, v := range ruleExecutors {
		_ = v.Close()
		delete(ruleExecutors, k)
	}
}
