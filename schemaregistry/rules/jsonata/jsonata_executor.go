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

package jsonata

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/xiatechs/jsonata-go"
	"sync"
)

func init() {
	Register()
}

// Register registers the JSONata rule executor
func Register() {
	e := &Executor{
		cache: map[string]*jsonata.Expr{},
	}
	serde.RegisterRuleExecutor(e)
}

// Executor is a JSONata rule executor
type Executor struct {
	Config    map[string]string
	cache     map[string]*jsonata.Expr
	cacheLock sync.RWMutex
}

// Configure configures the executor
func (c *Executor) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	c.Config = config
	return nil
}

// Type returns the type of the executor
func (c *Executor) Type() string {
	return "JSONATA"
}

// Transform transforms the message using the rule
func (c *Executor) Transform(ctx serde.RuleContext, msg interface{}) (interface{}, error) {
	c.cacheLock.RLock()
	expr, ok := c.cache[ctx.Rule.Expr]
	c.cacheLock.RUnlock()
	var err error
	if !ok {
		expr, err = jsonata.Compile(ctx.Rule.Expr)
		if err != nil {
			return nil, err
		}
		c.cacheLock.Lock()
		c.cache[ctx.Rule.Expr] = expr
		c.cacheLock.Unlock()
	}

	res, err := expr.Eval(msg)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Close closes the executor
func (c *Executor) Close() error {
	return nil
}
