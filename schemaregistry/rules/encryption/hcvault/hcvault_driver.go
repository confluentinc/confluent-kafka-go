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

package hcvault

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"os"
)

const (
	prefix    = "hcvault://"
	tokenID   = "token.id"
	namespace = "namespace"
)

func init() {
	Register()
}

// Register registers the HashiCorp Vault driver
func Register() {
	driver := &hcvaultDriver{}
	encryption.RegisterKMSDriver(driver)
}

type hcvaultDriver struct {
}

func (l *hcvaultDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *hcvaultDriver) NewKMSClient(config map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	ns := config[namespace]
	token := config[tokenID]
	if token == "" {
		ns = os.Getenv("VAULT_NAMESPACE")
		token = os.Getenv("VAULT_TOKEN")
	}
	return NewClient(uriPrefix, nil, ns, token)
}
