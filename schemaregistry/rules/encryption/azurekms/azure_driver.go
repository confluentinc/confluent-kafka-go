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

package azurekms

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

const (
	prefix       = "azure-kms://"
	tenantID     = "tenant.id"
	clientID     = "client.id"
	clientSecret = "client.secret"
)

func init() {
	Register()
}

// Register registers the Azure KMS driver
func Register() {
	driver := &azureDriver{}
	encryption.RegisterKMSDriver(driver)
}

type azureDriver struct {
}

func (l *azureDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *azureDriver) NewKMSClient(config map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	var creds azcore.TokenCredential
	var err error
	tenant, ok := config[tenantID]
	if ok {
		client, ok := config[clientID]
		if ok {
			secret, ok := config[clientSecret]
			if ok {
				creds, err = azidentity.NewClientSecretCredential(tenant, client, secret, nil)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if creds == nil {
		creds, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
	}
	return NewClient(uriPrefix, creds, defaultEncryptionAlgorithm)
}
