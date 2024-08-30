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

package gcpkms

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	kms "github.com/tink-crypto/tink-go-gcpkms/v2/integration/gcpkms"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"google.golang.org/api/option"
)

const (
	prefix       = "gcp-kms://"
	accountType  = "account.type"
	clientID     = "client.id"
	clientEmail  = "client.email"
	privateKeyID = "private.key.id"
	privateKey   = "private.key"
)

func init() {
	Register()
}

// Register registers the GCP KMS driver
func Register() {
	driver := &gcpDriver{}
	encryption.RegisterKMSDriver(driver)
}

type gcpDriver struct {
}

func (l *gcpDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *gcpDriver) NewKMSClient(config map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	var clientOption *option.ClientOption
	account, ok := config[accountType]
	if !ok {
		account = "service_account"
	}
	id, ok1 := config[clientID]
	email, ok2 := config[clientEmail]
	keyID, ok3 := config[privateKeyID]
	key, ok4 := config[privateKey]
	if ok1 && ok2 && ok3 && ok4 {
		creds := credentials{
			Type:         account,
			PrivateKeyID: keyID,
			PrivateKey:   key,
			ClientEmail:  email,
			ClientID:     id,
		}
		bytes, err := json.Marshal(creds)
		if err != nil {
			return nil, err
		}
		opt := option.WithCredentialsJSON(bytes)
		clientOption = &opt
	}
	if clientOption != nil {
		return kms.NewClientWithOptions(context.Background(), uriPrefix, *clientOption)
	}
	return kms.NewClientWithOptions(context.Background(), uriPrefix)
}

type credentials struct {
	Type         string `json:"type"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	ClientID     string `json:"client_id"`
}
