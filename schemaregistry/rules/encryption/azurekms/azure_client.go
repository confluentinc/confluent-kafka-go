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
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/tink-crypto/tink-go/v2/core/registry"

	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	defaultEncryptionAlgorithm = azkeys.EncryptionAlgorithmRSAOAEP256
)

// azureClient represents an Azure client
type azureClient struct {
	keyURI    string
	creds     azcore.TokenCredential
	algorithm azkeys.EncryptionAlgorithm
	config    map[string]string
}

// NewClient returns a new Azure KMS client
func NewClient(keyURI string, creds azcore.TokenCredential, algorithm azkeys.EncryptionAlgorithm, config map[string]string) (registry.KMSClient, error) {
	if !strings.HasPrefix(strings.ToLower(keyURI), prefix) {
		return nil, fmt.Errorf("keyURI must start with %s, but got %s", prefix, keyURI)
	}
	return &azureClient{
		keyURI:    keyURI,
		creds:     creds,
		algorithm: algorithm,
		config:    config,
	}, nil
}

// Supported true if this client does support keyURI
func (c *azureClient) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, c.keyURI)
}

// GetAEAD gets an AEAD backend by keyURI.
// keyURI must have the following format: 'azure-kms://https://{vaultURL}/keys/{keyName}/{keyVersion}'
// where keyVersion is optional and if not provided, the latest version will be used.
func (c *azureClient) GetAEAD(keyURI string) (tink.AEAD, error) {
	if !c.Supported(keyURI) {
		return nil, fmt.Errorf("keyURI must start with prefix %s, but got %s", c.keyURI, keyURI)
	}
	uri := strings.TrimPrefix(keyURI, prefix)

	saveVersion, _ := strconv.ParseBool(c.config[EncryptAzureKeyVersionSave])
	if !saveVersion {
		if _, _, keyVersion, err := getKeyInfo(uri); err == nil && keyVersion == "" {
			log.Printf("WARN: Azure Key Vault key '%s' is versionless and %s is not enabled; "+
				"DEKs wrapped with it may become undecryptable after the key is rotated.\n",
				uri, EncryptAzureKeyVersionSave)
		}
	}
	return NewAEAD(uri, c.creds, c.algorithm, saveVersion)
}
