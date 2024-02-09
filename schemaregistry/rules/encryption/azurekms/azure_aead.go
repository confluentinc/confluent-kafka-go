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
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/tink-crypto/tink-go/v2/tink"
	"net/url"
	"strings"
)

// azureAEAD represents an Azure AEAD
type azureAEAD struct {
	client     *azkeys.Client
	keyName    string
	keyVersion string
	algorithm  azkeys.EncryptionAlgorithm
}

var _ tink.AEAD = (*azureAEAD)(nil)

// NewAEAD returns a new remote AEAD primitive for Azure Vault
func NewAEAD(keyID string, creds azcore.TokenCredential, algorithm azkeys.EncryptionAlgorithm) (tink.AEAD, error) {
	vaultURL, keyName, keyVersion, err := getKeyInfo(keyID)
	if err != nil {
		return nil, err
	}
	client, err := azkeys.NewClient(vaultURL, creds, nil)
	if err != nil {
		return nil, err
	}
	a := &azureAEAD{
		client:     client,
		keyName:    keyName,
		keyVersion: keyVersion,
		algorithm:  algorithm,
	}
	return a, nil
}

// Encrypt encrypts the plaintext data using a key stored in HashiCorp Vault.
func (a *azureAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	encryptParams := azkeys.KeyOperationParameters{
		Algorithm: &a.algorithm,
		Value:     plaintext,
	}
	encryptResponse, err := a.client.Encrypt(context.Background(), a.keyName, a.keyVersion, encryptParams, nil)
	if err != nil {
		return nil, err
	}
	return encryptResponse.Result, nil
}

// Decrypt decrypts the ciphertext using a key stored in HashiCorp Vault.
func (a *azureAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	decryptParams := azkeys.KeyOperationParameters{
		Algorithm: &a.algorithm,
		Value:     ciphertext,
	}
	decryptResponse, err := a.client.Decrypt(context.Background(), a.keyName, a.keyVersion, decryptParams, nil)
	if err != nil {
		return nil, err
	}
	return decryptResponse.Result, nil
}

// getKeyInfo transforms keyID into the Azure key name and key version.
// The keyPath is expected to have the form "https://{vaultURL}/keys/{keyName}/{keyVersion}"
func getKeyInfo(keyID string) (vaultURL, keyName, keyVersion string, err error) {
	u, err := url.Parse(keyID)
	path := u.Path
	parts := strings.Split(path, "/")
	length := len(parts)
	if length != 4 || parts[0] != "" || parts[1] != "keys" {
		return "", "", "", errors.New("malformed keyPath")
	}
	return u.Scheme + "://" + u.Host, parts[2], parts[3], nil
}
