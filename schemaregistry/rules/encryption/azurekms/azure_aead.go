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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const versionPrefix = "azure:v1:"
const versionLength = 32

var versionPrefixBytes = []byte(versionPrefix)
var hexVersionRegex = regexp.MustCompile(`^[0-9a-fA-F]{32}$`)

// azureAEAD represents an Azure AEAD
type azureAEAD struct {
	client      *azkeys.Client
	keyName     string
	keyVersion  string
	algorithm   azkeys.EncryptionAlgorithm
	saveVersion bool
}

var _ tink.AEAD = (*azureAEAD)(nil)

// NewAEAD returns a new remote AEAD primitive for Azure Vault
func NewAEAD(keyID string, creds azcore.TokenCredential, algorithm azkeys.EncryptionAlgorithm, saveVersion bool) (tink.AEAD, error) {
	vaultURL, keyName, keyVersion, err := getKeyInfo(keyID)
	if err != nil {
		return nil, err
	}
	client, err := azkeys.NewClient(vaultURL, creds, nil)
	if err != nil {
		return nil, err
	}
	a := &azureAEAD{
		client:      client,
		keyName:     keyName,
		keyVersion:  keyVersion,
		algorithm:   algorithm,
		saveVersion: saveVersion,
	}
	return a, nil
}

// Encrypt encrypts the plaintext data using a key stored in Azure Key Vault.
//
// Unlike AWS KMS and GCP KMS, Azure Key Vault wrap/unwrap operations are scoped to an explicit key
// version and do not embed that version in the ciphertext. When saveVersion is enabled (see
// EncryptAzureKeyVersionSave), Encrypt makes its output self-describing by prepending the exact
// version that produced it: azure:v1: + 32-character key version + : + raw ciphertext bytes.
//
// Decrypt always checks for this prefix regardless of the current saveVersion value, since a DEK
// wrapped while the toggle was on must remain decryptable even after it is turned back off.
func (a *azureAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	if !a.saveVersion {
		return a.encryptWithVersion(plaintext, a.keyVersion)
	}
	keyResp, err := a.client.GetKey(context.Background(), a.keyName, "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve current Azure Key Vault key version for key '%s': %w",
			a.keyName, err)
	}
	if keyResp.Key == nil || keyResp.Key.KID == nil {
		return nil, fmt.Errorf("failed to resolve current Azure Key Vault key version for key '%s'", a.keyName)
	}
	version := keyResp.Key.KID.Version()
	if !isValidVersion(version) {
		// Mirrors Decrypt's own validation: a DEK this method wraps must always be one this same
		// type can later unwrap.
		return nil, fmt.Errorf("kms key version '%s' must be a %d-character hex string; cannot "+
			"be embedded in a fixed-width azure:v1: prefix", version, versionLength)
	}
	ciphertext, err := a.encryptWithVersion(plaintext, version)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString(versionPrefix)
	buf.WriteString(version)
	buf.WriteByte(':')
	buf.Write(ciphertext)
	return buf.Bytes(), nil
}

func (a *azureAEAD) encryptWithVersion(plaintext []byte, version string) ([]byte, error) {
	encryptParams := azkeys.KeyOperationParameters{
		Algorithm: &a.algorithm,
		Value:     plaintext,
	}
	encryptResponse, err := a.client.Encrypt(context.Background(), a.keyName, version, encryptParams, nil)
	if err != nil {
		return nil, err
	}
	return encryptResponse.Result, nil
}

// Decrypt decrypts the ciphertext using a key stored in Azure Key Vault.
func (a *azureAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	targetVersion := a.keyVersion
	if version, wrapped, ok := extractVersion(ciphertext); ok {
		if !isValidVersion(version) {
			// Encrypted key material is unauthenticated at this layer, so a corrupted or
			// tampered value could otherwise smuggle arbitrary characters (e.g. '/') into the
			// key name/version passed to the Azure SDK below.
			return nil, fmt.Errorf("ciphertext carries an invalid azure:v1: key version: '%s'", version)
		}
		targetVersion = version
		ciphertext = wrapped
	}
	decryptParams := azkeys.KeyOperationParameters{
		Algorithm: &a.algorithm,
		Value:     ciphertext,
	}
	decryptResponse, err := a.client.Decrypt(context.Background(), a.keyName, targetVersion, decryptParams, nil)
	if err != nil {
		return nil, err
	}
	return decryptResponse.Result, nil
}

func isValidVersion(version string) bool {
	return hexVersionRegex.MatchString(version)
}

// extractVersion returns the embedded version and the remaining ciphertext if ciphertext carries
// the azure:v1: prefix (see azureAEAD.Encrypt), and ok=true. Returns ok=false if it does not (e.g.
// a legacy DEK wrapped before EncryptAzureKeyVersionSave was enabled on its KEK, or the toggle is
// not set): the toggle can be flipped on/off over a KEK's lifetime, and old, un-prefixed
// ciphertext must remain decryptable.
func extractVersion(ciphertext []byte) (version string, wrapped []byte, ok bool) {
	headerLength := len(versionPrefixBytes) + versionLength + 1 // +1 for ':'
	if len(ciphertext) < headerLength ||
		!bytes.HasPrefix(ciphertext, versionPrefixBytes) ||
		ciphertext[headerLength-1] != ':' {
		return "", nil, false
	}
	version = string(ciphertext[len(versionPrefixBytes) : len(versionPrefixBytes)+versionLength])
	return version, ciphertext[headerLength:], true
}

// getKeyInfo transforms keyID into the Azure vault URL, key name and key version.
// The keyPath is expected to have the form "https://{vaultURL}/keys/{keyName}/{keyVersion}"
// where keyVersion is optional and if not provided, the latest version will be used.
func getKeyInfo(keyID string) (vaultURL, keyName, keyVersion string, err error) {
	u, err := url.Parse(keyID)
	if err != nil {
		return "", "", "", err
	}
	path := u.Path
	parts := strings.Split(path, "/")
	length := len(parts)
	if (length != 3 && length != 4) || parts[0] != "" || parts[1] != "keys" {
		return "", "", "", errors.New("malformed keyPath")
	}
	keyName = parts[2]
	if length == 4 {
		keyVersion = parts[3]
	}
	return u.Scheme + "://" + u.Host, keyName, keyVersion, nil
}
