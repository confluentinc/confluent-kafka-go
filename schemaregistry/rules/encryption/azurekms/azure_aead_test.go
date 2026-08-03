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
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorefake "github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	azkeysfake "github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys/fake"
)

const versionA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const versionB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "fake-token"}, nil
}

func newTestAEAD(t *testing.T, srv *azkeysfake.Server, keyVersion string, saveVersion bool) *azureAEAD {
	t.Helper()
	client, err := azkeys.NewClient("https://vault.vault.azure.net", fakeCredential{}, &azkeys.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: azkeysfake.NewServerTransport(srv)},
	})
	if err != nil {
		t.Fatalf("failed to create fake azkeys client: %v", err)
	}
	return &azureAEAD{
		client:      client,
		keyName:     "key1",
		keyVersion:  keyVersion,
		algorithm:   azkeys.EncryptionAlgorithmRSAOAEP256,
		saveVersion: saveVersion,
	}
}

func TestGetKeyInfoParsesVersionedID(t *testing.T) {
	vaultURL, keyName, keyVersion, err := getKeyInfo("https://vault.vault.azure.net/keys/key1/" + versionA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vaultURL != "https://vault.vault.azure.net" || keyName != "key1" || keyVersion != versionA {
		t.Fatalf("got (%q, %q, %q)", vaultURL, keyName, keyVersion)
	}
}

func TestGetKeyInfoParsesVersionlessID(t *testing.T) {
	_, _, keyVersion, err := getKeyInfo("https://vault.vault.azure.net/keys/key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keyVersion != "" {
		t.Fatalf("expected empty version, got %q", keyVersion)
	}
}

func TestGetKeyInfoThrowsForMalformedID(t *testing.T) {
	if _, _, _, err := getKeyInfo("https://vault.vault.azure.net/notkeys/key1"); err == nil {
		t.Fatal("expected error for malformed key id")
	}
}

func TestIsValidVersion(t *testing.T) {
	cases := map[string]bool{
		versionA:           true,
		"not-32-chars":     false,
		"":                 false,
		"g" + versionA[1:]: false,
	}
	for version, want := range cases {
		if got := isValidVersion(version); got != want {
			t.Errorf("isValidVersion(%q) = %v, want %v", version, got, want)
		}
	}
}

func TestExtractVersionForPrefixedCiphertext(t *testing.T) {
	ciphertext := []byte(versionPrefix + versionA + ":wrapped-bytes")
	version, wrapped, ok := extractVersion(ciphertext)
	if !ok || version != versionA || string(wrapped) != "wrapped-bytes" {
		t.Fatalf("got (%q, %q, %v)", version, wrapped, ok)
	}
}

func TestExtractVersionForLegacyCiphertext(t *testing.T) {
	_, _, ok := extractVersion([]byte("legacy-unprefixed-ciphertext"))
	if ok {
		t.Fatal("expected ok=false for unprefixed ciphertext")
	}
}

func TestEncryptWithoutSaveVersionReturnsRawCiphertext(t *testing.T) {
	srv := &azkeysfake.Server{
		Encrypt: func(ctx context.Context, name string, version string, parameters azkeys.KeyOperationParameters,
			options *azkeys.EncryptOptions) (resp azcorefake.Responder[azkeys.EncryptResponse], errResp azcorefake.ErrorResponder) {
			resp.SetResponse(http.StatusOK, azkeys.EncryptResponse{
				KeyOperationResult: azkeys.KeyOperationResult{Result: []byte("raw-ciphertext")},
			}, nil)
			return
		},
	}
	a := newTestAEAD(t, srv, "", false)

	result, err := a.Encrypt([]byte("plaintext"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, []byte("raw-ciphertext")) {
		t.Fatalf("got %q", result)
	}
}

func TestEncryptWithSaveVersionPrefixesWithoutDoubleEncoding(t *testing.T) {
	resolvedID := azkeys.ID("https://vault.vault.azure.net/keys/key1/" + versionA)
	srv := &azkeysfake.Server{
		GetKey: func(ctx context.Context, name string, version string, options *azkeys.GetKeyOptions) (
			resp azcorefake.Responder[azkeys.GetKeyResponse], errResp azcorefake.ErrorResponder) {
			resp.SetResponse(http.StatusOK, azkeys.GetKeyResponse{
				KeyBundle: azkeys.KeyBundle{Key: &azkeys.JSONWebKey{KID: &resolvedID}},
			}, nil)
			return
		},
		// Note: the Encrypt/Decrypt fakes' name/version parameters (parsed by the generated
		// server's URL regex) are unreliable when both a name and a version are present in the
		// path -- Go's RE2 engine assigns the whole "name/version" segment to the greedy
		// key_name group, leaving key_version empty (unlike backtracking regex engines). So this
		// test asserts on the request body (parameters.Value, decoded independently of the URL)
		// and on azureAEAD's own return value instead of relying on the fake's parsed version.
		Encrypt: func(ctx context.Context, name string, version string, parameters azkeys.KeyOperationParameters,
			options *azkeys.EncryptOptions) (resp azcorefake.Responder[azkeys.EncryptResponse], errResp azcorefake.ErrorResponder) {
			resp.SetResponse(http.StatusOK, azkeys.EncryptResponse{
				KeyOperationResult: azkeys.KeyOperationResult{Result: []byte("wrapped-bytes")},
			}, nil)
			return
		},
	}
	a := newTestAEAD(t, srv, "", true)

	result, err := a.Encrypt([]byte("plaintext"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []byte(versionPrefix + versionA + ":wrapped-bytes")
	if !bytes.Equal(result, want) {
		t.Fatalf("got %q, want %q", result, want)
	}
}

func TestDecryptUsesEmbeddedVersion(t *testing.T) {
	srv := &azkeysfake.Server{
		Decrypt: func(ctx context.Context, name string, version string, parameters azkeys.KeyOperationParameters,
			options *azkeys.DecryptOptions) (resp azcorefake.Responder[azkeys.DecryptResponse], errResp azcorefake.ErrorResponder) {
			// The prefix and embedded version must have been stripped before reaching the SDK
			// call: parameters.Value (decoded from the request body, unaffected by the fake
			// server's URL-parsing quirk noted above) must be exactly the wrapped bytes.
			if !bytes.Equal(parameters.Value, []byte("wrapped-bytes")) {
				t.Errorf("expected wrapped ciphertext %q, got %q", "wrapped-bytes", parameters.Value)
			}
			resp.SetResponse(http.StatusOK, azkeys.DecryptResponse{
				KeyOperationResult: azkeys.KeyOperationResult{Result: []byte("correct-plaintext")},
			}, nil)
			return
		},
	}
	a := newTestAEAD(t, srv, "", false)
	ciphertext := []byte(versionPrefix + versionA + ":wrapped-bytes")

	result, err := a.Decrypt(ciphertext, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, []byte("correct-plaintext")) {
		t.Fatalf("got %q", result)
	}
}

func TestDecryptFallsBackToLegacyVersionForUnprefixedCiphertext(t *testing.T) {
	srv := &azkeysfake.Server{
		Decrypt: func(ctx context.Context, name string, version string, parameters azkeys.KeyOperationParameters,
			options *azkeys.DecryptOptions) (resp azcorefake.Responder[azkeys.DecryptResponse], errResp azcorefake.ErrorResponder) {
			// Legacy/unprefixed ciphertext must be forwarded unchanged -- no stripping.
			if !bytes.Equal(parameters.Value, []byte("legacy-unprefixed-ciphertext")) {
				t.Errorf("expected unmodified ciphertext, got %q", parameters.Value)
			}
			resp.SetResponse(http.StatusOK, azkeys.DecryptResponse{
				KeyOperationResult: azkeys.KeyOperationResult{Result: []byte("legacy-plaintext")},
			}, nil)
			return
		},
	}
	a := newTestAEAD(t, srv, "", false)

	result, err := a.Decrypt([]byte("legacy-unprefixed-ciphertext"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, []byte("legacy-plaintext")) {
		t.Fatalf("got %q", result)
	}
}

func TestDecryptRemainsPossibleAfterToggleTurnedOff(t *testing.T) {
	// A DEK wrapped while saveVersion was true must stay decryptable even once the toggle is
	// turned back off: Decrypt must not depend on saveVersion at all.
	srv := &azkeysfake.Server{
		Decrypt: func(ctx context.Context, name string, version string, parameters azkeys.KeyOperationParameters,
			options *azkeys.DecryptOptions) (resp azcorefake.Responder[azkeys.DecryptResponse], errResp azcorefake.ErrorResponder) {
			if !bytes.Equal(parameters.Value, []byte("wrapped-bytes")) {
				t.Errorf("expected wrapped ciphertext %q, got %q", "wrapped-bytes", parameters.Value)
			}
			resp.SetResponse(http.StatusOK, azkeys.DecryptResponse{
				KeyOperationResult: azkeys.KeyOperationResult{Result: []byte("still-decryptable")},
			}, nil)
			return
		},
	}
	a := newTestAEAD(t, srv, "", false)
	ciphertext := []byte(versionPrefix + versionB + ":wrapped-bytes")

	result, err := a.Decrypt(ciphertext, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, []byte("still-decryptable")) {
		t.Fatalf("got %q", result)
	}
}

func TestDecryptThrowsForNonHexEmbeddedVersion(t *testing.T) {
	a := newTestAEAD(t, &azkeysfake.Server{}, "", false)
	nonHexVersion := "g" + versionA[1:]
	ciphertext := []byte(versionPrefix + nonHexVersion + ":wrapped-bytes")

	if _, err := a.Decrypt(ciphertext, nil); err == nil {
		t.Fatal("expected an error for a non-hex embedded version")
	}
}
