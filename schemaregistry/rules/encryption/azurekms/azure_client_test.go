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
	"log"
	"strings"
	"testing"
)

const versionlessKeyURI = "azure-kms://https://vault.vault.azure.net/keys/key1"
const versionedKeyURI = "azure-kms://https://vault.vault.azure.net/keys/key1/" + versionA

func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)
	fn()
	return buf.String()
}

func TestGetAEADLogsWarningForVersionlessKeyWithoutToggle(t *testing.T) {
	client, err := NewClient(versionlessKeyURI, fakeCredential{}, defaultEncryptionAlgorithm, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := captureLog(t, func() {
		if _, err := client.GetAEAD(versionlessKeyURI); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(output, "versionless") {
		t.Fatalf("expected a versionless warning, got log output: %q", output)
	}
}

func TestGetAEADNoWarningWhenToggleEnabled(t *testing.T) {
	config := map[string]string{EncryptAzureKeyVersionSave: "true"}
	client, err := NewClient(versionlessKeyURI, fakeCredential{}, defaultEncryptionAlgorithm, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := captureLog(t, func() {
		if _, err := client.GetAEAD(versionlessKeyURI); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(output, "versionless") {
		t.Fatalf("expected no versionless warning, got log output: %q", output)
	}
}

func TestGetAEADNoWarningForVersionedKey(t *testing.T) {
	client, err := NewClient(versionedKeyURI, fakeCredential{}, defaultEncryptionAlgorithm, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := captureLog(t, func() {
		if _, err := client.GetAEAD(versionedKeyURI); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(output, "versionless") {
		t.Fatalf("expected no versionless warning for a versioned key, got log output: %q", output)
	}
}
