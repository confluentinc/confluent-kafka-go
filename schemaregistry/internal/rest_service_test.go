/**
 * Copyright 2022 Confluent Inc.
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

package internal

import (
	"crypto/tls"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestConfigureTLS tests the configureTLS function called while creating a new
// REST client.
func TestConfigureTLS(t *testing.T) {
	tlsConfig := &tls.Config{}
	config := &ClientConfig{}

	// Empty config.
	if err := ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	// Valid CA.
	config.SslCaLocation = "../test/secrets/rootCA.crt"
	if err := ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, got %s", err)
	}

	// Invalid CA.
	config.SslCaLocation = "../test/secrets/rootCA.crt.malformed"
	if err := ConfigureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(), "could not parse certificate from") {
		t.Errorf(
			"Should not work with invalid CA with the give appropriate error, got err = %s",
			err)
	}

	config.SslCaLocation = ""

	// Valid certificate and key.
	config.SslCertificateLocation = "../test/secrets/rootCA.crt"
	config.SslKeyLocation = "../test/secrets/rootCA.key"
	if err := ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid certificate and key, got %s", err)
	}

	// Valid certificate and non-existent key.
	config.SslCertificateLocation = "../test/secrets/rootCA.crt"
	config.SslKeyLocation = ""
	if err := ConfigureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(),
			"SslKeyLocation needs to be provided if using SslCertificateLocation") {
		t.Errorf(
			"Should not work with non-existent keys and give appropriate error, got err = %s",
			err)
	}

	// Invalid certificate.
	config.SslCertificateLocation = "../test/secrets/rootCA.crt.malformed"
	config.SslKeyLocation = "../test/secrets/rootCA.key"
	if err := ConfigureTLS(config, tlsConfig); err == nil {
		t.Error("Should not work with invalid certificate")
	}

	// All three of CA, certificate and key valid.
	config.SslCertificateLocation = "../test/secrets/rootCA.crt"
	config.SslKeyLocation = "../test/secrets/rootCA.key"
	config.SslCaLocation = "../test/secrets/rootCA.crt"
	if err := ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, certificate and key, got %s", err)
	}
}

func TestNewAuthenticationHeaderProvider(t *testing.T) {
	url, err := url.Parse("mock://")
	if err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "STATIC_TOKEN"
	config.BasicAuthCredentialsSource = "URL"

	var provider AuthenticationHeaderProvider

	_, err = NewAuthenticationHeaderProvider(url, config)
	if err == nil {
		t.Errorf("Should not work with both basic auth source and bearer auth source")
	}

	// testing bearer auth
	config.BasicAuthCredentialsSource = ""
	_, err = NewAuthenticationHeaderProvider(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth token is empty")
	}

	config.BearerAuthToken = "token"
	config.BearerAuthLogicalCluster = "lsrc-123"
	config.BearerAuthIdentityPoolID = "poolID"
	provider, err = NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with bearer auth token, got %s", err)
	} else {
		authField, _ := provider.GetAuthenticationHeader()
		if authField != "Bearer "+config.BearerAuthToken {
			t.Errorf("Should have header with key Authorization")
		}
		providerIdentityPoolID, _ := provider.GetIdentityPoolID()
		if providerIdentityPoolID != config.BearerAuthIdentityPoolID {
			t.Errorf("Should have identity pool id %s", config.BearerAuthIdentityPoolID)
		}
		providerLogicalCluster, _ := provider.GetLogicalCluster()
		if providerLogicalCluster != config.BearerAuthLogicalCluster {
			t.Errorf("Should have logical cluster %s", config.BearerAuthLogicalCluster)
		}
	}

	config.BearerAuthCredentialsSource = "other"
	_, err = NewAuthenticationHeaderProvider(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth source is invalid")
	}

	// testing basic auth
	config.BearerAuthCredentialsSource = ""
	config.BasicAuthCredentialsSource = "USER_INFO"
	config.BasicAuthUserInfo = "username:password"
	provider, err = NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if authField, _ := provider.GetAuthenticationHeader(); authField != "Basic "+encodeBasicAuth(config.BasicAuthUserInfo) {
		t.Errorf("Should return encoded basic auth token")
	} else if providerIdentityPoolID, _ := provider.GetIdentityPoolID(); providerIdentityPoolID != "" {
		t.Errorf("Should not have identity pool id %s", providerIdentityPoolID)
	} else if providerLogicalCluster, _ := provider.GetLogicalCluster(); providerLogicalCluster != "" {
		t.Errorf("Should not have logical cluster %s", providerLogicalCluster)
	}

	config.BasicAuthCredentialsSource = "URL"
	_, err = NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	}

	config.BasicAuthCredentialsSource = "SASL_INHERIT"
	config.SaslUsername = "username"
	config.SaslPassword = "password"
	_, err = NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if authField, _ := provider.GetAuthenticationHeader(); authField != "Basic "+encodeBasicAuth(config.BasicAuthUserInfo) {
		t.Errorf("Should return encoded basic auth token")
	} else if providerIdentityPoolID, _ := provider.GetIdentityPoolID(); providerIdentityPoolID != "" {
		t.Errorf("Should not have identity pool id %s", providerIdentityPoolID)
	} else if providerLogicalCluster, _ := provider.GetLogicalCluster(); providerLogicalCluster != "" {
		t.Errorf("Should not have logical cluster %s", providerLogicalCluster)
	}

	config.BasicAuthCredentialsSource = "other"
	_, err = NewAuthenticationHeaderProvider(url, config)
	if err == nil {
		t.Errorf("Should not work if basic auth source is invalid")
	}
}

func TestFullJitter(t *testing.T) {
	config := &ClientConfig{}

	config.MaxRetries = 2
	config.RetriesWaitMs = 1000
	config.RetriesMaxWaitMs = 20000

	rs, _ := NewRestService(config)
	for i := 0; i < 10; i++ {
		v := fullJitter(i, rs.ceilingRetries, rs.retriesMaxWaitMs, rs.retriesWaitMs)
		var d time.Duration
		if i < 5 {
			d = time.Duration(
				math.Pow(2, float64(i))*float64(config.RetriesWaitMs)) * time.Millisecond
		} else {
			d = time.Duration(config.RetriesMaxWaitMs) * time.Millisecond
		}
		if v < 0 || v > d {
			t.Errorf("Value %d should be between 0 and %d ms", v, d)
		}
	}
}

func TestOAuthBearerAuthConfig(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "OAUTHBEARER"

	_, err := NewRestService(config)
	if !strings.Contains(err.Error(), "bearer.auth.issuer.endpoint.url") {
		t.Errorf("should have error about bearer.auth.issuer.endpoint.url")
	}

	config.BearerAuthIssuerEndpointURL = "https://example.com/oauth/token"
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.client.id") {
		t.Errorf("should have error about bearer.auth.client.id, got %s", err)
	}

	config.BearerAuthClientID = "client_id"
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.client.secret") {
		t.Errorf("should have error about bearer.auth.client.secret")
	}

	config.BearerAuthClientSecret = "client_secret"
	_, err = NewRestService(config)
	if !strings.Contains(err.Error(), "bearer.auth.logical.cluster") {
		t.Errorf("should have error about bearer.auth.logical.cluster, got %s", err)
	}

	config.BearerAuthLogicalCluster = "lsrc-123"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with oauth bearer auth config without identity pool ID (auto mapping), got %s", err)
	}

	// Should also work with identity pool ID specified
	config.BearerAuthIdentityPoolID = "pool_id"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with oauth bearer auth config with identity pool ID, got %s", err)
	}
}

type CustomHeaderProvider struct {
	token                        string
	schemaRegistryLogicalCluster string
	identityPoolID               string
}

func (p *CustomHeaderProvider) GetAuthenticationHeader() (string, error) {
	return "Bearer " + p.token, nil
}

func (p *CustomHeaderProvider) GetIdentityPoolID() (string, error) {
	return p.identityPoolID, nil
}
func (p *CustomHeaderProvider) GetLogicalCluster() (string, error) {
	return p.schemaRegistryLogicalCluster, nil
}

func TestCustomOAuthProvider(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "OAUTHBEARER"
	config.AuthenticationHeaderProvider = &CustomHeaderProvider{
		token:                        testToken,
		schemaRegistryLogicalCluster: testLogicalCluster,
		identityPoolID:               testIdentityPoolID,
	}

	_, err := NewRestService(config)
	if !strings.Contains(err.Error(), "cannot have bearer.auth.credentials.source oauthbearer") {
		t.Errorf("should have error with custom oauth provider and OAUTHBEARER")
	}

	config.BearerAuthCredentialsSource = "CUSTOM"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with custom oauth provider and CUSTOM")
	}
}

func TestSetAuthenticationHandlers(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "OAUTHBEARER"
	config.AuthenticationHeaderProvider = &CustomHeaderProvider{
		token:                        testToken,
		schemaRegistryLogicalCluster: testLogicalCluster,
		identityPoolID:               testIdentityPoolID,
	}
	config.BearerAuthCredentialsSource = "CUSTOM"
	rs, err := NewRestService(config)

	if err != nil {
		t.Errorf("should work with custom oauth provider and CUSTOM")
	}

	SetAuthenticationHeaders(config.AuthenticationHeaderProvider, &rs.headers)

	if rs.headers.Get("Authorization") != "Bearer "+testToken {
		t.Errorf("should have Authorization header with value Bearer token")
	}
	if rs.headers.Get("Target-Sr-Cluster") != testLogicalCluster {
		t.Errorf("should have Target-Sr-Cluster header with value lsrc-123")
	}
	if rs.headers.Get("Confluent-Identity-Pool-Id") != testIdentityPoolID {
		t.Errorf("should have Confluent-Identity-Pool-Id header with value pool_id")
	}
}

func TestNoAuthProviderSetAuthenticationHeaders(t *testing.T) {
	config := &ClientConfig{}

	rs, err := NewRestService(config)

	if err != nil {
		t.Errorf("should work with no auth provider")
	}

	err = SetAuthenticationHeaders(config.AuthenticationHeaderProvider, &rs.headers)

	if err != nil {
		t.Errorf("should work with no auth provider, got err %s", err)
	}
}

func TestStaticTokenAuthWithOptionalIdentityPoolID(t *testing.T) {
	url, err := url.Parse("mock://")
	if err != nil {
		t.Errorf("Failed to parse URL, got %s", err)
	}

	// Test without identity pool ID (auto mapping)
	config := &ClientConfig{
		BearerAuthCredentialsSource: "STATIC_TOKEN",
		BearerAuthToken:             "test-token",
		BearerAuthLogicalCluster:    "lsrc-123",
		BearerAuthIdentityPoolID:    "", // Empty for auto mapping
	}

	provider, err := NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with static token without identity pool ID (auto mapping), got %s", err)
	}

	authHeader, _ := provider.GetAuthenticationHeader()
	if authHeader != "Bearer test-token" {
		t.Errorf("Expected Bearer test-token, got %s", authHeader)
	}

	identityPoolID, _ := provider.GetIdentityPoolID()
	if identityPoolID != "" {
		t.Errorf("Expected empty identity pool ID, got %s", identityPoolID)
	}

	logicalCluster, _ := provider.GetLogicalCluster()
	if logicalCluster != "lsrc-123" {
		t.Errorf("Expected lsrc-123, got %s", logicalCluster)
	}

	// Test with identity pool ID specified
	config.BearerAuthIdentityPoolID = "pool-1,pool-2"
	provider, err = NewAuthenticationHeaderProvider(url, config)
	if err != nil {
		t.Errorf("Should work with static token with identity pool ID, got %s", err)
	}

	identityPoolID, _ = provider.GetIdentityPoolID()
	if identityPoolID != "pool-1,pool-2" {
		t.Errorf("Expected pool-1,pool-2, got %s", identityPoolID)
	}
}

func TestIdentityPoolIDsToString(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "Single pool ID",
			input:    []string{"pool-1"},
			expected: "pool-1",
		},
		{
			name:     "Multiple pool IDs",
			input:    []string{"pool-1", "pool-2", "pool-3"},
			expected: "pool-1,pool-2,pool-3",
		},
		{
			name:     "Empty list",
			input:    []string{},
			expected: "",
		},
		{
			name:     "Two pool IDs",
			input:    []string{"pool-a", "pool-b"},
			expected: "pool-a,pool-b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IdentityPoolIDsToString(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSetAuthenticationHeadersWithoutIdentityPoolID(t *testing.T) {
	config := &ClientConfig{
		BearerAuthCredentialsSource: "STATIC_TOKEN",
		BearerAuthToken:             "test-token",
		BearerAuthLogicalCluster:    "lsrc-123",
		BearerAuthIdentityPoolID:    "", // Empty for auto mapping
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Errorf("Should work with static token without identity pool ID, got %s", err)
	}

	err = SetAuthenticationHeaders(rs.authenticationHeaderProvider, &rs.headers)
	if err != nil {
		t.Errorf("Should set headers without identity pool ID, got %s", err)
	}

	if rs.headers.Get("Authorization") != "Bearer test-token" {
		t.Errorf("Should have Authorization header with value Bearer test-token")
	}

	if rs.headers.Get("Target-Sr-Cluster") != "lsrc-123" {
		t.Errorf("Should have Target-Sr-Cluster header with value lsrc-123")
	}

	// Identity pool ID header should not be set when empty
	if rs.headers.Get("Confluent-Identity-Pool-Id") != "" {
		t.Errorf("Should not have Confluent-Identity-Pool-Id header when not configured, got %s",
			rs.headers.Get("Confluent-Identity-Pool-Id"))
	}
}
