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
	"net/http"
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

func TestNewAuthHeader(t *testing.T) {
	url, err := url.Parse("mock://")
	if err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "STATIC_TOKEN"
	config.BasicAuthCredentialsSource = "URL"

	_, err = NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work with both basic auth source and bearer auth source")
	}

	// testing bearer auth
	config.BasicAuthCredentialsSource = ""
	_, err = NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth token is empty")
	}

	config.BearerAuthToken = "token"
	config.BearerAuthLogicalCluster = "lsrc-123"
	config.BearerAuthIdentityPoolID = "poolID"
	headers, err := NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with bearer auth token, got %s", err)
	} else {
		if val, exists := headers["Authorization"]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "Bearer token") {
			t.Errorf("Should have header with key Authorization")
		}
		if val, exists := headers[TargetIdentityPoolIDKey]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "poolID") {
			t.Errorf("Should have header with key Confluent-Identity-Pool-Id")
		}
		if val, exists := headers[TargetSRClusterKey]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "lsrc-123") {
			t.Errorf("Should have header with key Target-Sr-Cluster")
		}
	}

	config.BearerAuthCredentialsSource = "other"
	_, err = NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth source is invalid")
	}

	// testing basic auth
	config.BearerAuthCredentialsSource = ""
	config.BasicAuthCredentialsSource = "USER_INFO"
	config.BasicAuthUserInfo = "username:password"
	_, err = NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	}

	config.BasicAuthCredentialsSource = "URL"
	_, err = NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if val, exists := headers["Authorization"]; !exists || len(val) == 0 {
		t.Errorf("Should have header with key Authorization")
	}

	config.BasicAuthCredentialsSource = "SASL_INHERIT"
	config.SaslUsername = "username"
	config.SaslPassword = "password"
	_, err = NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if val, exists := headers["Authorization"]; !exists || len(val) == 0 {
		t.Errorf("Should have header with key Authorization")
	}

	config.BasicAuthCredentialsSource = "other"
	_, err = NewAuthHeader(url, config)
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
		t.Errorf("should have error about bearer.auth.client.id")
	}

	config.BearerAuthClientID = "client_id"
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.client.secret") {
		t.Errorf("should have error about bearer.auth.client.secret")
	}

	config.BearerAuthClientSecret = "client_secret"
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.scopes") {
		t.Errorf("should have error about bearer.auth.scopes")
	}

	config.BearerAuthScopes = []string{"scope1", "scope2"}
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.logical.cluster") {
		t.Errorf("should have error about bearer.auth.logical.cluster")
	}

	config.BearerAuthLogicalCluster = "lsrc-123"
	_, err = NewRestService(config)

	if !strings.Contains(err.Error(), "bearer.auth.identity.pool.id") {
		t.Errorf("should have error about bearer.auth.identity.pool.id")
	}

	config.BearerAuthIdentityPoolID = "pool_id"
	_, err = NewRestService(config)

	if err != nil {
		t.Errorf("should work with bearer auth config, got %s", err)
	}
}

type CustomHeaderProvider struct {
	token                        string
	schemaRegistryLogicalCluster string
	identityPoolID               string
}

func (p *CustomHeaderProvider) SetAuthenticationHeaders(header *http.Header) error {
	header.Set("Authorization", "Bearer "+p.token)
	header.Set("Target-Sr-Cluster", p.schemaRegistryLogicalCluster)
	header.Set("Confluent-Identity-Pool-Id", p.identityPoolID)
	return nil
}

func TestCustomOAuthProvider(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "OAUTHBEARER"
	config.AuthenticationHeaderProvider = &CustomHeaderProvider{
		token:                        "token",
		schemaRegistryLogicalCluster: "lsrc-123",
		identityPoolID:               "pool_id",
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
