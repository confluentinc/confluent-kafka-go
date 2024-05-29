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

package schemaregistry

import (
	"crypto/tls"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
	"net/url"
	"strings"
	"testing"
)

// TestConfigureTLS tests the configureTLS function called while creating a new
// REST client.
func TestConfigureTLS(t *testing.T) {
	tlsConfig := &tls.Config{}
	config := &internal.ClientConfig{}

	// Empty config.
	if err := internal.ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	// Valid CA.
	config.SslCaLocation = "test/secrets/rootCA.crt"
	if err := internal.ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, got %s", err)
	}

	// Invalid CA.
	config.SslCaLocation = "test/secrets/rootCA.crt.malformed"
	if err := internal.ConfigureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(), "could not parse certificate from") {
		t.Errorf(
			"Should not work with invalid CA with the give appropriate error, got err = %s",
			err)
	}

	config.SslCaLocation = ""

	// Valid certificate and key.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	if err := internal.ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid certificate and key, got %s", err)
	}

	// Valid certificate and non-existent key.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = ""
	if err := internal.ConfigureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(),
			"SslKeyLocation needs to be provided if using SslCertificateLocation") {
		t.Errorf(
			"Should not work with non-existent keys and give appropriate error, got err = %s",
			err)
	}

	// Invalid certificate.
	config.SslCertificateLocation = "test/secrets/rootCA.crt.malformed"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	if err := internal.ConfigureTLS(config, tlsConfig); err == nil {
		t.Error("Should not work with invalid certificate")
	}

	// All three of CA, certificate and key valid.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	config.SslCaLocation = "test/secrets/rootCA.crt"
	if err := internal.ConfigureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, certificate and key, got %s", err)
	}
}

func TestNewAuthHeader(t *testing.T) {
	url, err := url.Parse("mock://")
	if err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	config := &internal.ClientConfig{}

	config.BearerAuthCredentialsSource = "STATIC_TOKEN"
	config.BasicAuthCredentialsSource = "URL"

	_, err = internal.NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work with both basic auth source and bearer auth source")
	}

	// testing bearer auth
	config.BasicAuthCredentialsSource = ""
	_, err = internal.NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth token is empty")
	}

	config.BearerAuthToken = "token"
	config.BearerAuthLogicalCluster = "lsrc-123"
	config.BearerAuthIdentityPoolID = "poolID"
	headers, err := internal.NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with bearer auth token, got %s", err)
	} else {
		if val, exists := headers["Authorization"]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "Bearer token") {
			t.Errorf("Should have header with key Authorization")
		}
		if val, exists := headers[internal.TargetIdentityPoolIDKey]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "poolID") {
			t.Errorf("Should have header with key Confluent-Identity-Pool-Id")
		}
		if val, exists := headers[internal.TargetSRClusterKey]; !exists || len(val) == 0 ||
			!strings.EqualFold(val[0], "lsrc-123") {
			t.Errorf("Should have header with key Target-Sr-Cluster")
		}
	}

	config.BearerAuthCredentialsSource = "other"
	_, err = internal.NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work if bearer auth source is invalid")
	}

	// testing basic auth
	config.BearerAuthCredentialsSource = ""
	config.BasicAuthCredentialsSource = "USER_INFO"
	config.BasicAuthUserInfo = "username:password"
	_, err = internal.NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	}

	config.BasicAuthCredentialsSource = "URL"
	_, err = internal.NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if val, exists := headers["Authorization"]; !exists || len(val) == 0 {
		t.Errorf("Should have header with key Authorization")
	}

	config.BasicAuthCredentialsSource = "SASL_INHERIT"
	config.SaslUsername = "username"
	config.SaslPassword = "password"
	_, err = internal.NewAuthHeader(url, config)
	if err != nil {
		t.Errorf("Should work with basic auth token, got %s", err)
	} else if val, exists := headers["Authorization"]; !exists || len(val) == 0 {
		t.Errorf("Should have header with key Authorization")
	}

	config.BasicAuthCredentialsSource = "other"
	_, err = internal.NewAuthHeader(url, config)
	if err == nil {
		t.Errorf("Should not work if basic auth source is invalid")
	}
}
