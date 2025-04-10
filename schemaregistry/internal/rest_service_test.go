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
	for i := 0; i < 10; i++ {
		v := fullJitter(i, config.MaxRetries, config.RetriesMaxWaitMs, config.RetriesWaitMs)
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
