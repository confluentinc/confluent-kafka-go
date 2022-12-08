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
	"strings"
	"testing"
)

// TestConfigureTLS tests the configureTLS function called while creating a new
// REST client.
func TestConfigureTLS(t *testing.T) {
	tlsConfig := &tls.Config{}
	config := &Config{}

	// Empty config.
	if err := configureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with empty config, got %s", err)
	}

	// Valid CA.
	config.SslCaLocation = "test/secrets/rootCA.crt"
	if err := configureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, got %s", err)
	}

	// Invalid CA.
	config.SslCaLocation = "test/secrets/rootCA.crt.malformed"
	if err := configureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(), "could not parse certificate from") {
		t.Errorf(
			"Should not work with invalid CA with the give appropriate error, got err = %s",
			err)
	}

	config.SslCaLocation = ""

	// Valid certificate and key.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	if err := configureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid certificate and key, got %s", err)
	}

	// Valid certificate and non-existent key.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = ""
	if err := configureTLS(config, tlsConfig); err == nil ||
		!strings.HasPrefix(err.Error(),
			"SslKeyLocation needs to be provided if using SslCertificateLocation") {
		t.Errorf(
			"Should not work with non-existent keys and give appropriate error, got err = %s",
			err)
	}

	// Invalid certificate.
	config.SslCertificateLocation = "test/secrets/rootCA.crt.malformed"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	if err := configureTLS(config, tlsConfig); err == nil {
		t.Error("Should not work with invalid certificate")
	}

	// All three of CA, certificate and key valid.
	config.SslCertificateLocation = "test/secrets/rootCA.crt"
	config.SslKeyLocation = "test/secrets/rootCA.key"
	config.SslCaLocation = "test/secrets/rootCA.crt"
	if err := configureTLS(config, tlsConfig); err != nil {
		t.Errorf("Should work with valid CA, certificate and key, got %s", err)
	}
}
