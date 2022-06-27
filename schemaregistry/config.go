package schemaregistry

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

// Config is used to pass multiple configuration options to the Schema Registry client.
type Config struct {
	// SchemaRegistryURL determines the URL of Schema Registry
	SchemaRegistryURL string

	// BasicAuth keys
	BasicAuthUserInfo          string
	BasicAuthCredentialsSource string

	// Sasl keys
	SaslMechanism string
	SaslUsername  string
	SaslPassword  string

	// Ssl Keys
	SslCertificationLocation       string
	SslKeyLocation                 string
	SslCaLocation                  string
	SslDisableEndpointVerification bool

	// Timeouts
	ConnectionTimeoutMs int
	RequestTimeoutMs    int
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig(url string) *Config {
	c := &Config{}

	c.SchemaRegistryURL = url

	c.BasicAuthUserInfo = ""
	c.BasicAuthCredentialsSource = "URL"

	c.SaslMechanism = "GSSAPI"
	c.SaslUsername = ""
	c.SaslPassword = ""

	c.SslCertificationLocation = ""
	c.SslKeyLocation = ""
	c.SslCaLocation = ""
	c.SslDisableEndpointVerification = false

	c.ConnectionTimeoutMs = 10000
	c.RequestTimeoutMs = 10000

	return c
}
