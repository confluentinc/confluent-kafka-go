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

import "fmt"

// Config is used to pass multiple configuration options to the Schema Registry client.
type Config struct {
	// SchemaRegistryURL determines the URL of Schema Registry.
	SchemaRegistryURL string

	// BasicAuthUserInfo specifies the user info in the form of {username}:{password}.
	BasicAuthUserInfo string
	// BasicAuthCredentialsSource specifies how to determine the credentials, one of URL, USER_INFO, and SASL_INHERIT.
	BasicAuthCredentialsSource string

	// SaslMechanism specifies the SASL mechanism used for client connections, which defaults to GSSAPI.
	SaslMechanism string
	// SaslUsername specifies the username for SASL.
	SaslUsername string
	// SaslUsername specifies the password for SASL.
	SaslPassword string

	// SslCertificateLocation specifies the location of SSL certificates.
	SslCertificateLocation string
	// SslKeyLocation specifies the location of SSL keys.
	SslKeyLocation string
	// SslCaLocation specifies the location of SSL certificate authorities.
	SslCaLocation string
	// SslDisableEndpointVerification determines whether to disable endpoint verification.
	SslDisableEndpointVerification bool

	// ConnectionTimeoutMs determines the connection timeout in milliseconds.
	ConnectionTimeoutMs int
	// RequestTimeoutMs determines the request timeout in milliseconds.
	RequestTimeoutMs int
	// CacheCapacity positive integer or zero for unbounded capacity
	CacheCapacity int
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

	c.SslCertificateLocation = ""
	c.SslKeyLocation = ""
	c.SslCaLocation = ""
	c.SslDisableEndpointVerification = false

	c.ConnectionTimeoutMs = 10000
	c.RequestTimeoutMs = 10000

	return c
}

// NewConfigWithAuthentication returns a new configuration instance using basic authentication.
// For Confluent Cloud, use the API key for the username and the API secret for the password.
func NewConfigWithAuthentication(url string, username string, password string) *Config {
	c := NewConfig(url)

	c.BasicAuthUserInfo = fmt.Sprintf("%s:%s", username, password)
	c.BasicAuthCredentialsSource = "USER_INFO"

	return c
}
