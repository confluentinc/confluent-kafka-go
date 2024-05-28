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
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
)

// Config is used to pass multiple configuration options to the Schema Registry client.
type Config struct {
	internal.ClientConfig
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig(url string) *Config {
	c := &Config{}

	c.SchemaRegistryURL = url

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
// This method is deprecated.
func NewConfigWithAuthentication(url string, username string, password string) *Config {
	c := NewConfig(url)

	c.BasicAuthUserInfo = fmt.Sprintf("%s:%s", username, password)
	c.BasicAuthCredentialsSource = "USER_INFO"

	return c
}

// NewConfigWithBasicAuthentication returns a new configuration instance using basic authentication.
// For Confluent Cloud, use the API key for the username and the API secret for the password.
func NewConfigWithBasicAuthentication(url string, username string, password string) *Config {
	c := NewConfig(url)

	c.BasicAuthUserInfo = fmt.Sprintf("%s:%s", username, password)
	c.BasicAuthCredentialsSource = "USER_INFO"

	return c
}

// NewConfigWithBearerAuthentication returns a new configuration instance using bearer authentication.
// For Confluent Cloud, targetSr(`bearer.auth.logical.cluster` and
// identityPoolID(`bearer.auth.identity.pool.id`) is required
func NewConfigWithBearerAuthentication(url, token, targetSr, identityPoolID string) *Config {

	c := NewConfig(url)

	c.BearerAuthToken = token
	c.BearerAuthCredentialsSource = "STATIC_TOKEN"
	c.BearerAuthLogicalCluster = targetSr
	c.BearerAuthIdentityPoolID = identityPoolID

	return c
}
