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
	"net/http"
)

// ClientConfig is used to pass multiple configuration options to the Schema Registry client.
type ClientConfig struct {
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

	// BearerAuthToken specifies the token for authentication.
	BearerAuthToken string
	// BearerAuthCredentialsSource specifies how to determine the credentials.
	BearerAuthCredentialsSource string
	// BearerAuthLogicalCluster specifies the target SR logical cluster id. It is required for Confluent Cloud Schema Registry
	BearerAuthLogicalCluster string
	// BearerAuthIdentityPoolID specifies the identity pool ID. It is required for Confluent Cloud Schema Registry
	BearerAuthIdentityPoolID string

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
	// CacheLatestTTLSecs ttl in secs for caching the latest schema
	CacheLatestTTLSecs int

	// HTTP client
	HTTPClient *http.Client
}
