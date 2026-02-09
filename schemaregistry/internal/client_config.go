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
	// SchemaRegistryURL is a comma-space separated list of URLs for the Schema Registry.
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
	// BearerAuthIdentityPoolID specifies the identity pool ID. Optional - if omitted, the SR server will rely on
	// SDS auto pool mapping. Can be a single pool ID or comma-separated list for union-of-pools (e.g., "pool-a,pool-b").
	BearerAuthIdentityPoolID string
	// BearerAuthIssuerEndpointURL specifies the issuer endpoint URL for OAuth Bearer Token authentication.
	BearerAuthIssuerEndpointURL string
	// BearerAuthClientID specifies the client ID for OAuth Bearer Token authentication.
	BearerAuthClientID string
	// BearerAuthClientSecret specifies the client secret for OAuth Bearer Token authentication.
	BearerAuthClientSecret string
	// BearerAuthScopes specifies the scopes for OAuth Bearer Token authentication.
	BearerAuthScopes []string
	// AuthenticationHeaderProvider specifies a custom authentication header provider.
	AuthenticationHeaderProvider AuthenticationHeaderProvider

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

	// MaxRetries specifices the maximum number of retries for a request
	MaxRetries int
	// RetriesWaitMs specifies the maximum time to wait for the first retry.
	RetriesWaitMs int
	// RetriesMaxWaitMs specifies the maximum time to wait any retry.
	RetriesMaxWaitMs int

	// HTTP client
	HTTPClient *http.Client
}

// stringSlicesEqual compares two string slices for equality
func stringSlicesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ConfigsEqual compares two configurations for approximate equality
func ConfigsEqual(c1 *ClientConfig, c2 *ClientConfig) bool {
	return c1.SchemaRegistryURL == c2.SchemaRegistryURL &&
		c1.BasicAuthUserInfo == c2.BasicAuthUserInfo &&
		c1.BasicAuthCredentialsSource == c2.BasicAuthCredentialsSource &&
		c1.SaslMechanism == c2.SaslMechanism &&
		c1.SaslUsername == c2.SaslUsername &&
		c1.SaslPassword == c2.SaslPassword &&
		c1.BearerAuthToken == c2.BearerAuthToken &&
		c1.BearerAuthCredentialsSource == c2.BearerAuthCredentialsSource &&
		c1.BearerAuthLogicalCluster == c2.BearerAuthLogicalCluster &&
		c1.BearerAuthIdentityPoolID == c2.BearerAuthIdentityPoolID &&
		c1.BearerAuthIssuerEndpointURL == c2.BearerAuthIssuerEndpointURL &&
		c1.BearerAuthClientID == c2.BearerAuthClientID &&
		c1.BearerAuthClientSecret == c2.BearerAuthClientSecret &&
		stringSlicesEqual(c1.BearerAuthScopes, c2.BearerAuthScopes) &&
		c1.AuthenticationHeaderProvider == c2.AuthenticationHeaderProvider &&
		c1.SslCertificateLocation == c2.SslCertificateLocation &&
		c1.SslKeyLocation == c2.SslKeyLocation &&
		c1.SslCaLocation == c2.SslCaLocation &&
		c1.SslDisableEndpointVerification == c2.SslDisableEndpointVerification
}
