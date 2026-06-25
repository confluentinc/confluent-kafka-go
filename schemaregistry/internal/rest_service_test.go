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
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// networkErrorTransport is an http.RoundTripper that returns a network-level
// error (as if the request failed before a response was received) for the
// first failCalls invocations, then returns a 200 response. It records the
// total number of attempts so tests can assert retry behavior.
type networkErrorTransport struct {
	mu        sync.Mutex
	calls     int
	failCalls int
}

func (t *networkErrorTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.calls++
	n := t.calls
	t.mu.Unlock()
	if n <= t.failCalls {
		// Mimic a dial/connection failure surfaced by http.Client.Do.
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"name":"test-subject"}`)),
		Header:     make(http.Header),
	}, nil
}

func (t *networkErrorTransport) attempts() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// TestHandleRequest_RetriesOnNetworkError verifies that network-level errors
// (no HTTP response received) are retried, matching the Java client's behavior
// of retrying on IOException.
func TestHandleRequest_RetriesOnNetworkError(t *testing.T) {
	transport := &networkErrorTransport{failCalls: 2}
	config := &ClientConfig{
		SchemaRegistryURL: "http://localhost:65000",
		MaxRetries:        3,
		RetriesWaitMs:     1,
		RetriesMaxWaitMs:  2,
		HTTPClient:        &http.Client{Transport: transport},
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	request := NewRequest("GET", "/subjects/test-subject", nil)
	var response map[string]interface{}
	if err := rs.HandleRequest(request, &response); err != nil {
		t.Fatalf("Expected success after retrying network errors, got %v", err)
	}
	if response["name"] != "test-subject" {
		t.Errorf("Expected response name 'test-subject', got %v", response["name"])
	}
	if got := transport.attempts(); got != 3 {
		t.Errorf("Expected 3 attempts (2 network failures + 1 success), got %d", got)
	}
}

// TestHandleRequest_PreservesBodyOnRetry verifies that the request body is
// re-sent intact on a retried attempt, rather than being drained on the first
// attempt and sent empty on subsequent ones.
func TestHandleRequest_PreservesBodyOnRetry(t *testing.T) {
	var bodies []string
	attempt := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(data))
		attempt++
		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			// Force a retry on the first attempt.
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error_code": 50301, "message": "unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":1}`))
	}))
	defer server.Close()

	config := &ClientConfig{
		SchemaRegistryURL: server.URL,
		MaxRetries:        3,
		RetriesWaitMs:     1,
		RetriesMaxWaitMs:  2,
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	reqBody := map[string]interface{}{"schema": "string"}
	request := NewRequest("POST", "/subjects/test/versions", reqBody)
	var response map[string]interface{}
	if err := rs.HandleRequest(request, &response); err != nil {
		t.Fatalf("Expected success after retry, got %v", err)
	}

	if len(bodies) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(bodies))
	}
	if bodies[0] == "" {
		t.Error("First attempt sent an empty body")
	}
	if bodies[1] != bodies[0] {
		t.Errorf("Retried attempt body %q differs from first attempt body %q", bodies[1], bodies[0])
	}
}

// TestHandleRequest_DoesNotRetryRedirectPolicyError verifies that an error
// where http.Client.Do returns a non-nil response together with the error
// (i.e. a redirect policy failure) is not retried, since it is not a transient
// network failure.
func TestHandleRequest_DoesNotRetryRedirectPolicyError(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		// Always redirect to itself, so the client eventually fails its
		// redirect policy ("stopped after N redirects"), which makes Do return
		// a non-nil response alongside the error.
		http.Redirect(w, r, "/loop", http.StatusFound)
	}))
	defer server.Close()

	config := &ClientConfig{
		SchemaRegistryURL: server.URL,
		MaxRetries:        3,
		RetriesWaitMs:     1,
		RetriesMaxWaitMs:  2,
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	request := NewRequest("GET", "/start", nil)
	var response interface{}
	if err := rs.HandleRequest(request, &response); err == nil {
		t.Error("Expected redirect policy error, got nil")
	}

	// A single pass follows the default redirect limit (~10 requests). If the
	// redirect-policy error were incorrectly retried, the count would multiply
	// by maxRetries+1 (~40). Assert it stayed within a single pass.
	if got := atomic.LoadInt32(&requests); got > 15 {
		t.Errorf("Redirect policy error should not be retried, but saw %d requests", got)
	}
}

// TestHandleRequest_ExhaustsRetriesOnNetworkError verifies that a persistent
// network-level error is retried up to maxRetries+1 times before failing.
func TestHandleRequest_ExhaustsRetriesOnNetworkError(t *testing.T) {
	transport := &networkErrorTransport{failCalls: 100}
	config := &ClientConfig{
		SchemaRegistryURL: "http://localhost:65000",
		MaxRetries:        2,
		RetriesWaitMs:     1,
		RetriesMaxWaitMs:  2,
		HTTPClient:        &http.Client{Transport: transport},
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	request := NewRequest("GET", "/subjects/test-subject", nil)
	var response map[string]interface{}
	if err := rs.HandleRequest(request, &response); err == nil {
		t.Error("Expected error after exhausting retries on network failure, got nil")
	}
	if got := transport.attempts(); got != 3 {
		t.Errorf("Expected 3 attempts (maxRetries+1), got %d", got)
	}
}

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
		t.Errorf("should work with oauth bearer auth config without identity pool ID (auto mapping), got %v", err)
	}

	// Should also work with identity pool ID specified
	config.BearerAuthIdentityPoolID = "pool_id"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with oauth bearer auth config with identity pool ID, got %v", err)
	}
}

func TestUAMIAuthConfig(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "UAMI"

	_, err := NewRestService(config)
	if !strings.Contains(err.Error(), "bearer.auth.scopes must specify exactly one resource") {
		t.Errorf("should have error about bearer.auth.scopes, got %s", err)
	}

	config.BearerAuthScopes = []string{"https://example.com/.default"}
	_, err = NewRestService(config)
	if !strings.Contains(err.Error(), "bearer.auth.logical.cluster") {
		t.Errorf("should have error about bearer.auth.logical.cluster, got %s", err)
	}

	config.BearerAuthLogicalCluster = "lsrc-123"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with UAMI auth config without identity pool ID (auto mapping), got %s", err)
	}

	// Should also work with identity pool ID specified
	config.BearerAuthIdentityPoolID = "pool_id"
	_, err = NewRestService(config)
	if err != nil {
		t.Errorf("should work with UAMI auth config with identity pool ID, got %s", err)
	}
}

func TestUAMIAuthConfigWithCustomProvider(t *testing.T) {
	config := &ClientConfig{}

	config.BearerAuthCredentialsSource = "UAMI"
	config.AuthenticationHeaderProvider = &CustomHeaderProvider{
		token:                        testToken,
		schemaRegistryLogicalCluster: testLogicalCluster,
		identityPoolID:               testIdentityPoolID,
	}

	_, err := NewRestService(config)
	if !strings.Contains(err.Error(), "cannot have bearer.auth.credentials.source UAMI") {
		t.Errorf("should have error about custom provider conflict with UAMI, got %s", err)
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

func TestHandleRequest_UnauthorizedError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error_code": 401, "message": "Invalid credentials"}`))
	}))
	defer server.Close()

	config := &ClientConfig{
		SchemaRegistryURL: server.URL,
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	request := NewRequest("GET", "/subjects", nil)
	var response interface{}

	err = rs.HandleRequest(request, &response)

	if err == nil {
		t.Error("Expected error for 401 response, got nil")
	}

	expectedError := "schema registry request failed error code: 401: Invalid credentials"
	if err.Error() != expectedError {
		t.Errorf("Expected error message %q, got %q", expectedError, err.Error())
	}
}

func TestHandleRequest_SuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"name":"test-subject"}`))
	}))
	defer server.Close()

	config := &ClientConfig{
		SchemaRegistryURL: server.URL,
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	request := NewRequest("GET", "/subjects/test-subject", nil)
	var response map[string]interface{}

	err = rs.HandleRequest(request, &response)

	if err != nil {
		t.Errorf("Expected no error for 200 response, got %v", err)
	}

	if response["name"] != "test-subject" {
		t.Errorf("Expected response name 'test-subject', got %v", response["name"])
	}
}

func TestHandleRequest_ConcurrentAccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":1}`))
	}))
	defer server.Close()

	config := &ClientConfig{
		SchemaRegistryURL:           server.URL,
		BearerAuthCredentialsSource: "STATIC_TOKEN",
		BearerAuthToken:             "test-token",
		BearerAuthLogicalCluster:    "lsrc-123",
		BearerAuthIdentityPoolID:    "pool-id",
	}

	rs, err := NewRestService(config)
	if err != nil {
		t.Fatalf("Failed to create RestService: %v", err)
	}

	// Use a start barrier so all goroutines begin at the same time,
	// maximizing the chance of concurrent header map access.
	const goroutines = 20
	const requestsPerGoroutine = 5
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, goroutines*requestsPerGoroutine)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < requestsPerGoroutine; j++ {
				request := NewRequest("GET", "/subjects", nil)
				var response interface{}
				errs <- rs.HandleRequest(request, &response)
			}
		}()
	}

	// Release all goroutines simultaneously
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Concurrent request failed: %v", err)
		}
	}
}

func TestStaticTokenAuthWithOptionalIdentityPoolID(t *testing.T) {
	parsedURL, err := url.Parse("mock://")
	if err != nil {
		t.Fatalf("Failed to parse URL, got %v", err)
	}

	// Test without identity pool ID (auto mapping)
	config := &ClientConfig{
		BearerAuthCredentialsSource: "STATIC_TOKEN",
		BearerAuthToken:             "test-token",
		BearerAuthLogicalCluster:    "lsrc-123",
		BearerAuthIdentityPoolID:    "", // Empty for auto mapping
	}

	provider, err := NewAuthenticationHeaderProvider(parsedURL, config)
	if err != nil {
		t.Fatalf("Should work with static token without identity pool ID (auto mapping), got %v", err)
	}

	authHeader, err := provider.GetAuthenticationHeader()
	if err != nil {
		t.Errorf("GetAuthenticationHeader returned error: %v", err)
	}
	if authHeader != "Bearer test-token" {
		t.Errorf("Expected Bearer test-token, got %s", authHeader)
	}

	identityPoolID, err := provider.GetIdentityPoolID()
	if err != nil {
		t.Errorf("GetIdentityPoolID returned error: %v", err)
	}
	if identityPoolID != "" {
		t.Errorf("Expected empty identity pool ID, got %s", identityPoolID)
	}

	logicalCluster, err := provider.GetLogicalCluster()
	if err != nil {
		t.Errorf("GetLogicalCluster returned error: %v", err)
	}
	if logicalCluster != "lsrc-123" {
		t.Errorf("Expected lsrc-123, got %s", logicalCluster)
	}

	// Test with identity pool ID specified
	config.BearerAuthIdentityPoolID = "pool-1,pool-2"
	provider, err = NewAuthenticationHeaderProvider(parsedURL, config)
	if err != nil {
		t.Fatalf("Should work with static token with identity pool ID, got %v", err)
	}

	identityPoolID, err = provider.GetIdentityPoolID()
	if err != nil {
		t.Errorf("GetIdentityPoolID returned error: %v", err)
	}
	if identityPoolID != "pool-1,pool-2" {
		t.Errorf("Expected pool-1,pool-2, got %s", identityPoolID)
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
		t.Fatalf("Should work with static token without identity pool ID, got %v", err)
	}

	err = SetAuthenticationHeaders(rs.authenticationHeaderProvider, &rs.headers)
	if err != nil {
		t.Fatalf("Should set headers without identity pool ID, got %v", err)
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
