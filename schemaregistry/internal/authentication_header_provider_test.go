package internal

import (
	"context"
	"net/http"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var tokenURL = "test-url"
var clientID = "client-id"
var clientSecret = "client-secret"
var scopes = []string{"schema_registry"}

var maxRetries = 3
var retriesWaitMs = 1000
var retriesMaxWaitMs = 5000

// func TestBearerTokenAuthenticationHeaderProvider(t *testing.T) {

// 	client := &clientcredentials.Config{
// 		ClientID:     clientId,
// 		ClientSecret: clientSecret,
// 		TokenURL:     tokenUrl,
// 		Scopes:       scopes,
// 	}

// 	provider := NewBearerTokenAuthenticationHeaderProvider(
// 		client,
// 		maxRetries,
// 		retriesWaitMs,
// 		retriesMaxWaitMs,
// 	)
// 	token, err := provider.GetToken()
// 	if err != nil {
// 		t.Fatalf("Expected no error, got %v", err)
// 	}
// 	fmt.Println("token", token)
// }

func TestSetAuthenticationHeaders(t *testing.T) {
	client := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
		Scopes:       scopes,
	}
	provider := NewBearerTokenAuthenticationHeaderProvider(
		client,
		maxRetries,
		retriesWaitMs,
		retriesMaxWaitMs,
	)

	token := oauth2.Token{
		AccessToken: "test-token",
		Expiry:      time.Now().Add(time.Hour * 1),
	}
	provider.token = &token
	provider.expiry = time.Now().Add(time.Hour * 1)

	headers := &http.Header{}
	err := provider.SetAuthenticationHeaders(headers)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if headers.Get("Authorization") != "Bearer test-token" {
		t.Errorf("Expected Authorization header to be 'Bearer test-token', got '%s'", headers.Get("Authorization"))
	}
}

func TestBearerTokenAuthenticationHeaderProviderWithMock(t *testing.T) {
	// Create a mock client that returns a test token
	mockClient := &clientcredentials.Config{
		ClientID:     "mock-client-id",
		ClientSecret: "mock-client-secret",
		TokenURL:     "https://mock-token-url.com",
		Scopes:       []string{"mock_scope"},
	}

	// Create a custom token source that returns a predefined token
	mockToken := &oauth2.Token{
		AccessToken: "mock-test-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour * 1),
	}

	provider := NewBearerTokenAuthenticationHeaderProvider(
		mockClient,
		maxRetries,
		retriesWaitMs,
		retriesMaxWaitMs,
	)

	// Manually set the token and expiry to simulate a successful token generation
	provider.token = mockToken
	provider.expiry = time.Now().Add(time.Hour * 1)

	// Test GetToken
	token, err := provider.GetToken()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if token != "mock-test-token" {
		t.Errorf("Expected token to be 'mock-test-token', got '%s'", token)
	}

	// Test SetAuthenticationHeaders
	headers := &http.Header{}
	err = provider.SetAuthenticationHeaders(headers)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if headers.Get("Authorization") != "Bearer mock-test-token" {
		t.Errorf("Expected Authorization header to be 'Bearer mock-test-token', got '%s'", headers.Get("Authorization"))
	}
}

// MockTokenSource implements oauth2.TokenSource for testing
type MockTokenSource struct {
	token *oauth2.Token
	err   error
}

type MockClientCredentialsConfig struct {
	mockTokenSource *MockTokenSource
}

func (m *MockClientCredentialsConfig) Token(ctx context.Context) (*oauth2.Token, error) {
	return m.mockTokenSource.token, m.mockTokenSource.err
}

func TestBearerTokenAuthenticationHeaderProviderWithMockTokenSource(t *testing.T) {
	// Create a mock token with a specific expiry time
	expiryTime := time.Now().Add(time.Second * 1)
	mockToken := &oauth2.Token{
		AccessToken: "mock-test-token",
		TokenType:   "Bearer",
		Expiry:      expiryTime,
	}

	// Create a mock token source
	mockTokenSource := &MockTokenSource{
		token: mockToken,
		err:   nil,
	}

	mockClient := &MockClientCredentialsConfig{
		mockTokenSource: mockTokenSource,
	}

	provider := NewBearerTokenAuthenticationHeaderProvider(
		mockClient,
		maxRetries,
		retriesWaitMs,
		retriesMaxWaitMs,
	)

	// Test GenerateToken
	token, err := provider.GetToken()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if token != "mock-test-token" {
		t.Errorf("Expected token to be 'mock-test-token', got '%s'", token)
	}

	expectedExpiry := time.Now().Add(time.Duration(float64(time.Until(expiryTime)) * tokenExpiryThreshold))
	// expectedExpiry := time.Now().Add(time.Duration(float64(time.Until(expiryTime)) * 100))
	actualExpiry := provider.expiry

	// Allow for a small time difference due to execution time
	timeDiff := actualExpiry.Sub(expectedExpiry)
	if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
		maybeFail("testBearerTokenAuthenticationHeaderProviderWithMockTokenSource", err, expect(actualExpiry, expectedExpiry))
	}

	time.Sleep(time.Second * 1)

	mockToken.AccessToken = "new-mock-test-token"
	mockToken.Expiry = time.Now().Add(time.Second * 1)

	token, err = provider.GetToken()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if token != "new-mock-test-token" {
		t.Errorf("Expected token to be 'new-mock-test-token', got '%s'", token)
	}
}
