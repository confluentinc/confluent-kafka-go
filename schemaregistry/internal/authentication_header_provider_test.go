package internal

import (
	"context"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

var testTokenURL = "test-url"
var testClientID = "client-id"
var testClientSecret = "client-secret"
var testScopes = []string{"schema_registry"}
var testIdentityPoolID = "identity-pool-id"
var testLogicalCluster = "logical-cluster"
var testToken = "test-token"

var maxRetries = 3
var retriesWaitMs = 1000
var retriesMaxWaitMs = 5000

func TestGetAuthenicationHeader(t *testing.T) {
	client := &clientcredentials.Config{
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		TokenURL:     testTokenURL,
		Scopes:       testScopes,
	}
	provider := NewBearerTokenAuthenticationHeaderProvider(
		testIdentityPoolID,
		testLogicalCluster,
		client,
		maxRetries,
		retriesWaitMs,
		retriesMaxWaitMs,
	)

	token := oauth2.Token{
		AccessToken: testToken,
		Expiry:      time.Now().Add(time.Hour * 1),
	}
	provider.token = &token
	provider.expiry = time.Now().Add(time.Hour * 1)

	tokenStr, err := provider.GetAuthenticationHeader()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if tokenStr != "Bearer "+testToken {
		t.Errorf("Expected Authorization header to be %s, got '%s'", "Bearer "+testToken, tokenStr)
	}
}

func TestGetIdentityPoolIDAndLogicalCluster(t *testing.T) {
	client := &clientcredentials.Config{
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		TokenURL:     testTokenURL,
		Scopes:       testScopes,
	}
	provider := NewBearerTokenAuthenticationHeaderProvider(
		testIdentityPoolID,
		testLogicalCluster,
		client,
		maxRetries,
		retriesWaitMs,
		retriesMaxWaitMs,
	)

	id, err := provider.GetIdentityPoolID()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if id != testIdentityPoolID {
		t.Errorf("Expected Identity Pool ID to be %s, got '%s'", testIdentityPoolID, id)
	}

	logicalCluster, err := provider.GetLogicalCluster()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if logicalCluster != testLogicalCluster {
		t.Errorf("Expected Logical Cluster to be %s, got '%s'", testLogicalCluster, logicalCluster)
	}
}

func TestBearerTokenAuthenticationHeaderProviderWithMock(t *testing.T) {
	// Create a mock client that returns a test token
	mockClient := &clientcredentials.Config{
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		TokenURL:     testTokenURL,
		Scopes:       testScopes,
	}

	// Create a custom token source that returns a predefined token
	mockToken := &oauth2.Token{
		AccessToken: testToken,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour * 1),
	}

	provider := NewBearerTokenAuthenticationHeaderProvider(
		testIdentityPoolID,
		testLogicalCluster,
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
	if token != testToken {
		t.Errorf("Expected token to be %s, got '%s'", testToken, token)
	}

	// Test SetAuthenticationHeaders
	tokenStr, err := provider.GetAuthenticationHeader()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if tokenStr != "Bearer "+testToken {
		t.Errorf("Expected Authorization header to be %s, got '%s'", "Bearer "+testToken, tokenStr)
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
	expiryTime := time.Now().Add(time.Second * 1)
	mockToken := &oauth2.Token{
		AccessToken: testToken,
		TokenType:   "Bearer",
		Expiry:      expiryTime,
	}

	mockTokenSource := &MockTokenSource{
		token: mockToken,
		err:   nil,
	}

	mockClient := &MockClientCredentialsConfig{
		mockTokenSource: mockTokenSource,
	}

	provider := NewBearerTokenAuthenticationHeaderProvider(
		testIdentityPoolID,
		testLogicalCluster,
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

	if token != testToken {
		t.Errorf("Expected token to be %s, got %s", testToken, token)
	}

	expectedExpiry := time.Now().Add(time.Duration(float64(time.Until(expiryTime)) * tokenExpiryThreshold))
	actualExpiry := provider.expiry

	timeDiff := actualExpiry.Sub(expectedExpiry)
	if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
		t.Errorf("Expected expiry to be %v, got %v", expectedExpiry, actualExpiry)
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
