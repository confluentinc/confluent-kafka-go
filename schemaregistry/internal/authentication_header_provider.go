/**
 * Copyright 2025 Confluent Inc.
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
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

const tokenExpiryThreshold = 0.8

// AuthenticationHeaderProvider is an interface that provides a method to set authentication headers.
type AuthenticationHeaderProvider interface {
	GetAuthenticationHeader() (string, error)
	GetIdentityPoolID() (string, error)
	GetLogicalCluster() (string, error)
}

// BasicAuthenticationHeaderProvider is a struct that implements the AuthenticationHeaderProvider interface
// and provides a method to set the Basic Authentication header.
type BasicAuthenticationHeaderProvider struct {
	basicAuth string
}

// GetAuthenticationHeader returns the Basic Authentication header
func (p *BasicAuthenticationHeaderProvider) GetAuthenticationHeader() (string, error) {
	return "Basic " + p.basicAuth, nil
}

// GetIdentityPoolID returns an empty string as Basic Authentication does not use identity pool ID
func (p *BasicAuthenticationHeaderProvider) GetIdentityPoolID() (string, error) {
	return "", nil
}

// GetLogicalCluster returns an empty string as Basic Authentication does not use logical cluster
func (p *BasicAuthenticationHeaderProvider) GetLogicalCluster() (string, error) {
	return "", nil
}

// NewBasicAuthenticationHeaderProvider creates a new BasicAuthenticationHeaderProvider
func NewBasicAuthenticationHeaderProvider(convertedString string) *BasicAuthenticationHeaderProvider {
	return &BasicAuthenticationHeaderProvider{
		basicAuth: convertedString,
	}
}

// StaticTokenAuthenticationHeaderProvider is a struct that implements the AuthenticationHeaderProvider interface
// and provides a method to set the Static Token Authentication header.
type StaticTokenAuthenticationHeaderProvider struct {
	token          string
	identityPoolID string
	logicalCluster string
}

// GetAuthenticationHeader returns the Static Token Authentication header
func (p *StaticTokenAuthenticationHeaderProvider) GetAuthenticationHeader() (string, error) {
	return "Bearer " + p.token, nil
}

// GetIdentityPoolID returns the identity pool ID
func (p *StaticTokenAuthenticationHeaderProvider) GetIdentityPoolID() (string, error) {
	return p.identityPoolID, nil
}

// GetLogicalCluster returns the logical cluster
func (p *StaticTokenAuthenticationHeaderProvider) GetLogicalCluster() (string, error) {
	return p.logicalCluster, nil
}

// NewStaticTokenAuthenticationHeaderProvider creates a new StaticTokenAuthenticationHeaderProvider
func NewStaticTokenAuthenticationHeaderProvider(token string, identityPoolID string, logicalCluster string) *StaticTokenAuthenticationHeaderProvider {
	return &StaticTokenAuthenticationHeaderProvider{
		token:          token,
		identityPoolID: identityPoolID,
		logicalCluster: logicalCluster,
	}
}

// TokenFetcher is an interface that provides a method to fetch a token
type TokenFetcher interface {
	Token(ctx context.Context) (*oauth2.Token, error)
}

// BearerTokenAuthenticationHeaderProvider is a struct that implements the AuthenticationHeaderProvider interface
type BearerTokenAuthenticationHeaderProvider struct {
	identityPoolID   string
	logicalCluster   string
	maxRetries       int
	retriesWaitMs    int
	retriesMaxWaitMs int
	ceilingRetries   int
	client           TokenFetcher
	token            *oauth2.Token
	expiry           time.Time
	tokenLock        sync.RWMutex
}

// NewBearerTokenAuthenticationHeaderProvider creates a new BearerTokenAuthenticationHeaderProvider
func NewBearerTokenAuthenticationHeaderProvider(
	identityPoolID string,
	logicalCluster string,
	client TokenFetcher,
	maxRetries int,
	retriesWaitMs int,
	retriesMaxWaitMs int,
) *BearerTokenAuthenticationHeaderProvider {
	ceilingRetries := int(math.Log2(float64(retriesMaxWaitMs) / float64(retriesWaitMs)))

	return &BearerTokenAuthenticationHeaderProvider{
		identityPoolID:   identityPoolID,
		logicalCluster:   logicalCluster,
		client:           client,
		maxRetries:       maxRetries,
		retriesWaitMs:    retriesWaitMs,
		retriesMaxWaitMs: retriesMaxWaitMs,
		ceilingRetries:   ceilingRetries,
	}
}

// GetToken returns an up-to-date token if it is still valid, otherwise it generates a new token
func (p *BearerTokenAuthenticationHeaderProvider) GetToken() (string, error) {
	p.tokenLock.RLock()
	if time.Now().Before(p.expiry) {
		curToken := p.token.AccessToken
		p.tokenLock.RUnlock()
		return curToken, nil
	}
	p.tokenLock.RUnlock()

	p.tokenLock.Lock()
	defer p.tokenLock.Unlock()

	if time.Now().Before(p.expiry) {
		curToken := p.token.AccessToken
		return curToken, nil
	}

	err := p.GenerateToken()
	if err != nil {
		return "", err
	}

	return p.token.AccessToken, nil
}

// GenerateToken generates a new token and updates the provider's token and expiry
func (p *BearerTokenAuthenticationHeaderProvider) GenerateToken() error {
	ctx := context.Background()

	for i := 0; i < p.maxRetries+1; i++ {
		token, err := p.client.Token(ctx)
		if err == nil {
			lifetime := time.Until(token.Expiry)
			p.expiry = time.Now().Add(time.Duration(float64(lifetime) * tokenExpiryThreshold))
			p.token = token
			return nil
		}
		if i == p.maxRetries {
			return err
		}

		time.Sleep(fullJitter(i, p.ceilingRetries, p.retriesMaxWaitMs, p.retriesWaitMs))
	}

	return fmt.Errorf("failed to generate token after %d retries", p.maxRetries)
}

// GetAuthenticationHeader returns the Bearer Authentication token and returns an error
// if the token cannot be fetched
func (p *BearerTokenAuthenticationHeaderProvider) GetAuthenticationHeader() (string, error) {
	token, err := p.GetToken()
	if err != nil {
		return "", err
	}

	return "Bearer " + token, nil
}

// GetIdentityPoolID returns the identity pool ID
func (p *BearerTokenAuthenticationHeaderProvider) GetIdentityPoolID() (string, error) {
	return p.identityPoolID, nil
}

// GetLogicalCluster returns the logical cluster
func (p *BearerTokenAuthenticationHeaderProvider) GetLogicalCluster() (string, error) {
	return p.logicalCluster, nil
}
