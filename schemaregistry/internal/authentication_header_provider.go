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
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

const tokenExpiryThreshold = 0.8

// AuthenticationHeaderProvider is an interface that provides a method to set authentication headers.
type AuthenticationHeaderProvider interface {
	SetAuthenticationHeaders(header *http.Header) error
}

// TokenFetcher is an interface that provides a method to fetch a token
type TokenFetcher interface {
	Token(ctx context.Context) (*oauth2.Token, error)
}

// BearerTokenAuthenticationHeaderProvider is a struct that implements the AuthenticationHeaderProvider interface
type BearerTokenAuthenticationHeaderProvider struct {
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
	client TokenFetcher,
	maxRetries int,
	retriesWaitMs int,
	retriesMaxWaitMs int,
) *BearerTokenAuthenticationHeaderProvider {
	ceilingRetries := int(math.Log2(float64(retriesMaxWaitMs) / float64(retriesWaitMs)))

	return &BearerTokenAuthenticationHeaderProvider{
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

// SetAuthenticationHeaders sets the Authorization header on the given http.Header using the current token
// and returns an error if the token cannot be fetched or the header cannot be set.
func (p *BearerTokenAuthenticationHeaderProvider) SetAuthenticationHeaders(header *http.Header) error {
	token, err := p.GetToken()
	if err != nil {
		return err
	}

	header.Set("Authorization", "Bearer "+token)
	return nil
}
