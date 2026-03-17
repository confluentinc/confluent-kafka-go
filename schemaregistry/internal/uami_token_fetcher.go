/**
 * Copyright 2026 Confluent Inc.
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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"golang.org/x/oauth2"
)

// UAMITokenFetcher implements TokenFetcher using Azure User-Assigned Managed Identity
type UAMITokenFetcher struct {
	credential *azidentity.ManagedIdentityCredential
	scopes     []string
}

// NewUAMITokenFetcher creates a new UAMITokenFetcher
func NewUAMITokenFetcher(endpointURL, endpointQuery string, scopes []string) (*UAMITokenFetcher, error) {
	opts := &azidentity.ManagedIdentityCredentialOptions{}

	if endpointURL != "" {
		opts.ClientOptions = azcore.ClientOptions{
			Cloud: cloud.Configuration{
				ActiveDirectoryAuthorityHost: endpointURL,
			},
		}
	}

	if endpointQuery != "" {
		opts.ID = azidentity.ClientID(endpointQuery)
	}

	cred, err := azidentity.NewManagedIdentityCredential(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create UAMI credential: %w", err)
	}

	return &UAMITokenFetcher{
		credential: cred,
		scopes:     scopes,
	}, nil
}

// Token fetches an access token from Azure and returns it as an oauth2.Token
func (f *UAMITokenFetcher) Token(ctx context.Context) (*oauth2.Token, error) {
	accessToken, err := f.credential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: f.scopes,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get UAMI token: %w", err)
	}

	return &oauth2.Token{
		AccessToken: accessToken.Token,
		Expiry:      accessToken.ExpiresOn,
	}, nil
}
