/**
 * Copyright 2024 Confluent Inc.
 * Copyright 2017 Google Inc.
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

package hcvault

import (
	"crypto/tls"
	"errors"
	"fmt"
	vault "github.com/tink-crypto/tink-go-hcvault/v2/integration/hcvault"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/vault/api"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// vaultClient represents a client that connects to the HashiCorp Vault backend.
type vaultClient struct {
	keyURIPrefix string
	client       *api.Logical
}

var _ registry.KMSClient = (*vaultClient)(nil)

// NewClient returns a new client to HashiCorp Vault.
// uriPrefix parameter is a valid URI which must have "hcvault" scheme and
// vault server address and port. Specific key URIs will be matched against this
// prefix to determine if the client supports the key or not.
// tlsCfg represents tls.Config which will be used to communicate with Vault
// server via HTTPS protocol. If not specified a default tls.Config{} will be
// used.
func NewClient(uriPrefix string, tlsCfg *tls.Config, namespace string, token string) (registry.KMSClient, error) {
	if !strings.HasPrefix(strings.ToLower(uriPrefix), prefix) {
		return nil, fmt.Errorf("key URI must start with %s", prefix)
	}

	uri := strings.TrimPrefix(uriPrefix, prefix)
	if !strings.HasPrefix(uri, "http") {
		uri = "https://" + uri
	}
	httpClient := api.DefaultConfig().HttpClient
	transport := httpClient.Transport.(*http.Transport)
	if tlsCfg == nil {
		tlsCfg = &tls.Config{}
	} else {
		tlsCfg = tlsCfg.Clone()
	}
	transport.TLSClientConfig = tlsCfg

	vurl, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	address := vurl.Scheme + "://" + vurl.Host
	cfg := &api.Config{
		Address:    address,
		HttpClient: httpClient,
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	if namespace != "" {
		client.SetNamespace(namespace)
	}
	client.SetToken(token)
	return &vaultClient{
		keyURIPrefix: uriPrefix,
		client:       client.Logical(),
	}, nil
}

// Supported returns true if this client does support keyURI.
func (c *vaultClient) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, c.keyURIPrefix)
}

// GetAEAD gets an AEAD backed by keyURI.
func (c *vaultClient) GetAEAD(keyURI string) (tink.AEAD, error) {
	if !c.Supported(keyURI) {
		return nil, errors.New("unsupported keyURI")
	}
	uri := strings.TrimPrefix(keyURI, prefix)
	u, err := url.Parse(uri)
	if err != nil {
		return nil, errors.New("malformed keyURI")
	}
	keyPath := u.EscapedPath()
	return vault.NewAEAD(keyPath, c.client)
}
