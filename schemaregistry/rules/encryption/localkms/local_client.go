/**
 * Copyright 2024 Confluent Inc.
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

package localkms

import (
	"fmt"
	"strings"

	"github.com/tink-crypto/tink-go/v2/tink"
)

// localClient represents a client to be used for local testing
type localClient struct {
	keyURIPrefix string
	primitive    tink.AEAD
}

// Supported true if this client does support keyURI
func (c *localClient) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, c.keyURIPrefix)
}

// GetAEAD gets an AEAD backend by keyURI.
// keyURI must have the following format: 'local-kms://{key}'.
func (c *localClient) GetAEAD(keyURI string) (tink.AEAD, error) {
	if !c.Supported(keyURI) {
		return nil, fmt.Errorf("keyURI must start with prefix %s, but got %s", c.keyURIPrefix, keyURI)
	}
	return c.primitive, nil
}
