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

package awskms

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// awsClient is a wrapper around an AWS SDK provided KMS client that can
// instantiate Tink primitives.
type awsClient struct {
	keyURI string
	creds  aws.CredentialsProvider
}

// NewClient returns a [registry.KMSClient] which wraps an AWS KMS
// client and will handle keys whose URIs start with uriPrefix.
//
// By default, the client will use default credentials.
func NewClient(keyURI string, creds aws.CredentialsProvider) (registry.KMSClient, error) {
	if !strings.HasPrefix(strings.ToLower(keyURI), prefix) {
		return nil, fmt.Errorf("keyURI must start with %q, but got %q", prefix, keyURI)
	}

	return &awsClient{
		keyURI: keyURI,
		creds:  creds,
	}, nil
}

// Supported returns true if keyURI starts with the URI prefix provided when
// creating the client.
func (c *awsClient) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, prefix)
}

// GetAEAD returns an implementation of the AEAD interface which performs
// cryptographic operations remotely via AWS KMS using keyURI.
//
// keyUri must be supported by this client and must have the following format:
//
//	aws-kms://arn:<partition>:kms:<region>:<path>
//
// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
func (c *awsClient) GetAEAD(keyURI string) (tink.AEAD, error) {
	if !c.Supported(keyURI) {
		return nil, fmt.Errorf("keyURI must start with prefix %s, but got %s", prefix, keyURI)
	}
	uri := strings.TrimPrefix(keyURI, prefix)
	return NewAEAD(uri, c.creds)
}
