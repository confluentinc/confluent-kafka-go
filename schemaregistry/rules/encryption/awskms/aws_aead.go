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
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/tink-crypto/tink-go/v2/tink"
	"regexp"
)

// awsAEAD is an implementation of the AEAD interface which performs
// cryptographic operations remotely via the AWS KMS service using a specific
// key URI.
type awsAEAD struct {
	keyID string
	kms   *kms.Client
}

// NewAEAD returns a new awsAEAD instance.
//
// keyURI must have the following format:
//
//	aws-kms://arn:<partition>:kms:<region>:[<path>]
//
// See http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html.
func NewAEAD(keyID string, creds aws.CredentialsProvider) (tink.AEAD, error) {
	var client *kms.Client
	region, err := getRegion(keyID)
	if err != nil {
		return nil, err
	}
	if creds != nil {
		client = kms.New(kms.Options{
			Credentials: creds,
			Region:      region,
		})
	} else {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		cfg.Region = region
		client = kms.NewFromConfig(cfg)
	}
	return &awsAEAD{
		keyID: keyID,
		kms:   client,
	}, nil
}

// Encrypt encrypts the plaintext with associatedData.
func (a *awsAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	req := &kms.EncryptInput{
		KeyId:     aws.String(a.keyID),
		Plaintext: plaintext,
	}
	resp, err := a.kms.Encrypt(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.CiphertextBlob, nil
}

// Decrypt decrypts the ciphertext and verifies the associated data.
func (a *awsAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	req := &kms.DecryptInput{
		KeyId:          aws.String(a.keyID),
		CiphertextBlob: ciphertext,
	}
	resp, err := a.kms.Decrypt(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.Plaintext, nil
}

// getRegion extracts the region from keyURI.
func getRegion(keyURI string) (string, error) {
	re1, err := regexp.Compile(`arn:(aws[a-zA-Z0-9-_]*):kms:([a-z0-9-]+):`)
	if err != nil {
		return "", err
	}
	r := re1.FindStringSubmatch(keyURI)
	if len(r) != 3 {
		return "", errors.New("extracting region from URI failed")
	}
	return r[2], nil
}
