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

package awskms

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

const (
	prefix          = "aws-kms://"
	accessKeyID     = "access.key.id"
	secretAccessKey = "secret.access.key"
)

func init() {
	Register()
}

// Register registers the AWS KMS driver
func Register() {
	driver := &awsDriver{}
	encryption.RegisterKMSDriver(driver)
}

type awsDriver struct {
}

func (l *awsDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *awsDriver) NewKMSClient(conf map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	var creds aws.CredentialsProvider
	key, ok := conf[accessKeyID]
	if ok {
		secret, ok := conf[secretAccessKey]
		if ok {
			creds = credentials.NewStaticCredentialsProvider(key, secret, "")
		}
	}
	return NewClient(uriPrefix, creds)
}
