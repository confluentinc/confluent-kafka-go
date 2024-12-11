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
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"os"
	"strings"
)

const (
	prefix          = "aws-kms://"
	accessKeyID     = "access.key.id"
	secretAccessKey = "secret.access.key"
	profile         = "profile"
	roleArn         = "role.arn"
	roleSessionName = "role.session.name"
	roleExternalId  = "role.external.id"
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
	arn := conf[roleArn]
	if arn == "" {
		arn = os.Getenv("AWS_ROLE_ARN")
	}
	sessionName := conf[roleSessionName]
	if sessionName == "" {
		sessionName = os.Getenv("AWS_ROLE_SESSION_NAME")
	}
	externalId := conf[roleExternalId]
	if externalId == "" {
		externalId = os.Getenv("AWS_ROLE_EXTERNAL_ID")
	}
	var creds aws.CredentialsProvider
	key := conf[accessKeyID]
	secret := conf[secretAccessKey]
	sourceProfile := conf[profile]
	if key != "" && secret != "" {
		creds = credentials.NewStaticCredentialsProvider(key, secret, "")
	} else if sourceProfile != "" {
		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithSharedConfigProfile(sourceProfile),
		)
		if err != nil {
			return nil, err
		}
		creds = cfg.Credentials
	}
	if arn != "" {
		region, err := getRegion(strings.TrimPrefix(uriPrefix, prefix))
		if err != nil {
			return nil, err
		}
		stsSvc := sts.New(sts.Options{
			Credentials: creds,
			Region:      region,
		})
		if sessionName == "" {
			sessionName = "confluent-encrypt"
		}
		var extId *string
		if externalId != "" {
			extId = &externalId
		}
		creds = stscreds.NewAssumeRoleProvider(stsSvc, arn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = sessionName
			o.ExternalID = extId
		})
		creds = aws.NewCredentialsCache(creds)
	}

	return NewClient(uriPrefix, creds)
}
