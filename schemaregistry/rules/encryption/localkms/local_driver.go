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
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/subtle"
	"google.golang.org/protobuf/proto"
	"strings"

	agpb "github.com/tink-crypto/tink-go/v2/proto/aes_gcm_go_proto"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	prefix = "local-kms://"
	secret = "secret"
)

func init() {
	Register()
}

// Register registers the local KMS driver
func Register() {
	driver := &localDriver{}
	encryption.RegisterKMSDriver(driver)
}

type localDriver struct {
}

func (l *localDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *localDriver) NewKMSClient(config map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	secretKey, ok := config[secret]
	if !ok {
		return nil, errors.New("cannot load secret")
	}
	return NewLocalClient(uriPrefix, secretKey)
}

// NewLocalClient returns a new local KMS client
func NewLocalClient(uriPrefix string, secret string) (registry.KMSClient, error) {
	if !strings.HasPrefix(strings.ToLower(uriPrefix), prefix) {
		return nil, fmt.Errorf("uriPrefix must start with %s, but got %s", prefix, uriPrefix)
	}
	keyBytes, err := subtle.ComputeHKDF("SHA256", []byte(secret), nil, nil, 16)
	if err != nil {
		return nil, err
	}
	aesGCMKey := &agpb.AesGcmKey{Version: 0, KeyValue: keyBytes}
	serializedAESGCMKey, err := proto.Marshal(aesGCMKey)
	if err != nil {
		return nil, err
	}
	dekTemplate := aead.AES128GCMKeyTemplate()
	primitive, err := registry.Primitive(dekTemplate.TypeUrl, serializedAESGCMKey)
	if err != nil {
		return nil, err
	}
	return &localClient{
		keyURIPrefix: uriPrefix,
		primitive:    primitive.(tink.AEAD),
	}, nil
}
