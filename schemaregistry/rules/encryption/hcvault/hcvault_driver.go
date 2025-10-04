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

package hcvault

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"os"

	auth "github.com/hashicorp/vault/api/auth/approle"
)

const (
	prefix          = "hcvault://"
	tokenID         = "token.id"
	namespace       = "namespace"
	appRoleID       = "app.role.id"
	appRoleSecretID = "app.role.secret.id"
)

func init() {
	Register()
}

// Register registers the HashiCorp Vault driver
func Register() {
	driver := &hcvaultDriver{}
	encryption.RegisterKMSDriver(driver)
}

type hcvaultDriver struct {
}

func (l *hcvaultDriver) GetKeyURLPrefix() string {
	return prefix
}

func (l *hcvaultDriver) NewKMSClient(config map[string]string, keyURL *string) (registry.KMSClient, error) {
	uriPrefix := prefix
	if keyURL != nil {
		uriPrefix = *keyURL
	}
	token := config[tokenID]
	if token == "" {
		token = os.Getenv("VAULT_TOKEN")
	}
	ns := config[namespace]
	if ns == "" {
		ns = os.Getenv("VAULT_NAMESPACE")
	}
	client, err := NewClient(uriPrefix, nil, ns, token)
	if err != nil {
		return nil, err
	}
	roleID := config[appRoleID]
	if roleID == "" {
		roleID = os.Getenv("VAULT_APP_ROLE_ID")
	}
	roleSecretID := config[appRoleSecretID]
	if roleSecretID == "" {
		roleSecretID = os.Getenv("VAULT_APP_ROLE_SECRET_ID")
	}
	if roleID != "" && roleSecretID != "" {
		secretID := &auth.SecretID{FromString: roleSecretID}
		appRoleAuth, err := auth.NewAppRoleAuth(
			roleID,
			secretID,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize AppRole auth method: %w", err)
		}

		vaultClient := client.(*vaultClient).Client()
		authInfo, err := vaultClient.Auth().Login(context.Background(), appRoleAuth)
		if err != nil {
			return nil, fmt.Errorf("unable to login to AppRole auth method: %w", err)
		}
		if authInfo == nil {
			return nil, fmt.Errorf("no auth info was returned after login")
		}
	}
	return client, nil
}
