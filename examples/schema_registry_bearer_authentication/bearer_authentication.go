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

// Examples of using bearer authentication with schema registry
package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

var srUrl = "https://psrc-1234.us-east-1.aws.confluent.cloud"
var tokenUrl = "your-token-url"
var clientId = "your-client-id"
var clientSecret = "your-client-secret"
var scopes = []string{"schema_registry"}
var identityPoolID = "pool-1234"
var schemaRegistryLogicalCluster = "lsrc-abcd"

type CustomHeaderProvider struct {
	token                        string
	schemaRegistryLogicalCluster string
	identityPoolID               string
}

func (p *CustomHeaderProvider) GetAuthenticationHeader() (string, error) {
	return "Bearer " + p.token, nil
}

func (p *CustomHeaderProvider) GetLogicalCluster() (string, error) {
	return p.schemaRegistryLogicalCluster, nil
}
func (p *CustomHeaderProvider) GetIdentityPoolID() (string, error) {
	return p.identityPoolID, nil
}

func main() {
	// Static token
	staticConf := schemaregistry.NewConfigWithBearerAuthentication(srUrl, "token", schemaRegistryLogicalCluster, identityPoolID)
	staticClient, _ := schemaregistry.NewClient(staticConf)

	subjects, err := staticClient.GetAllSubjects()
	if err != nil {
		fmt.Println("Error fetching subjects:", err)
		return
	}
	fmt.Println("Static token subjects:", subjects)

	//OAuthBearer

	ClientCredentialsConf := schemaRegistry.NewConfig(srUrl)
	ClientCredentialsConf.BearerAuthCredentialsSource = "OAUTHBEARER"
	ClientCredentialsConf.BearerAuthToken = "token"
	ClientCredentialsConf.BearerAuthIdentityPoolID = identityPoolId
	ClientCredentialsConf.BearerAuthLogicalCluster = schemaRegistryLogicalCluster
	ClientCredentialsConf.BearerAuthIssuerEndpointURL = tokenUrl
	ClientCredentialsConf.BearerAuthClientID = clientId
	ClientCredentialsConf.BearerAuthClientSecret = clientSecret
	ClientCredentialsConf.BearerAuthScopes = scopes

	ClientCredentialsClient, _ := schemaregistry.NewClient(ClientCredentialsConf)
	subjects, err = ClientCredentialsClient.GetAllSubjects()
	if err != nil {
		fmt.Println("Error fetching subjects:", err)
		return
	}
	fmt.Println("OAuthBearer subjects:", subjects)

	// Custom
	conf := schemaregistry.NewConfig(srUrl)
	conf.BearerAuthCredentialsSource = "CUSTOM"
	conf.AuthenticationHeaderProvider = &CustomHeaderProvider{
		token:                        "customToken",
		schemaRegistryLogicalCluster: schemaRegistryLogicalCluster,
		identityPoolId:               identityPoolId,
	}
	schemaRegistryClient, err := schemaregistry.NewClient(conf)

	if err != nil {
		fmt.Println("Error creating schema registry client:", err)
		return
	}

	subjects, err = schemaRegistryClient.GetAllSubjects()
	if err != nil {
		fmt.Println("Error fetching subjects:", err)
		return
	}
	fmt.Println("Custom OAuth subjects:", subjects)
}
