// Example client with a custom OAUTHBEARER token implementation.
package main

/**
 * Copyright 2019 Confluent Inc.
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

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"regexp"
	"strconv"
	"time"
)

var (
	// Regex for sasl.oauthbearer.config, which constrains it to be
	// 1 or more name=value pairs with optional ignored whitespace
	oauthbearerConfigRegex = regexp.MustCompile("^(\\s*(\\w+)\\s*=\\s*(\\w+))+\\s*$")
	// Regex used to extract name=value pairs from sasl.oauthbearer.config
	oauthbearerNameEqualsValueRegex = regexp.MustCompile("(\\w+)\\s*=\\s*(\\w+)")
)

const (
	principalClaimNameKey = "principalClaimName"
	principalKey          = "principal"
	lifeSecondsKey        = "lifeSeconds"
	joseHeaderEncoded     = "eyJhbGciOiJub25lIn0" // {"alg":"none"}
)

// handleOAuthBearerTokenRefreshEvent generates an unsecured JWT based on the configuration defined
// in sasl.oauthbearer.config and sets the token on the client for use in any future authentication attempt.
// It must be invoked whenever kafka.OAuthBearerTokenRefresh appears on the client's event channel,
// which will occur whenever the client requires a token (i.e. when it first starts and when the
// previously-received token is 80% of the way to its expiration time).
func handleOAuthBearerTokenRefreshEvent(client kafka.Handle, e kafka.OAuthBearerTokenRefresh) {
	oauthBearerToken, retrieveErr := retrieveUnsecuredToken(e, e.Config)
	if retrieveErr != nil {
		fmt.Fprintf(os.Stderr, "%% Token retrieval error: %v\n", retrieveErr)
		client.SetOAuthBearerTokenFailure(retrieveErr.Error())
	} else {
		setTokenError := client.SetOAuthBearerToken(oauthBearerToken)
		if setTokenError != nil {
			fmt.Fprintf(os.Stderr, "%% Error setting token and extensions: %v\n", setTokenError)
			client.SetOAuthBearerTokenFailure(setTokenError.Error())
		}
	}
}

func retrieveUnsecuredToken(e kafka.OAuthBearerTokenRefresh, config string) (kafka.OAuthBearerToken, error) {
	if !oauthbearerConfigRegex.MatchString(config) {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T due to malformed config: %s", e, config)
	}
	// set up initial map with default values
	oauthbearerConfigMap := map[string]string{
		principalClaimNameKey: "sub",
		lifeSecondsKey:        "3600",
	}
	// parse the provided config and store name=value pairs in the map
	for _, kv := range oauthbearerNameEqualsValueRegex.FindAllStringSubmatch(config, -1) {
		oauthbearerConfigMap[kv[1]] = kv[2]
	}
	principalClaimName := oauthbearerConfigMap[principalClaimNameKey]
	principal := oauthbearerConfigMap[principalKey]
	// regexp is such that principalClaimName (and lifeSeconds, below) cannot
	// end up blank, so check for a blank principal (which will happen if it
	// isn't specified)
	if principal == "" {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T: no %s: %s", e, principalKey, config)
	}
	lifeSeconds, lifeSecondsErr := strconv.Atoi(oauthbearerConfigMap[lifeSecondsKey])
	// sanity-check the provided lifeSeconds value, which must parse as a
	// positive integer (it cannot parse negative since the regex doesn't allow
	// the minus sign)
	if lifeSecondsErr != nil || lifeSeconds == 0 {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T: bad %s: %s", e, lifeSecondsKey, config)
	}
	// do not proceed if there are any unknown name=value pairs
	if len(oauthbearerConfigMap) > 3 {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T: unrecognized key(s): %s", e, config)
	}

	now := time.Now()
	nowSecondsSinceEpoch := now.Unix()

	expiration := now.Add(time.Second * time.Duration(lifeSeconds))
	expirationSecondsSinceEpoch := expiration.Unix()

	oauthbearerMapForJSON := map[string]interface{}{
		principalClaimName: principal,
		"iat":              nowSecondsSinceEpoch,
		"exp":              expirationSecondsSinceEpoch,
	}
	claimsJSON, _ := json.Marshal(oauthbearerMapForJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	jwsCompactSerialization := joseHeaderEncoded + "." + encodedClaims + "."
	extensions := map[string]string{}
	oauthBearerToken := kafka.OAuthBearerToken{
		TokenValue: jwsCompactSerialization,
		Expiration: expiration,
		Principal:  principal,
		Extensions: extensions,
	}
	return oauthBearerToken, nil
}

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> \"scope=.. oauthbearer config..\"", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	oauthConf := os.Args[2]

	// You'll probably need to modify this configuration to
	// match your environment.
	config := kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"security.protocol":       "SASL_PLAINTEXT",
		"sasl.mechanisms":         "OAUTHBEARER",
		"sasl.oauthbearer.config": oauthConf,
	}

	p, err := kafka.NewProducer(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// Token refresh events are posted on the Events channel, instructing
	// the application to refresh its token.
	go func(eventsChan chan kafka.Event) {
		for ev := range eventsChan {
			oart, ok := ev.(kafka.OAuthBearerTokenRefresh)
			if !ok {
				// Ignore other event types
				continue
			}

			handleOAuthBearerTokenRefreshEvent(p, oart)
		}
	}(p.Events())

	// Get Metadata and print the broker list.
	md, err := p.GetMetadata(nil, false, 5000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire metadata: %s\n", err)
		p.Close()
		os.Exit(1)
	}

	for _, broker := range md.Brokers {
		fmt.Printf("Broker %d: %s:%d\n",
			broker.ID, broker.Host, broker.Port)
	}

	p.Close()
}
