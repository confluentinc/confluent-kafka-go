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

// Example consumer with a custom OAUTHBEARER token implementation.
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	joseHeaderEncoded     = "eyJhbGciOiJub25lIn0" // {"alg":"none"}
)

// handleOAuthBearerTokenRefreshEvent generates an unsecured JWT based on the configuration defined
// in sasl.oauthbearer.config and sets the token on the client for use in any future authentication attempt.
// It must be invoked whenever kafka.OAuthBearerTokenRefresh appears on the client's event channel,
// which will occur whenever the client requires a token (i.e. when it first starts and when the
// previously-received token is 80% of the way to its expiration time).
func handleOAuthBearerTokenRefreshEvent(client kafka.Handle, e kafka.OAuthBearerTokenRefresh) {
	fmt.Fprintf(os.Stderr, "Token refresh\n")
	oauthBearerToken, retrieveErr := retrieveUnsecuredToken(e)
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

func retrieveUnsecuredToken(e kafka.OAuthBearerTokenRefresh) (kafka.OAuthBearerToken, error) {
	config := e.Config
	if !oauthbearerConfigRegex.MatchString(config) {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T due to malformed config: %s", e, config)
	}
	// set up initial map with default values
	oauthbearerConfigMap := map[string]string{
		principalClaimNameKey: "sub",
	}
	// parse the provided config and store name=value pairs in the map
	for _, kv := range oauthbearerNameEqualsValueRegex.FindAllStringSubmatch(config, -1) {
		oauthbearerConfigMap[kv[1]] = kv[2]
	}
	principalClaimName := oauthbearerConfigMap[principalClaimNameKey]
	principal := oauthbearerConfigMap[principalKey]
	// regexp is such that principalClaimName cannot end up blank,
	// so check for a blank principal (which will happen if it isn't specified)
	if principal == "" {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T: no %s: %s", e, principalKey, config)
	}
	// do not proceed if there are any unknown name=value pairs
	if len(oauthbearerConfigMap) > 2 {
		return kafka.OAuthBearerToken{}, fmt.Errorf("ignoring event %T: unrecognized key(s): %s", e, config)
	}

	now := time.Now()
	nowSecondsSinceEpoch := now.Unix()

	// The token lifetime needs to be long enough to allow connection and a broker metadata query.
	// We then exit immediately after that, so no additional token refreshes will occur.
	// Therefore set the lifetime to be an hour (though anything on the order of a minute or more
	// would be fine).
	// In this example it's kept very short to quickly show the token refresh event in action.
	expiration := now.Add(time.Second * 3)
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

	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <topic> <group> \"[principalClaimName=<claimName>] principal=<value>\"\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	group := os.Args[3]
	oauthConf := os.Args[4]

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"security.protocol":        "SASL_PLAINTEXT",
		"sasl.mechanisms":          "OAUTHBEARER",
		"sasl.oauthbearer.config":  oauthConf,
		"group.id":                 group,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %s\n", topic)
		os.Exit(1)
	}

	run := true
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-signalChannel:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			case kafka.OAuthBearerTokenRefresh:
				handleOAuthBearerTokenRefreshEvent(c, e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
