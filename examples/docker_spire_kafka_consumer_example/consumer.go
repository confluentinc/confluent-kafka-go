/**
 * Copyright 2023 Confluent Inc.
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

// Example consumer with a custom SPIRE token implementation.
package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//var bootstrapServers, topic, socketPath, principal string

//func init() {
//	bootstrapServers = readFromENV("BOOTSTRAP_SERVERS", "")
//	topic = readFromENV("TOPIC", "SASL_SSL")
//	principal = readFromENV("principal", "")
//	socketPath = readFromENV("socketPath", "")
//}

// handleJWTTokenRefreshEvent retrieves JWT from the SPIRE workload API and
// sets the token on the client for use in any future authentication attempt.
// It must be invoked whenever kafka.OAuthBearerTokenRefresh appears on the client's event channel,
// which will occur whenever the client requires a token (i.e. when it first starts and when the
// previously-received token is 80% of the way to its expiration time).
func handleJWTTokenRefreshEvent(ctx context.Context, client kafka.Handle, principal, socketPath string, audience []string) {
	fmt.Fprintf(os.Stderr, "Token refresh in handleJWTTokenRefreshEvent\n")
	oauthBearerToken, closer, retrieveErr := retrieveJWTToken(ctx, principal, socketPath, audience)
	defer closer()
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

func retrieveJWTToken(ctx context.Context, principal, socketPath string, audience []string) (kafka.OAuthBearerToken, func() error, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	jwtSource, err := workloadapi.NewJWTSource(
		ctx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(socketPath)),
	)

	if err != nil {
		return kafka.OAuthBearerToken{}, nil, fmt.Errorf("unable to create JWTSource: %w", err)
	}

	defer jwtSource.Close()

	params := jwtsvid.Params{
		Audience: audience[0],
		// Other fields...
	}

	jwtSVID, err := jwtSource.FetchJWTSVID(ctx, params)
	if err != nil {
		return kafka.OAuthBearerToken{}, nil, fmt.Errorf("unable to fetch JWT SVID: %w", err)
	}

	extensions := map[string]string{
		"logicalCluster": "lkc-0yoqvq",
		"identityPoolId": "pool-W9j5",
	}
	oauthBearerToken := kafka.OAuthBearerToken{
		TokenValue: jwtSVID.Marshal(),
		Expiration: jwtSVID.Expiry,
		Principal:  principal,
		Extensions: extensions,
	}

	return oauthBearerToken, jwtSource.Close, nil
}

func main() {

	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	topic := os.Getenv("TOPIC")
	principal := os.Getenv("PRINCIPAL")
	socketPath := os.Getenv("SOCKET_PATH")
	audience := []string{"audience1", "audience2"}

	fmt.Fprintf(os.Stderr, "Token refresh\n")
	fmt.Fprintf(os.Stderr, "bootstrapServers is: %s\n", bootstrapServers)

	config := kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "OAUTHBEARER",
		"sasl.oauthbearer.config":  principal,
		"group.id":                 "myGroup",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	}

	c, err := kafka.NewConsumer(&config)

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

	ctx := context.Background()

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
				handleJWTTokenRefreshEvent(ctx, c, principal, socketPath, audience)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
