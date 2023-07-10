package main

import (
	"context"
	"fmt"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// handleJWTTokenRefreshEvent retrieves JWT from the SPIRE workload API and
// sets the token on the client for use in any future authentication attempt.
// It must be invoked whenever kafka.OAuthBearerTokenRefresh appears on the client's event channel,
// which will occur whenever the client requires a token (i.e. when it first starts and when the
// previously-received token is 80% of the way to its expiration time).
func handleJWTTokenRefreshEvent(ctx context.Context, client kafka.Handle, principal, socketPath string, audience []string) {
	fmt.Fprintf(os.Stderr, "Token refresh\n")
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
		// initialize the fields of Params here
		Audience: audience[0],
		// Other fields...
	}

	jwtSVID, err := jwtSource.FetchJWTSVID(ctx, params)
	if err != nil {
		return kafka.OAuthBearerToken{}, nil, fmt.Errorf("unable to fetch JWT SVID: %w", err)
	}

	extensions := map[string]string{
		"logicalCluster": "lkc-r6gdo0",
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

	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <topic> <principal> <socketPath>\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	principal := os.Args[3]
	socketPath := os.Args[4]
	audience := []string{"audience1", "audience2"}

	// You'll probably need to modify this configuration to
	// match your environment.
	config := kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"security.protocol":       "SASL_SSL",
		"sasl.mechanisms":         "OAUTHBEARER",
		"sasl.oauthbearer.config": principal,
	}

	p, err := kafka.NewProducer(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// Token refresh events are posted on the Events channel, instructing
	// the application to refresh its token.
	ctx := context.Background()

	go func(eventsChan chan kafka.Event) {
		for ev := range eventsChan {
			_, ok := ev.(kafka.OAuthBearerTokenRefresh)
			if !ok {
				// Ignore other event types
				continue
			}

			handleJWTTokenRefreshEvent(ctx, p, principal, socketPath, audience)
		}
	}(p.Events())

	run := true
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	msgcnt := 0
	for run {
		select {
		case sig := <-signalChannel:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			value := fmt.Sprintf("Producer example, message #%d", msgcnt)
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(value),
				Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			}, nil)

			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrQueueFull {
					// Producer queue is full, wait 1s for messages
					// to be delivered then try again.
					time.Sleep(time.Second)
					continue
				}
				fmt.Printf("Failed to produce message: %v\n", err)
			} else {
				fmt.Printf("Produced message: %s\n", value)
			}

			time.Sleep(1 * time.Second)
			msgcnt++
		}
	}

	p.Close()
}
