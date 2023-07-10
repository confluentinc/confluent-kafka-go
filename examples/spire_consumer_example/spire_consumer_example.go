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
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <topic> <principal> <socketPath> \n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	principal := os.Args[3]
	socketPath := os.Args[4]
	audience := []string{"audience1", "audience2"}

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

	ctx := context.Background()
	go func() {
		for {
			ev := c.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.StoreOffsets([]kafka.TopicPartition{e.TopicPartition})
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset: %v\n", err)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					fmt.Fprintf(os.Stderr, "%% All brokers are down: terminating\n")
					return
				}
			case kafka.OAuthBearerTokenRefresh:
				handleJWTTokenRefreshEvent(ctx, c, principal, socketPath, audience)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}()

	err = c.Subscribe(topic, nil)
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
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
