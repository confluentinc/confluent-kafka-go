package aws_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	oauthaws "github.com/confluentinc/confluent-kafka-go/v2/kafka/oauthbearer/aws"
)

// ExampleTokenProvider_Refresh shows the full wiring for a Kafka consumer
// using AWS IAM Outbound Identity Federation to mint OAUTHBEARER tokens.
//
// The TokenProvider is constructed once at startup. Inside the poll loop,
// Refresh is called whenever librdkafka emits an OAuthBearerTokenRefresh
// event — the client schedules this automatically based on the previous
// token's Expiration, so no timers or goroutines are needed.
func ExampleTokenProvider_Refresh() {
	ctx := context.Background()

	provider, err := oauthaws.New(ctx, oauthaws.Config{
		Region:   "eu-north-1",
		Audience: "https://confluent.cloud/oidc",
		Duration: time.Hour,
	})
	if err != nil {
		log.Fatalf("construct provider: %v", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "broker.example.com:9092",
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "OAUTHBEARER",
		"group.id":          "example",
	})
	if err != nil {
		log.Fatalf("construct consumer: %v", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{"example-topic"}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	for {
		switch ev := c.Poll(1000).(type) {
		case kafka.OAuthBearerTokenRefresh:
			// The provider handles Token() + SetOAuthBearerToken/Failure
			// internally. One line integration inside the event loop.
			provider.Refresh(ctx, c)
		case *kafka.Message:
			fmt.Printf("%s\n", string(ev.Value))
		case kafka.Error:
			log.Printf("kafka error: %v", ev)
		}
	}
}
