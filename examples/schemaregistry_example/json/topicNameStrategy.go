package main

import (
	// "errors"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"log"
	"time"
)

const (
	producerMode          string = "producer"
	consumerMode          string = "consumer"
	nullOffset                   = -1
	topic                        = "my-topic"
	kafkaURL                     = "127.0.0.1:29092"
	srURL                        = "http://127.0.0.1:8081"
	schemaFile            string = "./api/v1/proto/Person.proto"
	consumerGroupID              = "test-consumer"
	defaultSessionTimeout        = 6000
	noTimeout                    = -1
)

func main() {

	clientMode := os.Args[1]

	if strings.Compare(clientMode, producerMode) == 0 {
		producer()
	} else if strings.Compare(clientMode, consumerMode) == 0 {
		consumer()
	} else {
		fmt.Printf("Invalid option. Valid options are '%s' and '%s'.",
			producerMode, consumerMode)
	}
}

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func producer() {
	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := Person{
		Name: "robert",
		Age:  23,
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg interface{}, topic string) (int64, error)
	Close()
}

type srProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(kafkaURL, srURL string) (SRProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		return nil, err
	}
	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}
	s, err := jsonschema.NewSerializer(c, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		return nil, err
	}
	return &srProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg interface{}, topic string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	payload, err := p.serializer.Serialize(topic, msg)
	if err != nil {
		return nullOffset, err
	}
	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
	}, kafkaChan); err != nil {
		return nullOffset, err
	}
	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		log.Println("message sent: ", string(ev.Value))
		return int64(ev.TopicPartition.Offset), nil
	case kafka.Error:
		return nullOffset, err
	}
	return nullOffset, nil
}

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}

/*
* ===============================
* CONSUMER
* ===============================
**/
func consumer() {
	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	err = consumer.Run(topic)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// SRConsumer interface
type SRConsumer interface {
	Run(topic string) error
	Close()
}

type srConsumer struct {
	consumer     *kafka.Consumer
	deserializer *jsonschema.Deserializer
}

// NewConsumer returns new consumer with schema registry
func NewConsumer(kafkaURL, srURL string) (SRConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaURL,
		"group.id":           consumerGroupID,
		"session.timeout.ms": defaultSessionTimeout,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	sr, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	d, err := jsonschema.NewDeserializer(sr, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

// // RegisterMessageFactory Pass a pointer to the receiver object for the SR to unmarshal the payload into
// func (c *srConsumer) RegisterMessageFactory(receiver interface{}) func(string, string) (interface{}, error) {
// 	return func(subject string, name string) (interface{}, error) {
// 		return receiver, nil
// 	}
// }

// NOTE doing like so make sure the event subject match the expected receiver's subject
func (c *srConsumer) RegisterMessageFactoryWithMap(subjectTypes map[string]interface{}) func(string, string) (interface{}, error) {
	// !!! in json there is no 'name' passed into the MessageFactory
	// we only can rely on the subject
	return func(subject string, name string) (interface{}, error) {
		if tp, ok := subjectTypes[subject]; !ok {
			return nil, fmt.Errorf("Invalid receiver")
		} else {
			return tp, nil
		}
	}
}

// Run consumer
// func (c *srConsumer) Run(messageType protoreflect.MessageType, topic string) error {
func (c *srConsumer) Run(topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	// receivers := make(map[string]interface{})
	// receivers[fmt.Sprintf("%v-value", topic)] = &Person{}
	// c.deserializer.MessageFactory = c.RegisterMessageFactoryWithMap(receivers)
	// c.deserializer.MessageFactory = c.RegisterMessageFactory(&Person{})

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		// get a msg of type interface{}
		msg, err := c.deserializer.Deserialize(topic, kafkaMsg.Value)
		if err != nil {
			return err
		}
		// fmt.Println("with MessageFactory: ", msg.(*Person).Name)
		c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// use deserializer.DeserializeInto to get a struct back
		person := &Person{}
		err = c.deserializer.DeserializeInto(topic, kafkaMsg.Value, person)
		if err != nil {
			return err
		}
		fmt.Println("See the struct: ", person.Name, " - ", person.Age)

		if _, err = c.consumer.CommitMessage(kafkaMsg); err != nil {
			return err
		}
	}
}

func (c *srConsumer) handleMessageAsInterface(message interface{}, offset int64) {
	fmt.Printf("message %v with offset %d\n", message, offset)

}

// Close all connections
func (c *srConsumer) Close() {
	if err := c.consumer.Close(); err != nil {
		log.Fatal(err)
	}
	c.deserializer.Close()
}
