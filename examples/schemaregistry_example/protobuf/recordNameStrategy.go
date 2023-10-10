package main

import (
	"errors"
	pb "examples/api/v1/proto"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	subjectPerson                = "test.v1.Person"
	subjectAddress               = "another.v1.Address"
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

func producer() {
	producer, err := NewProducer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	msg := &pb.Person{
		Name: "robert",
		Age:  23,
	}

	city := &pb.Address{
		Street: "myStreet",
		City:   "Bangkok",
	}

	for {
		offset, err := producer.ProduceMessage(msg, topic, subjectPerson)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		offset, err = producer.ProduceMessage(city, topic, subjectAddress)
		if err != nil {
			log.Println("Error producing Message: ", err)
		}

		log.Println("Message produced, offset is: ", offset)
		time.Sleep(2 * time.Second)
	}
}

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic, subject string) (int64, error)
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
	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, err
	}
	return &srProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic, subject string) (int64, error) {
	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	// convenient
	// payload, err := p.serializer.SerializeRecordName(msg)

	// assert the fullyQualifiedName. log an err if mismatch
	payload, err := p.serializer.SerializeRecordName(msg, subject)
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

var person = &pb.Person{}
var address = &pb.Address{}

func consumer() {
	consumer, err := NewConsumer(kafkaURL, srURL)
	if err != nil {
		log.Fatal("Can not create producer: ", err)
	}

	personType := (&pb.Person{}).ProtoReflect().Type()
	addressType := (&pb.Address{}).ProtoReflect().Type()

	err = consumer.Run([]protoreflect.MessageType{personType, addressType}, topic)
	if err != nil {
		log.Println("ConsumerRun Error: ", err)
	}

}

// SRConsumer interface
type SRConsumer interface {
	Run(messagesType []protoreflect.MessageType, topic string) error
	Close()
}

type srConsumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
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

	d, err := protobuf.NewDeserializer(sr, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &srConsumer{
		consumer:     c,
		deserializer: d,
	}, nil
}

// RegisterMessageFactory will overwrite the default one
// In this case &pb.Person{} is the "msg" at "msg, err := c.deserializer.DeserializeRecordName()"
func (c *srConsumer) RegisterMessageFactory() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case subjectPerson:
			return &pb.Person{}, nil
		case subjectAddress:
			return &pb.Address{}, nil
		}
		return nil, errors.New("No matching receiver")
	}
}

// Run consumer
func (c *srConsumer) Run(messagesType []protoreflect.MessageType, topic string) error {
	if err := c.consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return err
	}

	if len(messagesType) > 0 {
		for _, mt := range messagesType {
			if err := c.deserializer.ProtoRegistry.RegisterMessage(mt); err != nil {

				return err
			}
		}
	}

	// register the MessageFactory is facultatif
	// but is it useful to allow the event receiver to be an initialized object
	c.deserializer.MessageFactory = c.RegisterMessageFactory()

	for {
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			return err
		}

		msg, err := c.deserializer.DeserializeRecordName(kafkaMsg.Value)
		if err != nil {
			return err
		}
		// without RegisterMessageFactory()
		// c.handleMessageAsInterface(msg, int64(kafkaMsg.TopicPartition.Offset))

		// with RegisterMessageFactory()
		if _, ok := msg.(*pb.Person); ok {
			fmt.Println("Person: ", msg.(*pb.Person).Name, " - ", msg.(*pb.Person).Age)
		} else {

			fmt.Println("Address: ", msg.(*pb.Address).City, " - ", msg.(*pb.Address).Street)
		}

		// // Deserialize into a struct
		// // receivers for DeserializeIntoRecordName
		// subjects := make(map[string]interface{})
		// subjects[subjectPerson] = person
		// subjects[subjectAddress] = address
		// err = c.deserializer.DeserializeIntoRecordName(subjects, kafkaMsg.Value)
		// if err != nil {
		// 	return err
		// }

		// fmt.Println("person: ", person.Name, " - ", person.Age)
		// fmt.Println("address: ", address.City, " - ", address.Street)

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
