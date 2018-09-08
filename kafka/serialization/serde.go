package serialization

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Serializer interface{
	Configure(configs map[string]interface{}) error
	Serialize(topic string, data []byte) (interface{}, error)
	Close()
}

type Deserializer interface{
	Configure(configs map[string]interface{}) error
	Deserialize(topic string, data []byte) (interface{}, error)
	Close()
}

type ExtendedSerializer interface{
	Configure(configs map[string]interface{}) error
	Serialize(topic string, header *kafka.Header, data []byte) (interface{}, error)
	Close()
}

type ExtendedDeserializer interface{
	Configure(configs map[string]interface{}) error
	Deserialize(topic string, header *kafka.Header, data []byte) (interface{}, error)
	Close()
}