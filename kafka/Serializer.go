package kafka

import (
	"fmt"
)

// Serializer is the interface implemented by types that can Serialize kafka.Message.
type Serializer interface {
	// Configure extracts the configuration values needed by the serializer returning the delta.
	Configure(configs ConfigMap, isKey bool) (ConfigMap, *Error)
	// Serialize encodes the Message [Key|Value] contents.
	// This is called per message so care should be taken to ensure this is not too expensive.
	Serialize(msg *Message) *Error
	// Close handles any necessary clean-up within Producer.Close().
	Close()
}

// Deserializer is the interface implemented by types that can deserialize kafka.Message.
type Deserializer interface {
	// Configure extracts the configuration value needed by the serializer and returns the delta.
	Configure(configs ConfigMap, isKey bool) (ConfigMap, *Error)
	// Deserialize decodes the Message contents in the Message.[Key|Value]Object.
	// This is called per message so care should be taken to ensure this is not too expensive.
	Deserialize(msg *Message) *Error
	// Close should handle any necessary clean-up; It will be called on Consumer.Close().
	Close()
}

// NewSerializationError returns a new SerializationError wrapping the Encoder exception if present.
// If err is not present(nil) nil a nil pointer is returned.
// See generated__errors.go for a complete list of Error codes and functions.
func NewSerializationError(err error, code ErrorCode) *Error {
	if err == nil {
		return nil
	}
	return &Error{
		code: code,
		str:  fmt.Sprintf("%s: %s", code.String(), err),
	}
}

// NewDeserializationError returns a new SerializationError wrapping the Decoder exception if present.
// If err is not present(nil) nil a nil pointer is returned.
// See generated__errors.go for a complete list of Error codes and functions.
func NewDeserializationError(err error, code ErrorCode) *Error {
	if err == nil {
		return nil
	}
	return &Error{
		code: code,
		str:  fmt.Sprintf("%s : %s", code.String(), err),
	}
}

// AbstractSerializer represents the bare-minimum necessary to satisfy the Serializer API.
type AbstractSerializer struct {
	IsKey bool
}

// Configure provides all of the configurations needed for the serializer from the global configuration object
func (as *AbstractSerializer) Configure(configs ConfigMap, isKey bool) (ConfigMap, *Error) {
	as.IsKey = isKey
	return configs, nil
}

// Serialize encodes Message [Key|Value] contents.
func (*AbstractSerializer) Serialize(msg *Message) *Error {
	return nil
}

// Deserialize decodes Message [Key|Value] contents.
func (*AbstractSerializer) Deserialize(msg *Message) *Error {
	return nil
}

// Close performs any required cleanup upon calling [Consumer|Producer].Close().
func (*AbstractSerializer) Close() {
	return
}
