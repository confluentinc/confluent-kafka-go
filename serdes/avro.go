package serdes

import (
	"encoding/json"
	"fmt"
)

func GetSubjectName(topic string, isKey bool) Subject {
	if isKey {
		return Subject(topic + "-key")
	}
	return Subject(topic + "-value")
}

func (c *client) NewSchema(schema string) (avroSchema Schema, err error) {
	if !json.Valid([]byte(schema)) {
		err = fmt.Errorf("Invalid JSON string provided, unable to create Schema object\n%s", schema)
	}
	return &schema, nil
}
