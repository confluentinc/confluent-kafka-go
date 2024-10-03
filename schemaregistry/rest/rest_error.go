package rest

import (
	"fmt"
)

// Error represents a Schema Registry HTTP Error response
type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

// Error implements the errors.Error interface
func (err *Error) Error() string {
	return fmt.Sprintf("schema registry request failed error code: %d: %s", err.Code, err.Message)
}
