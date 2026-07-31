package rest

import (
	"fmt"
	"strconv"
	"strings"
)

// Error represents a Schema Registry HTTP Error response
type Error struct {
	Code    int    `json:"error_code"`
	Message string `json:"message"`
	// Status is the HTTP status of the response. It is not part of the response
	// body, and is populated from the response itself, so that an error can be
	// classified even when the body carries no error code (for example a 404
	// produced by a proxy rather than by Schema Registry).
	Status int `json:"-"`
}

// Error implements the errors.Error interface
func (err *Error) Error() string {
	return fmt.Sprintf("schema registry request failed error code: %d: %s", err.Code, err.Message)
}

// HasStatus reports whether the error corresponds to the given HTTP status,
// either because the response carried that status, or because the Schema
// Registry error code refines it (error codes are the HTTP status optionally
// followed by two more digits, e.g. 40470 for a 404).
func (err *Error) HasStatus(status int) bool {
	if err.Status == status {
		return true
	}
	return strings.HasPrefix(strconv.Itoa(err.Code), strconv.Itoa(status))
}
