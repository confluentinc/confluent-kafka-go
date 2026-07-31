package rest

import (
	"fmt"
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

// HasStatus reports whether the error corresponds to the given HTTP status.
//
// The status of the response is authoritative when it is known, so that a body
// whose error code disagrees with it (for example an upstream 404 body returned
// by a proxy as a 502) is not classified by the error code.
func (err *Error) HasStatus(status int) bool {
	if err.Status != 0 {
		return err.Status == status
	}

	// The error was not built from a response, so fall back to the error code,
	// which is either the HTTP status itself or the status followed by two more
	// digits (e.g. 40470 for a 404).
	if err.Code == status {
		return true
	}
	return err.Code >= 10000 && err.Code/100 == status
}
