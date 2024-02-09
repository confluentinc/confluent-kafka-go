package parser

import (
	"fmt"
)

type SchemaError struct {
	FieldName   string
	NestedError error
}

func NewSchemaError(fieldName string, err error) *SchemaError {
	fullName := fieldName
	nestedErr := err
	if schemaErr, ok := err.(*SchemaError); ok {
		fullName = fieldName + "." + schemaErr.FieldName
		nestedErr = schemaErr.NestedError
	}
	return &SchemaError{
		FieldName:   fullName,
		NestedError: nestedErr,
	}
}

func (s *SchemaError) Error() string {
	return fmt.Sprintf("Error parsing schema for field %q: %v", s.FieldName, s.NestedError)
}

type WrongMapValueTypeError struct {
	Key          string
	ExpectedType string
	ActualValue  interface{}
}

func NewWrongMapValueTypeError(key, expectedType string, actualValue interface{}) *WrongMapValueTypeError {
	return &WrongMapValueTypeError{
		Key:          key,
		ExpectedType: expectedType,
		ActualValue:  actualValue,
	}
}

func (w *WrongMapValueTypeError) Error() string {
	return fmt.Sprintf("Wrong type for map key %q: expected type %v, got value %q of type %t", w.Key, w.ExpectedType, w.ActualValue, w.ActualValue)
}

type RequiredMapKeyError struct {
	Key string
}

func NewRequiredMapKeyError(key string) *RequiredMapKeyError {
	return &RequiredMapKeyError{
		Key: key,
	}
}

func (r *RequiredMapKeyError) Error() string {
	return fmt.Sprintf("No value supplied for required map key %q", r.Key)
}
