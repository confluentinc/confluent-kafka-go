/*
Package avro provides encoding and decoding for the Avro binary data format.

The format uses out-of-band schemas to determine the encoding,
with a schema migration scheme to allow data written with
one schema to be read using another schema.

See here for more information on the format:

https://avro.apache.org/docs/1.9.1/spec.html

This package provides a mapping from regular Go types
to Avro schemas. See the TypeOf function for more details.

There is also a code generation tool that can generate
Go data structures from Avro schemas.
See https://pkg.go.dev/github.com/heetch/avro/cmd/avrogo
for details.
*/
package avro
