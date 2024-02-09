#!/bin/sh
# show coverage of the base Avro package with respect to all generated
# test packages.

f=$(mktemp)
go test -coverpkg ./... -coverprofile $f ./... && go tool cover -html $f
rm $f
