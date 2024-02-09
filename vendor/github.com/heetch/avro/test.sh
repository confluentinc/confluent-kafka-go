#!/bin/sh

set -ex
PKG=github.com/heetch/avro
go install $PKG/cmd/avrogo
go generate $PKG/...
go test $PKG/...
