#!/bin/bash

# This is an interactive script that might be used to generate the secrets in case
# they expire. It doesn't matter what the values of the secrets are, they are
# used only for testing.
# openssl is a prerequisite for this script.
# rootCA.crt.malformed does not need to be regenerated.

openssl genrsa -out rootCA.key 4096
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 4096 -out rootCA.crt
