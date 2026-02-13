#!/bin/bash
#

# Per-instance Go kafkatest client dependency deployment.
# Installs required dependencies.

#sudo apt-get install -y libsasl2 libsasl2-modules-gssapi-mit libssl1.1.0 liblz4-1
#sudo apt-get install -y golang

wget https://go.dev/dl/go1.24.3.linux-arm64.tar.gz
sudo tar -C /usr/local -xzf go1.24.3.linux-arm64.tar.gz
rm go1.24.3.linux-arm64.tar.gz
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc

cd /opt/kafka-dev/tests/kafkatest/go/go_verifiable_producer/
go build -tags static

cd /opt/kafka-dev/tests/kafkatest/go/go_verifiable_consumer/
go build -tags static
