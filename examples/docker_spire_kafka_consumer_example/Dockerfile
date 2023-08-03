FROM fedora:latest AS builder

# Install pre-reqs
RUN dnf install wget -y
RUN dnf install gcc -y

RUN rpm --import https://packages.confluent.io/rpm/5.4/archive.key

# Install Go v1.19
RUN wget https://dl.google.com/go/go1.19.linux-amd64.tar.gz && tar -xvzf go1.19.linux-amd64.tar.gz && rm go1.19.linux-amd64.tar.gz
RUN mv go /usr/local
ENV GOROOT=/usr/local/go
ENV PATH="${GOROOT}/bin:${PATH}"
WORKDIR /docker_spire_kafka_consumer_example
COPY go.mod .
COPY go.sum .
COPY consumer.go .

RUN go build -o consumer .
CMD ["go", "run", "consumer.go"]