FROM golang:1.19 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "I am running on $BUILDPLATFORM, building for $TARGETPLATFORM"

WORKDIR /docker_spire_kafka_consumer_example

# Copy go files
COPY go.mod .
COPY go.sum .
COPY consumer.go .
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o consumer .

CMD ["./consumer"]