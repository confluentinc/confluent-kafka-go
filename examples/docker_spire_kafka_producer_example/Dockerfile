#FROM golang:1.19-alpine AS builder
#
#ENV PATH="/go/bin:${PATH}"
#ENV GO111MODULE=on
#ENV CGO_ENABLED=1
#ENV GOOS=linux
#ENV GOARCH=amd64
#
#WORKDIR /docker_spire_kafka_consumer_example
#
#COPY go.mod .
#COPY go.sum .
#RUN go mod download
#
#RUN apk -U add ca-certificates
#RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo
#RUN git clone https://github.com/edenhill/librdkafka.git && cd librdkafka && ./configure --prefix /usr && make && make install
#
#COPY . .
#
##RUN go build -tags musl --ldflags "-extldflags -static" -o /go/src/github.com/confluentinc/confluent-kafka-go/examples/docker_spire_consumer_example/main .
##RUN go build -tags musl --ldflags "-extldflags -static" -o main .
#RUN go build -tags musl --ldflags "-extldflags -static" -o /go/src/github.com/confluentinc/confluent-kafka-go/examples/docker_spire_kafka_consumer_example/docker_spire_consumer_example/docker_spire_consumer_example/main .
#
#FROM scratch AS runner
#
#COPY --from=builder /go/src/github.com/confluentinc/confluent-kafka-go/examples/docker_spire_kafka_consumer_example/docker_spire_consumer_example/docker_spire_consumer_example/main /
#
#EXPOSE 8080
#
#ENTRYPOINT ["./main"]

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
WORKDIR /docker_spire_kafka_producer_example
COPY go.mod .
COPY go.sum .
COPY producer.go .
#ADD . tcp://127.0.0.1:31523


# Build the producer_example
RUN go build -o producer .

#ENTRYPOINT ["./producer"]
CMD ["go", "run", "producer.go"]
#COPY consumer.go .
#RUN go build -o consumer .
#
#ENTRYPOINT ["./consumer"]

#FROM golang:1.19-alpine AS builder
#
#RUN apk add --update --no-cache alpine-sdk bash ca-certificates \
#      libressl \
#      tar \
#      git openssh openssl yajl-dev zlib-dev cyrus-sasl-dev openssl-dev build-base coreutils
#WORKDIR /root
#RUN git clone https://github.com/edenhill/librdkafka.git
#WORKDIR /root/librdkafka
#RUN /root/librdkafka/configure
#RUN make
#RUN make install
##For golang applications
#RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
#
#ENV docker_spire_kafka_consumer_example "organization/repository"
#WORKDIR /go/src/github.com/docker_spire_kafka_consumer_example
#
##RUN go get -d -v github.com/confluentinc/confluent-kafka-go
#
#COPY . .
#RUN GOOS=linux go build -a -installsuffix cgo -o producer .
#RUN GOOS=linux go build -a -installsuffix cgo -o producer .
#
#ENV PORT 4000
#EXPOSE ${PORT}
#COPY producer.go .
#CMD ["go", "run", "producer.go"]
#COPY consumer.go .
#CMD ["go", "run", "consumer.go"]
##CMD ["./app"]

