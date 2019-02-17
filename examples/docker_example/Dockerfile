# build stage
FROM golang as builder 

# librdkafka Build from source

RUN git clone https://github.com/edenhill/librdkafka.git

WORKDIR librdkafka

RUN ./configure --prefix /usr

RUN make

RUN make install

# Build go binary 

ENV GO111MODULE=on

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go build -o main

# final stage
FROM ubuntu

COPY --from=builder /usr/lib/pkgconfig /usr/lib/pkgconfig
COPY --from=builder /usr/lib/librdkafka* /usr/lib/
COPY --from=builder /app/main main

CMD ["./main"]


# to build 
# docker build -t {{tag_name}} .

# to run 
# docker run {{tag_name}}