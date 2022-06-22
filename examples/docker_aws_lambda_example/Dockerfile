FROM fedora:latest

# Install pre-reqs
RUN dnf install wget -y
RUN dnf install gcc -y

RUN rpm --import https://packages.confluent.io/rpm/5.4/archive.key

# Install Go v1.14
RUN wget https://dl.google.com/go/go1.14.linux-amd64.tar.gz && tar -xvzf go1.14.linux-amd64.tar.gz && rm go1.14.linux-amd64.tar.gz
RUN mv go /usr/local
ENV GOROOT=/usr/local/go
ENV PATH="${GOROOT}/bin:${PATH}"

# Build the producer_example
WORKDIR /kafka
COPY examples/docker_aws_lambda_example/go.mod .
COPY examples/docker_aws_lambda_example/producer_example.go .
RUN go build -o producer_example .

ENTRYPOINT ["./producer_example"]
