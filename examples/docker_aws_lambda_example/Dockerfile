FROM fedora:latest
ARG GOVERSION=1.25.5
ARG TARGETARCH
# Install pre-reqs
RUN dnf install wget -y
RUN dnf install gcc -y

RUN rpm --import https://packages.confluent.io/rpm/5.4/archive.key

# Install Go
RUN wget https://dl.google.com/go/go${GOVERSION}.linux-${TARGETARCH}.tar.gz && tar -xvzf go${GOVERSION}.linux-${TARGETARCH}.tar.gz && rm go${GOVERSION}.linux-${TARGETARCH}.tar.gz
RUN mv go /usr/local
ENV GOROOT=/usr/local/go
ENV PATH="${GOROOT}/bin:${PATH}"

# Build the producer_example
WORKDIR /kafka
COPY examples/docker_aws_lambda_example/go.mod .
COPY examples/docker_aws_lambda_example/producer_example.go .
RUN go mod download && go mod tidy
RUN go build -o producer_example .

ENTRYPOINT ["./producer_example"]