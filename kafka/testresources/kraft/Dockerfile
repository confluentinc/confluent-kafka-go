FROM debian:bookworm

RUN apt update
RUN apt install -y git curl
RUN apt install -y openjdk-17-jdk

RUN git clone https://github.com/apache/kafka.git

RUN cd kafka && ./gradlew jar --info -x test -x checkstyleMain \
    -x checkstyleTest -x spotbugsMain -xspotbugsTest

RUN mkdir /logs

WORKDIR /kafka
ENTRYPOINT ./bin/kafka-storage.sh format -t $(./bin/kafka-storage.sh random-uuid) \
    -c /etc/kafka/server.properties && \
    ./bin/kafka-server-start.sh /etc/kafka/server.properties