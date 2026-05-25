FROM debian:bookworm

RUN apt update
RUN apt install -y git curl
RUN apt install -y openjdk-17-jdk

# Trunk at 2024-04-10, replace with 3.8.0 tag when available
RUN git clone --single-branch --branch trunk \
    https://github.com/apache/kafka.git && \
    (cd /kafka  && git checkout f6c9feea76d01a46319b0ca602d70aa855057b07)

RUN cd kafka && ./gradlew jar --info -x test -x checkstyleMain \
    -x checkstyleTest -x spotbugsMain -xspotbugsTest

RUN mkdir /logs

WORKDIR /kafka
ENTRYPOINT ./bin/kafka-storage.sh format -t $(./bin/kafka-storage.sh random-uuid) \
    -c /etc/kafka/server.properties && \
    ./bin/kafka-server-start.sh /etc/kafka/server.properties