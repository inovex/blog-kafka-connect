FROM ubuntu:jammy as base

RUN apt-get update && apt-get install -y wget openjdk-18-jre gettext-base
WORKDIR /opt
RUN wget https://dlcdn.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz && tar xvf kafka_2.13-3.4.0.tgz
RUN mv kafka_2.13-3.4.0 kafka

FROM base as kafka
COPY server.properties kafka/config/kraft/server.properties
COPY entrypoint_kafka.sh kafka/entrypoint.sh
EXPOSE 9092 9093

ENTRYPOINT ["./kafka/entrypoint.sh"]

FROM base as connect
RUN mkdir -p kafka/connector
COPY build/libs/airquality-1.0-SNAPSHOT-all.jar kafka/connector
COPY source_connector_config.properties kafka/config/source_connector_config.properties
COPY sink_connector_config.properties kafka/config/sink_connector_config.properties
COPY connect-standalone.properties kafka/config/connect-standalone.properties
COPY entrypoint_connect.sh kafka/entrypoint.sh

ENTRYPOINT ["./kafka/entrypoint.sh"]
