#!/bin/sh

echo $KAFKA_HOST_NAME
echo "listeners=CONTROLLER://:19092,INTERNAL://:9092,EXTERNAL://:9093" >> kafka/config/kraft/server.properties
echo "advertised.listeners=INTERNAL://$KAFKA_HOST_NAME:9092,EXTERNAL://localhost:9093" >> kafka/config/kraft/server.properties
echo "listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT" >> kafka/config/kraft/server.properties
kafka/bin/kafka-storage.sh format -t $(kafka/bin/kafka-storage.sh random-uuid) -c kafka/config/kraft/server.properties


kafka/bin/kafka-server-start.sh kafka/config/kraft/server.properties
