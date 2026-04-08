#!/bin/bash

TOPIC_NAME="conorder"
BOOTSTRAP_SERVER="localhost:9092"
KAFKA_CONTAINER="kafka-1"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"

echo "Cleaning kafka topic: $TOPIC_NAME ..."
docker exec -it $KAFKA_CONTAINER $KAFKA_BIN \
    --delete \
    --topic $TOPIC_NAME \
    --bootstrap-server $BOOTSTRAP_SERVER

sleep 2

docker exec -it $KAFKA_CONTAINER $KAFKA_BIN \
    --create \
    --topic $TOPIC_NAME \
    --partitions 3 \
    --replication-factor 2 \
    --bootstrap-server $BOOTSTRAP_SERVER

echo "Done."