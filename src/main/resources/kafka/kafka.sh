#!/usr/bin bash

bin/kafka-server-start.sh config/server.properties # start kafka server

# create input topic
bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-plaintext-input

# create output topic
bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-wordcount-output \
    --config cleanup.policy=compact

bin/kafka-topics.sh --zookeeper localhost:2181 --describe # check topic description

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input #write input data to topic

# inspect output
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# checks records
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mytest --from-beginning
