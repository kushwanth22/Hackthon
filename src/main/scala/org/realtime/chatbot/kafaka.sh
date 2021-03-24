#!/bin/bash
echo "Starting Zookeeper Server"
zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties

echo "starting 2 brokers"
kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
kafka-server-start.sh -daemon /usr/local/kafka/config/server-1.properties

echo "Creating chatbot Topic"
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic chatbot

echo "listing Topic"
kafka-topics.sh --list --zookeeper localhost:2181

echo "Describing Topic"
kafka-topics.sh --describe --topic chatbot --zookeeper localhost:2181
