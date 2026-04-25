# CryptoStream-Pipeline
A scalable end-to-end data engineering pipeline for real-time cryptocurrency analytics

Requirements:
pip install kafka-python websocket-client requests

Run Kafka first:
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

