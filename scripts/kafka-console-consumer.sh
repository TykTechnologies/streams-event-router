echo "Consuming Kafka topic using Kakfa CLI... (CTRL+C to quit)"

docker exec -it streams-event-router-kafka-1 /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic high-priority-topic --from-beginning