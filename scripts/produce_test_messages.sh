#!/usr/bin/env bash
# Produces a stream of test messages into a Kafka topic so you can watch lazykafka-rs tail live.
# Usage: scripts/produce_test_messages.sh [topic] [count]
set -euo pipefail

TOPIC="${1:-test-rust}"
COUNT="${2:-50000}"
CONTAINER="lazykafka-rs-kafka-1"

echo "Producing $COUNT messages to '$TOPIC' as fast as possible..."
time seq 1 "$COUNT" | awk '{print "msg-" $1 " @ " strftime("%Y-%m-%dT%H:%M:%S")}' \
  | docker exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:19092 --topic "$TOPIC" \
      --producer-property batch.size=65536 \
      --producer-property linger.ms=5
echo "done"
