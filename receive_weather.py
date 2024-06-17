from confluent_kafka import Consumer, KafkaException
import json

# Kafka broker
bootstrap_servers = 'localhost:9092'
topic = 'weather_topic'
group_id = 'weather_group'

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer
consumer = Consumer(conf)

# Subscribe to Kafka topic
consumer.subscribe([topic])

# Function to consume and process weather updates
def consume_weather_updates():
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        # Decode and process message value
        weather_data = json.loads(msg.value().decode('utf-8'))
        print(f"Received weather update: {weather_data}")

# Run the consumer
if __name__ == '__main__':
    consume_weather_updates()
