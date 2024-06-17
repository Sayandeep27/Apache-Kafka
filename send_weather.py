from confluent_kafka import Producer
import json
import time
import random

# Kafka broker
bootstrap_servers = 'localhost:9092'
topic = 'weather_topic'

# Kafka producer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
}

# Create Kafka producer
producer = Producer(conf)

# Function to generate random weather data
def generate_weather_data():
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    weather_conditions = ['Sunny', 'Cloudy', 'Rainy', 'Snowy']
    city = random.choice(cities)
    condition = random.choice(weather_conditions)
    temperature = round(random.uniform(-20, 40), 2)  # Random temperature between -20 and 40 Celsius
    return {'city': city, 'condition': condition, 'temperature': temperature}

# Function to send weather updates to Kafka
def produce_weather_updates():
    while True:
        weather_data = generate_weather_data()
        # Serialize data to JSON
        data_str = json.dumps(weather_data)
        # Produce data to Kafka topic
        producer.produce(topic, value=data_str.encode('utf-8'))
        print(f"Produced weather update: {weather_data}")
        time.sleep(random.randint(1, 5))  # Simulate random delay
        producer.flush()  # Ensure all messages are sent

# Run the producer
if __name__ == '__main__':
    produce_weather_updates()
