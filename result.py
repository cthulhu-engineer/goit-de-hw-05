from kafka import KafkaConsumer
import json

unique_id = "my_name"
temperature_topic = f"temperature_alerts_{unique_id}"
humidity_topic = f"humidity_alerts_{unique_id}"

consumer = KafkaConsumer(
    temperature_topic,
    humidity_topic,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    print(f"Received alert: {message.value}")
