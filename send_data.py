import random
import time
from kafka import KafkaProducer
import json
from datetime import datetime

unique_id = "my_name"
sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = f"building_sensors_{unique_id}"

while True:
    temperature = random.uniform(25, 45)
    humidity = random.uniform(15, 85)
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": temperature,
        "humidity": humidity
    }
    producer.send(topic, value=data)
    print(f"Sent data: {data}")
    time.sleep(2)
