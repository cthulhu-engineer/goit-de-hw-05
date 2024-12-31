from kafka import KafkaConsumer, KafkaProducer
import json

unique_id = "my_name"
input_topic = f"building_sensors_{unique_id}"
temperature_topic = f"temperature_alerts_{unique_id}"
humidity_topic = f"humidity_alerts_{unique_id}"

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    data = message.value
    if data["temperature"] > 40:
        alert = {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "temperature": data["temperature"],
            "alert": "Temperature exceeds 40°C!"
        }
        producer.send(temperature_topic, value=alert)
        print(f"Temperature alert sent: {alert}")

    if data["humidity"] > 80 or data["humidity"] < 20:
        alert = {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "humidity": data["humidity"],
            "alert": "Humidity out of range (20-80%)!"
        }
        producer.send(humidity_topic, value=alert)
        print(f"Humidity alert sent: {alert}")
