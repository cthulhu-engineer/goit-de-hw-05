from kafka.admin import KafkaAdminClient, NewTopic

unique_id = "my_name"

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="admin"
)

topics = [
    NewTopic(name=f"building_sensors_{unique_id}", num_partitions=1, replication_factor=1),
    NewTopic(name=f"temperature_alerts_{unique_id}", num_partitions=1, replication_factor=1),
    NewTopic(name=f"humidity_alerts_{unique_id}", num_partitions=1, replication_factor=1)
]

admin_client.create_topics(new_topics=topics, validate_only=False)

print([topic for topic in admin_client.list_topics() if unique_id in topic])
