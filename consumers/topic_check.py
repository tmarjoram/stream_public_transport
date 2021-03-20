from confluent_kafka.admin import AdminClient

HOSTNAME = 'DESKTOP-MRAIJ7J'
def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": f"PLAINTEXT://{HOSTNAME}:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
