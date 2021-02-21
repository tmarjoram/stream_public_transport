"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


HOSTNAME = 'DESKTOP-MRAIJ7J'
BROKER_URL = f"PLAINTEXT://{HOSTNAME}:9092"
SCHEMA_REGISTRY_URL = f"http://{HOSTNAME}:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': BROKER_URL,
            # TODO
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties,
                                     schema_registry=  CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
        )

    @staticmethod
    def topic_exists(client, topic_name):

        topics = client.list_topics()

        return topic_name in topics.topics

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": BROKER_URL})

        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        if not self.topic_exists(client, self.topic_name) :

            """Creates the topic with the given topic name"""
            # Create the topic. Make sure to set the topic name, the number of partitions, the
            # replication factor. Additionally, set the config to have a cleanup policy of delete, a
            # compression type of lz4, delete retention milliseconds of 2 seconds, and a file delete delay
            # milliseconds of 2 second.
            #
            # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
            # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
            futures = client.create_topics(
                [
                    # SEE http://kafka.apache.org/documentation.html#adminapi
                    # NewTopic(topic=topic_name,
                    #         num_partitions=5,
                    #         replication_factor=1,
                    #         config={
                    #             "cleanup.policy":"compact",
                    #             "compression.type":"lz4",
                    #             "delete.retention.ms":1*1000,
                    #             "file.delete.delay.ms":1*1000
                    #         }
                    # )
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas,
                        config={
                            # TODO TM What to set here ? ****
                            #  *** compact doesn't work - presume because no key **
                            "cleanup.policy": "delete",
                            "compression.type": "lz4",
                            "delete.retention.ms": "2000",
                            "file.delete.delay.ms": "2000",
                        },
                    )

                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic {self.topic_name} created")
                except Exception as e:
                    logger.fatal(f"failed to create topic {self.topic_name}: {e}")
                    raise

        # logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        # TODO TM Anything else ?  ****
        self.producer.flush(30)


        # logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
