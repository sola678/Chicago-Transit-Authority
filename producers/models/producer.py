"""Producer base-class providing common utilites and functionality"""
import logging
import time
import asyncio

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


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
        BROKER_URL="PLAINTEXT://localhost:9092"
        SCHEMA_REGISTRY_URL="http://localhost:8081"
        self.broker_properties = {
            "schema.registry.url":"http://localhost:8081",
            "bootstrap.servers":"PLAINTEXT://localhost:9092"
            
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        p=AvroProducer(self.broker_properties, default_key_schema=key_schema, default_value_schema=value_schema
         )
        p.produce(
            topic=topic_name,
            
            value_schema=value_schema,
            
            key_schema=key_schema
        
        )
        p.flush()
    #def topic_exists(self, client, topic_name):
        #topic_metadata=client.list_topics(timeout=5)
        #return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))
    
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #global BROKER_URL="PLAINTEXT://localhost:9092"
        #global SCHEMA_REGISTRY_URL="http://localhost:8081"       
        client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
        topic_name=self.topic_name
        #exists=self.topic_exists(client, topic_name)
        #print(f"Topic{topic_name} exists:{exists}")
        #if exists is False:
        if self.topic_name not in Producer.existing_topics:
            futures = client.create_topics(
        [NewTopic(topic=topic_name, num_partitions=5, replication_factor=1)]
    )
            for topic, future in futures.items():
                try:
                    future.result()
                    print("topic created")
                except Exception as e:
                    print(f"failed to create topic {topic_name}:{e}")

        
        #try:
            #asyncio.run(producer(self))
        #except KeyboardInterrupt as e:
            #print("shutting down")
        logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        self.flush();
        self.close();
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
