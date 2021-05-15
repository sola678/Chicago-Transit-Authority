"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        BROKER_URL="PLAINTEXT://localhost:9092"
        SCHEMA_REGISTRY_URL="http://localhost:8081"
        
        self.broker_properties = {
            "schema.registry.url" : "http://localhost:8081",
            "broker.url" : "PLAINTEXT://localhost:9092"
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            consumer = AvroConsumer(
                {"bootstrap.servers": self.broker_properties["broker.url"]},schema_registry=self.broker_properties["schema.registry.url"]
                )
        else:
            consumer = Consumer({"group.id":"0" , "bootstrap.servers": "PLAINTEXT://localhost:9092", "enable.auto.commit" : True, "default.topic.config":{"auto.offset.reset": "earliest"}, "enable.auto.offset.store": True})
        consumer.subscribe([topic_name_pattern], on_assign=on_assign)    
        messages=consumer.poll(1, timeout=consume_timeout)
        for message in messages:
            if message is None:
                continue
            elif message.error() is not None:
                continue
            else:
                print(message.key(), message.value())
        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
         #self.consumer.subscribe(topic_name_pattern=self.topic_name_pattern#, on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        
        #partition_offset = earliest
        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            partition.offset = "earliest"
        print(f"Assign: {partitions}")
        consumer.assign(partitions)
            #
            #
            # TODO
            #
            #

        logger.info("partitions assigned for %s", self)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        consumer.subscribe(self, on_assign=self.on_assign)
        while True:
            msg = consumer.poll(timeout=1.0)
            for m in msg:
                if m is None:
                    return 0
                    continue
                elif m.error() is not None:
                    continue
                else:
                    print(m.key(), m.value())
                    return 1
        
        logger.info("_consume is incomplete - skipping")
        #return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
            finally:
                consumer.close();
                self.close()
            
        

        #
