from confluent_kafka import Consumer, Producer
import json

"""
This class is responsible for consuming orders from the order_details topic and 
producing confirmed orders to the order_confirmed topic.
"""


class transactions_backend:

    def __init__(self, config):
        """
        Initializes the transactions backend with the given configuration.

        Args:
            config (dict): The configuration dictionary for the Kafka client.
        """
        self.config = config
        self.receive_topic = "order_details"
        self.send_topic = "order_confirmed"
        self.messages = []
        self.confirmed_orders = []

    def consume_orders(self):
        """
        Consumes orders from the order_details topic and stores them in the messages list.
        """
        self.config["group.id"] = "transactions-group-1"
        self.config["auto.offset.reset"] = "earliest"
        consumer = Consumer(self.config)
        consumer.subscribe([self.receive_topic])
        try:
            while True:
                # Consumer polls the topic and prints any incoming messages
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    key = msg.key().decode("utf-8")
                    value = json.loads(msg.value().decode("utf-8"))
                    self.messages.append((key, value))
                    print(
                        f"Consumed message from {self.receive_topic}:\n \
                            key = {key}\n value = {value}\n\n"
                    )
        except KeyboardInterrupt:
            pass
        finally:
            # Closes the consumer connection
            consumer.close()

    def produce_orders(self):
        """
        Produces confirmed orders to the order_confirmed topic.
        """
        producer = Producer(self.config)
        for key, val in self.messages:
            order_data = val
            order_data.update({"order_status": "confirmed"})
            self.confirmed_orders.append((key, json.dumps(order_data)))

        for order in self.confirmed_orders:
            key, order_data = order[0], order[1]
            producer.produce(self.send_topic, key=key, value=order_data)
            producer.flush()
            print("Produced order confirmed to", {self.send_topic})
