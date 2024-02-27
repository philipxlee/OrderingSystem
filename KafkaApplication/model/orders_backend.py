from confluent_kafka import Producer
from .order_detail import OrderDetail, OrderEmail
import random
import json

"""
This class is responsible for creating orders and producing them to the order_details 
topic. It also provides a method to get the topic name for the orders.
"""


class orders_backend:

    # Constants
    ORDER_COUNT = 10

    def __init__(self, config):
        """
        Initializes the orders backend with the given configuration.

        Args:
            config (dict): The configuration dictionary for the Kafka client.
        """
        self.config = config
        self.order_count = self.ORDER_COUNT
        self.orders = []
        self.topic = "order_details"

    def create_orders(self):
        """
        Creates orders and stores them in the orders list.
        """
        details = [order_detail.value for order_detail in OrderDetail]
        emails = [order_email.value for order_email in OrderEmail]
        for i in range(self.order_count):
            rand_index = random.randint(0, len(details) - 1)
            order_detail = details[rand_index]
            revenue = random.randint(1, 300)
            order_id = "order" + " " + str(i + 1)
            order_data = {
                "item_purchased": order_detail,
                "revenue": str(revenue),
                "customer_email": emails[i % len(emails)],
            }
            self.orders.append((order_id, json.dumps(order_data)))

    def produce_orders(self):
        """
        Produces orders to the order_details topic.
        """
        producer = Producer(self.config)
        for order in self.orders:
            key, value = order[0], order[1]
            producer.produce(self.topic, key=key, value=value)
            print(
                f"Produced message to {self.topic}:\n key = {key}\n value = {value}\n\n"
            )
            producer.flush()

    def get_topic(self):
        """
        Gets the topic name for the orders.

        Returns:
            str: The topic name for the orders.
        """
        return self.topic
