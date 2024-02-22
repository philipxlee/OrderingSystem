from confluent_kafka import Producer
from .order_detail import OrderDetail, OrderEmail
import random
import json

# Constants
ORDER_COUNT = 10


class orders_backend:

    def __init__(self, config):
        self.config = config
        self.order_count = ORDER_COUNT
        self.orders = []
        self.topic = "order_details"

    def create_orders(self):
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
        producer = Producer(self.config)
        for order in self.orders:
            key, value = order[0], order[1]
            producer.produce(self.topic, key=key, value=value)
            print(
                f"Produced message to {self.topic}:\n key = {key}\n value = {value}\n\n"
            )
            producer.flush()

    def get_topic(self):
        return self.topic
