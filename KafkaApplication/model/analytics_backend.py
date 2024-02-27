from collections import defaultdict
from confluent_kafka import Consumer
import json

"""
Analytics backend class is responsible for consuming orders from the order_confirmed topic
and calculating analytics.
"""


class analytics_backend:

    def __init__(self, config):
        """
        Initializes the analytics backend with the given configuration.
        """
        self.config = config
        self.receive_topic = "order_confirmed"
        self.total_revenue = 0
        self.total_orders = 0
        self.frequently_purchased = defaultdict(int)

    def consume_orders(self):
        """
        Consumes orders from the order_confirmed topic and calculates analytics.
        """
        # Sets the consumer group ID and offset
        self.config["group.id"] = "analytics-group-1"
        self.config["auto.offset.reset"] = "earliest"
        consumer = Consumer(self.config)
        consumer.subscribe([self.receive_topic])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    value = json.loads(msg.value().decode("utf-8"))
                    print("Listening to orders and calculating analytics...")
                    # Build analytics
                    self.total_orders += 1
                    self.total_revenue += int(value["revenue"])
                    self.frequently_purchased[value["item_purchased"]] += 1

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    def print_analytics(self):
        """
        Prints the calculated analytics.
        """
        print("Total Orders: ", self.total_orders)
        print("Total Revenue: ", self.total_revenue)
        print("Frequently Purchased Items: ", self.frequently_purchased)
