from collections import defaultdict
from confluent_kafka import Consumer
import json


class analytics_backend:

    def __init__(self, config):
        self.config = config
        self.topic = "order_details"
        self.total_revenue = 0
        self.total_orders = 0
        self.frequently_purchased = defaultdict(int)

    def consume_orders(self):
        # Sets the consumer group ID and offset
        self.config["group.id"] = "analytics-group-1"
        self.config["auto.offset.reset"] = "earliest"
        consumer = Consumer(self.config)
        consumer.subscribe([self.topic])
        try:
            while True:

                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    key = msg.key().decode("utf-8")
                    value = json.loads(msg.value().decode("utf-8"))

                print(value)
                # Build analytics
                self.total_orders += 1
                self.total_revenue += int(value["revenue"])
                self.frequently_purchased[value["item_purchased"]] += 1

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    def print_analytics(self):
        print("Total Orders: ", self.total_orders)
        print("Total Revenue: ", self.total_revenue)
        print("Frequently Purchased Items: ", self.frequently_purchased)
