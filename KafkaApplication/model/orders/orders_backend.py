from confluent_kafka import Producer
import random
import json

# Constants
ORDER_COUNT = 10

class orders_backend():
    
    def __init__(self, config):
        self.config = config
        self.order_count = ORDER_COUNT
        self.orders = []
        self.topic = "order_details"

    def create_orders(self):
        details = ["shoe, shirt, pants, hat, socks, gloves, jacket, tie, belt, watch"]
        for i in range(self.order_count):
            rand_index = random.randint(0, len(details) - 1)
            order_detail = details[rand_index]
            revenue = random.randint(1, 1000)
            order_id = str(i)
            order_data = {
                "order_detail": order_detail,
                "revenue": str(revenue)
            }
            self.orders.append((order_id, json.dumps(order_data))) 

    def produce_orders(self):
        producer = Producer(self.config)
        for order in self.orders:
            key, value = order[0], order[1]
            producer.produce(self.topic, key=key, value=value)
            print(f"Produced message to {self.topic}: key = {key:12} value = {value:12}")
            producer.flush()
    
    def get_topic(self):
        return self.topic








