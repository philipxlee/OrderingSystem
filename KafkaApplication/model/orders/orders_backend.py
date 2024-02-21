from confluent_kafka import Producer
import random
import config.config_manager as config

# Constants
ORDER_COUNT = 10

class orders_backend():
    
    def __init__(self):
        self.config = config.get_config()
        self.order_count = ORDER_COUNT
        self.orders = []
        self.topic = "order_details"

    def produce_orders(self):
        details = ["shoe, shirt, pants, hat, socks, gloves, jacket, tie, belt, watch"]
        for i in range(self.order_count):
            rand_index = random.randint(1, 10)
            self.orders.append((str(i), details[rand_index]))

    def produce_orders(self):
        producer = Producer(self.config)
        for order in self.orders:
            key, value = order[0], order[1]
            producer.produce(self.topic, key=key, value=value)
            print(f"Produced message to {self.topic}: key = {key:12} value = {value:12}")
            producer.flush()
    
    def get_topic(self):
        return self.topic








