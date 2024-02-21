from confluent_kafka import Consumer
from config import config_manager as config
from orders import orders_backend as orders

class transactions_backend():

    def __init__(self):
        self.config = config.get_config()
        self.topic = orders.get_topic()
        self.messages = []
    
    def consume_orders(self):
        # Sets the consumer group ID and offset  
        self.config["group.id"] = "python-group-1"
        self.config["auto.offset.reset"] = "earliest"

        # Creates a new consumer and subscribes to your topic
        consumer = Consumer(self.config)
        consumer.subscribe([self.topic])
        try:
            while True:
                # Consumer polls the topic and prints any incoming messages
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    key = msg.key().decode("utf-8")
                    value = msg.value().decode("utf-8")
                    self.messages.append((key, value))
                    print(f"Consumed message from {self.topic}: \
                            key = {key:12} value = {value:12}")
        except KeyboardInterrupt:
            pass
        finally:
            # Closes the consumer connection
            consumer.close()
    





        

