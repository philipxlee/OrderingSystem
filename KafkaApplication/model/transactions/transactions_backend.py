from confluent_kafka import Consumer


class transactions_backend:

    def __init__(self, config):
        self.config = config
        self.topic = "order_details"
        self.messages = []

    def consume_orders(self):
        self.config["group.id"] = "transactions-group-1"
        self.config["auto.offset.reset"] = "earliest"
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
                    print(
                        f"Consumed message from {self.topic}:\n \
                            key = {key}\n value = {value}\n\n"
                    )
        except KeyboardInterrupt:
            pass
        finally:
            # Closes the consumer connection
            consumer.close()
