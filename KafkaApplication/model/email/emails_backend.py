from confluent_kafka import Consumer
import json


class emails_backend:

    def __init__(self, config):
        self.config = config
        self.receive_topic = "order_confirmed"
        self.emails = []

    def consume_orders(self):
        self.config["group.id"] = "emails-group-1"
        self.config["auto.offset.reset"] = "earliest"
        consumer = Consumer(self.config)
        consumer.subscribe([self.receive_topic])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    value = json.loads(msg.value().decode("utf-8"))
                    print("Listening to orders and processing emails...")
                    customer_email = value["customer_email"]
                    customer_purchase = value["item_purchased"]
                    customer_order_confirmed = value["order_status"]
                    self.emails.append(
                        (
                            "Thank you for purchasing the " + customer_purchase + "!",
                            customer_email
                            + ". Your order has been "
                            + customer_order_confirmed
                            + "!",
                        )
                    )
        except KeyboardInterrupt:
            pass
        finally:
            # Closes the consumer connection
            consumer.close()

    def send_emails(self):
        for email in self.emails:
            print(f"Sent email to {email[1]}:\n {email[0]}\n\n")
        print("All emails sent!")
