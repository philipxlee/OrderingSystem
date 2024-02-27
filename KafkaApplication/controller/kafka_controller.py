from model.orders_backend import orders_backend
from model.transactions_backend import transactions_backend
from model.analytics_backend import analytics_backend
from model.emails_backend import emails_backend
from config.read_config import read_config

"""
kafka_controller class is responsible for initializing the Kafka client configuration
and setting up the orders, transactions, analytics, and emails backends. It also runs
the program by calling the methods to produce and consume messages for each backend.
"""


class kafka_controller:

    def __init__(self):
        """
        Initializes the kafka_controller by setting up the Kafka client configuration and
        the orders, transactions, analytics, and emails backends.
        """
        self.kafka_client = self.__initialize_config()
        self.orders = orders_backend(self.kafka_client)
        self.transactions = transactions_backend(self.kafka_client)
        self.analytics = analytics_backend(self.kafka_client)
        self.emails = emails_backend(self.kafka_client)

    def run_program(self):
        """
        Runs the program by calling the methods to produce and consume messages for each
        backend.
        """
        print("Running orders backend")
        self.__run_orders_backend()
        print("Running transactions backend")
        self.__run_transactions_backend()
        print("Running analytics backend")
        self.__run_analytics_backend()
        print("Running email backend")
        self.__run_email_backend()

    def __initialize_config(self):
        # Set up the client configuration
        client = read_config()
        return client.get_config()

    def __run_orders_backend(self):
        # Set up orders backend and produce orders
        self.orders.create_orders()
        self.orders.produce_orders()

    def __run_transactions_backend(self):
        # Set up transactions backend and consume orders
        self.transactions.consume_orders()
        self.transactions.produce_orders()

    def __run_analytics_backend(self):
        # Set up analytics backend and consume orders
        self.analytics.consume_orders()
        self.analytics.print_analytics()

    def __run_email_backend(self):
        # Set up email backend and consume orders
        self.emails.consume_orders()
        self.emails.send_emails()
