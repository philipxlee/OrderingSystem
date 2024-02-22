from config import read_config as cfg
from model.orders import orders_backend as ord
from model.transactions import transactions_backend as trn
from model.analytics import analytics_backend as anl


class control:

    def __init__(self):
        self.kafka_client = self.initialize_config()

    def initialize_config(self):
        # Set up the client configuration
        client = cfg.read_config()
        return client.get_config()

    def run_orders_backend(self, kafka_client):
        # Set up orders backend and produce orders
        orders = ord.orders_backend(kafka_client)
        orders.create_orders()
        orders.produce_orders()

    def run_transactions_backend(self, kafka_client):
        # Set up transactions backend and consume orders
        transactions = trn.transactions_backend(kafka_client)
        transactions.consume_orders()

    def run_analytics_backend(self, kafka_client):
        # Set up analytics backend and consume orders
        analytics = anl.analytics_backend(kafka_client)
        analytics.consume_orders()
        analytics.print_analytics()

    def run_program(self):
        print("Running orders backend")
        self.run_orders_backend(self.kafka_client)
        print("Running transactions backend")
        self.run_transactions_backend(self.kafka_client)
        print("Running analytics backend")
        self.run_analytics_backend(self.kafka_client)


def main():
    controller = control()
    controller.run_program()


if __name__ == "__main__":
    main()
