from config import read_config as cfg
from model.orders import orders_backend as ord
from model.transactions import transactions_backend as trn

def initialize_config():
    # Set up the client configuration
    control = cfg.read_config()
    return control.get_config()

def run_orders_backend(config):
    # Set up orders backend and produce orders
    orders = ord.orders_backend(config)
    orders.create_orders()
    orders.produce_orders()

def run_transactions_backend(config):
    # Set up transactions backend and consume orders
    transactions = trn.transactions_backend(config)
    transactions.consume_orders()

"""
Main function to run the orders backend and produce orders to Kafka
"""
def main():
    config = initialize_config()
    run_orders_backend(config)
    run_transactions_backend(config)

if __name__ == "__main__":
    main()
