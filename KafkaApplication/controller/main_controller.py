from config import config_manager as cfg
from model.orders import orders_backend as ord
from model.transactions import transactions_backend as trn

def initialize_config():
    # Set up the client configuration
    control = cfg.config_manager()
    control.create_config()

def run_orders_backend():
    # Set up orders backend and produce orders
    orders = ord.orders_backend()
    orders.produce_orders()

def run_transactions_backend():
    # Set up transactions backend and consume orders
    transactions = trn.transactions_backend()
    transactions.consume_orders()

"""
Main function to run the orders backend and produce orders to Kafka
"""
def main():
    initialize_config()
    run_orders_backend()
    run_transactions_backend()

if __name__ == "__main__":
    main()
