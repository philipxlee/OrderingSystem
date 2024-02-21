from confluent_kafka import Consumer, KafkaError
from . import read_config as config

class analytics_backend():
    
    def __init__(self):
        self.config = config.get_config()
        




    