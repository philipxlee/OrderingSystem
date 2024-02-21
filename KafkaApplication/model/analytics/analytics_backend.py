from confluent_kafka import Consumer, KafkaError
from config import config_manager as config

class analytics_backend():
    
    def __init__(self):
        self.config = config.get_config()




    