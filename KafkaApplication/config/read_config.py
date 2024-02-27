"""
read_config class is responsible for reading the client configuration from the 
client.properties file. This class is used by the kafka_controller to initialize
the client configuration for the Kafka client. The client configuration is then
passed to the orders_backend, transactions_backend, analytics_backend, and
emails_backend classes to initialize the Kafka clients for each backend.
"""


class read_config:

    def __init__(self):
        """
        Initializes the read_config class by creating the client configuration.
        """
        self.config = self.create_config()

    def create_config(self):
        """
        Creates the client configuration by reading the client.properties file.
        """
        config = {}
        client_path = "KafkaApplication/config/client.properties"
        with open(client_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split("=", 1)
                    config[parameter] = value.strip()
        return config

    def get_config(self):
        """
        Returns the client configuration.
        """
        return self.config
