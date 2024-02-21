class read_config():

    """
    Reads the client configuration from client.properties and returns it as a key-value map
    """
    def __init__(self):
        self.config = self.create_config()

    def create_config(self):
        config = {}
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
    
    def get_config(self):
        return self.config
    






