import threading


class Client(object) :
    def __init__(self, cfgFile:str = None):
        if (cfgFile is not None) :
            self.config = self.readConfig(cfgFile)
        else:
            self.config = None

        # Create atomic flag for synchronization so can run in seperate thread
        self.runFlag = threading.Event()
        self.runFlag.set()


    @staticmethod
    def readConfig(cfgFile:str) :
        # Reads the client configuration from client.properties
        # and returns it as a key-value map
        config = {}
        with open(cfgFile) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#" :
                    parameter, value = line.strip().split('=', 1)
                    config[parameter.strip()] = value.strip()

        return config