class DriverMeta:

    def __init__(self, topic="", partition=-1):
        """
        :param topic: kafka topic
        :param partition: kafka topic partition
        """
        self.topic = topic
        self.partition = partition