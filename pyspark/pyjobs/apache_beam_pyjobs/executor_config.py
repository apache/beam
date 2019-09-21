class ExecutorConfig:

    def __init__(self, config):
        self.logging_level = config["logging"]["level"]
        self.logging_topic = config["logging"]["topic"]
        self.resque_topic = config["resque"]["topic"]
        self.kafka_brokers = config['kafka.brokers']
        self.hbase_host = config["hbase.host"]
        self.hbase_port = config["hbase.port"]
        nameservice = config["namenode.host"]
        self.hdfs_root = 'hdfs://' + nameservice