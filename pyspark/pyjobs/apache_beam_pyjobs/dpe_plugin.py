from kafka_logger import KafkaLogger
from executor_config import ExecutorConfig
from stream_processor import StreamProcessor
from dpe_api import DPEApi

class DPEPlugin(StreamProcessor):

    def __init__(self, config=None, executor_config=None, ssc=None):
        super(DPEPlugin, self): __init__(config=config, executor_config=None, ssc=ssc)
        self.config = config
        self.executor_config = executor_config if executor_config is not None else ExecutorConfig(self.config)
        self.kafka_logger= KafkaLogger(self.config, self.executor_config)
        self.api = DPEApi(self.config, self.executor_config)

    def dpe_init(self):
        """
        :return: nothing
        """
        return self

    def dpe_prepare(self):
        if type(self.output) == type("") or self.output is None or type(self.output) == type(self.kafka_logger):
            self.api.log_info("macro DPE_PREPARE changed self.output to kafka_logger (1) \"\"\"" + str(self.output) + "\"\"\"")
            self.output = self.kafka_logger
        else:
            self.api.log_info("macro DPE_PREPARE failed to change to kafka_logger (1)")
        return self

    def dpe_admin(self, msg):
        import json
        self.api.log_debug("Start admin")
        try:
            parsed = json.loads(msg)
            secret = parsed.get("secret", "")

            task = parsed.get("task", "")
            if task == "hbase_table_create":
                tablename = parsed.get("tablename", "(null)")
                list_cf = []
                self.api.as_group_admin().hbase_table_create(secret, tablename, list_cf)
            elif task == "hbase_table_list":
                tablename = parsed.get("tablename", "(null)")
                self.api.as_group_admin().hbase_table_list(secret, tablename)
            elif task == "hbase_table_delete":
                tablename = parsed.get("tablename", "(null)")
                self.api.as_group_admin().hbase_table_delete(secret, tablename)
            elif task == "hbase_trigger_create":
                tablename = parsed.get("tablename", "(null)")
                columnfamily = parsed.get("columnfamily", "(null)")
                column = parsed.get("column", "(null)")
                list_topics = []
                self.api.as_group_admin().hbase_trigger_create(secret, tablename, columnfamily, column, list_topics)
            elif task == "hbase_trigger_list":
                tablename = parsed.get("tablename", "(null)")
                columnfamily = parsed.get("columnfamily", "(null)")
                column = parsed.get("column", "(null)")
                list_topics = []
                self.api.as_group_admin().hbase_trigger_list(secret, tablename, columnfamily, column)
            elif task == "hbase_trigger_delete":
                tablename = parsed.get("tablename", "(null)")
                columnfamily = parsed.get("columnfamily", "(null)")
                column = parsed.get("column", "(null)")
                list_topics = []
                self.api.as_group_admin().hbase_trigger_delete(secret, tablename, columnfamily, column)
            else:
                raise Exception("task is unknown " + task)
            self.api.kafka_put("log_dpe_admin", "success " + task + "finished" + msg)
        except Exception as e:
            st = "~".join(self.api.format_stack().split("\n"))
            what = str(self.api.prepare_info(msg)) + str(e) + str(st)
            self.api.kafka_put("log_dpe_admin", "fail" + what)
        self.api.log_debug("End admin")