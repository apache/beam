from secret_storage import SecretStorage
from hbase_handler import HbaseHandler
from kafka_handler import KafkaHandler
from kafka_logger import KafkaLogger
from executor_config import ExecutorConfig
import datetime
import json


class HbaseSource(object):

    def __init__(self, in_tbl, in_cf, in_vars):
        """
        :param in_tbl: hbase table
        :param in_cf: hbase column family
        :param in_vars: hbase qualifiers
        """
        d = {"in_tbl": in_tbl, "in_cf": in_cf, "in_Vars": in_vars}
        self.in_tbl = d["in_tbl"]
        self.in_cf = d["in_cf"]
        self.in_vars = d["in_vars"]
        self.tags = {}

    def __str__(self):
        """
        :return: serialization
        """
        return str(self.in_tbl) + ":" + str(self.in_cf) + ":" + str(self.in_vars)

    def with_tag(self, k, v):
        """
        :param k: tags key (nifi style)
        :param v: tags value (nifi style)
        :return: self
        """
        self.tags[k] = v
        return self

class HbaseDestination(object):

    def __init__(self, out_tbl, out_cf, out_var, out_mark=""):
        """
        :param out_tbl: hbase table
        :param out_cf: hbase column family
        :param out_var: hbase qualifier
        :param out_mark: hbase rowkey extra prefix
        """
        d = {"out_tbl": out_tbl, "out_cf": out_cf, "out_var": out_var, "out_mark": out_mark}
        self.out_tbl = d["out_tbl"]
        self.out_cf = d["out_cf"]
        self.out_var = d["out_var"]
        self.out_mark = d["out_mark"]
        self.tags = {}

    def __str__(self):
        """
        :return: serialization
        """
        return str(self.out_tbl) + ":" + str(self.out_cf) + ":" + str(self.out_var) + ":" + str(self.out_mark)

    def with_tag(self, k, v):
        """
        :param k: tags key (nifi style)
        :param v: tags value (nifi style)
        :return: self
        """
        self.tags[k] = v
        return self


class GroupAdminApi(object):

    def __init__(self, config, executor_config):
        self.config = config
        self.executor_config = executor_config if executor_config is not None else ExecutorConfig(self.config)
        self.kafka_handler = KafkaHandler(self.config, self.executor_config)
        self.kafka_logger = KafkaLogger(self.config, self.executor_config)
        self.hbase_handler = HbaseHandler(self.config, self.executor_config)
        self.storage = SecretStorage()
        self.log_topic = self.executor_config.logging_topic

    def hbase_table_create(self, secret, tablename, list_cf):
        self.storage.check(secret)
        return self.hbase_handler.createTable(tablename, list_cf)

    def hbase_table_list(self, secret, tablename):
        self.storage.check(secret)
        return self.hbase_handler.listTable(tablename)

    def hbase_table_delete(self, secret, tablename):
        self.storage.check(secret)
        return self.hbase_handler.deleteTable(tablename)

    def hbase_trigger_create(self, secret, tablename, columnfamily, column, list_topics):
        self.storage.check(secret)
        return self.hbase_handler.createTrigger(tablename, columnfamily, column, list_topics)

    def hbase_trigger_list(self, secret, tablename, columnfamily, column):
        self.storage.check(secret)
        return self.hbase_handler.listTrigger(tablename, columnfamily, column)

    def hbase_trigger_delete(self, secret, tablename, columnfamily, column):
        self.storage.check(secret)
        return self.hbase_handler.deleteTrigger(tablename, columnfamily, column)
