from executor_config import ExecutorConfig
from hbase_handler import HBaseFilter
from hbase_handler import HBaseHandler
from group_admin_api import HbaseHandler
from group_admin_api import HbaseSource
from group_admin_api import HbaseDestination
from kafka_handler import KafkaHandler
from kafka_logger import KafkaLogger
from flume_handler import SuperFlow
from flume_handler import SuperInfo
from group_admin_api import GroupAdminApi
from group_student_api import GroupStudentApi

class DPEApi(object):

    def __init__(self, config, executor_config):
        """
        :param config: dictionary (on driver)
        :param executor_config: object (on executor)
        """
        self.config = config
        self.executor_config = executor_config if executor_config is not None else ExecutorConfig(self.config)
        self.kafka_handler = KafkaHandler(self.config, self.executor_config)
        self.kafka_logger = KafkaLogger(self.config, self.executor_config)
        self.hbase_handler = HbaseHandler(self.config, self.executor_config)

    def as_group_admin(self):
        """
        :return: api with methods for group admin
        """
        return GroupAdminApi(self.config, self.executor_config)

    def as_group_student(self):
        """
        :return: api with methods for group student
        """
        return  GroupStudentApi(self.config, self.executor_config)

    def log_debug(self, msg):
        """
        :param msg: message for kafka logging (string)
        :return: nothing
        """
        self.kafka_logger.log_debug(msg)

    def log_info(self, msg):
        """
        :param msg: message for kafka logging (string)
        :return: nothing
        """
        self.kafka_logger.log_info(msg)

    def log_warn(self, msg):
        """
        :param msg: message for kafka logging (string)
        :return: nothing
        """
        self.kafka_logger.log_warn(msg)

    def log_error(self, msg):
        """
        :param msg: message for kafka logging (string)
        :return: nothing
        """
        self.kafka_logger.log_error(msg)

    def hbase_to_bytes(self, val):
        """
        :param val: string for convertation
        :return: converted val to bytes
        """
        return HbaseHandler.to_bytes(val, False)

    def hbase_from_bytes_str(self, val):
        """
        :param val: bytes for convertation
        :return: converted val to string
        """
        return HbaseHandler.from_bytes_str(val)

    def hbase_prepare_filter(self):
        """
        :return: hbase filter for full scan
        """
        return HbaseFilter(HbaseFilter.FULLSCAN_FILTER_TYPE, {}, self.hbase_handler)

    def prefix_filter(self, rowkey_prefix_str):
        """
        :param rowkey_prefix_str: hbase param
        :return: hbase filter for prefix scan
        """
        return HbaseFilter(HbaseFilter.PREFIX_FILTER_TYPE
                           , {HbaseFilter.ARG_PREFIX_STR: rowkey_prefix_str}, self.hbase_handler)

    def hbase_scan(self, in_tbl, scan_filter):
        """
        :param in_tbl: hbase table
        :param scan_filter: hbase filter
        :return: hbase iterator
        """
        return scan_filter.process(in_tbl)

    def hbase_extract_value(self, rowdata, cf_str, q_str):
        """
        :param rowdata:
        :param cf_str:
        :param q_str:
        :return:
        """
        return self.hbase_handler.extract_hbase_values(rowdata, cf_str, q_str)

    def hbase_extract(self, in_tbl, rowkey_str):
        """
        :param in_tbl:
        :param rowkey_str:
        :return:
        """
        return self.hbase_handler.start_stop_filter_values()

    def hbase_get(self, in_tbl, rowkey_str, cf_str, q_str):
        """
        :param in_tbl: hbase table
        :param rowkey_str: hbase rowkey
        :param cf_str: hbase column family
        :param q_str: hbase qualifier
        :return: fetched value
        """
        return self.hbase_handler.extract_hbase_values(self.hbase_handler.start_stop_filter_values(in_tbl, rowkey_str), cf_str, q_str)

    def hbase_put(self, out_tbl, rowkey_str, cf_str, q_str, val_str):
        """
        :param out_tbl: hbase table
        :param rowkey_str: hbase rowkey
        :param cf_str: hbase column family
        :param q_str: hbase qualifier
        :param val_str: value
        :return: nothing
        """
        self.hbase_handler.newPut(out_tbl, rowkey_str, cf_str, q_str, val_str)

    def hbase_source(self, in_tbl, in_cf, in_vars):
        """
        :param in_tbl: hbase table
        :param in_cf: hbase column family
        :param in_vars: hbase qualifiers
        :return: useful object
        """
        return HbaseSource(in_tbl, in_cf, in_vars)

    def hbase_destination(self, out_tbl, out_cf, out_var, out_mark=""):
        """
        :param out_tbl: hbase table
        :param out_cf: hbase column family
        :param out_var: hbase qualifier
        :param out_mark: hbase extra rowkey suffix
        :return: useful object
        """
        return HbaseDestination(out_tbl, out_cf, out_var, out_mark)

    def kafka_put(self, topic_name, value):
        """
        :param topic_name: kafka topic
        :param value: string value
        :return: nothing
        """
        self.kafka_handler.save_to_kafka(topic_name, value)

    def prepare_flow(self, msg=""):
        """
        :param msg: data flow provenance (like nifi)
        :return: useful object
        """
        return SuperFlow().with_info(SuperInfo(msg))

    def prepare_info(self, msg, headers=None):
        """
        :param msg: data flow provenance (like nifi)
        :param headers: data flow headers (like nifi)
        :return: useful object
        """
        return SuperInfo(msg, headers)

    def format_stack(self):
        """
        :return: stacktrace for debug printing
        """
        return KafkaLogger.format_stack()
