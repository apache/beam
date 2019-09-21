import time
import math
import sys
import happybase
import traceback

from kafka_logger import KafkaLogger
from executor_config import ExecutorConfig
from pyspark.streaming.kafka import TopicAndPartition

class HbaseFilter(object):
    PREFIX_FILTER_TYPE = 1
    STARTSTOP_FILTER_TYPE = 2
    FULLSCAN_FILTER_TYPE = 3
    ARG_PREFIX_STR = "prefix_str"
    ARG_STOPSTART_STR = "stopstart_str"

    filter_type = None
    filter_args = None
    handler = None

    def __init__(self, filter_type=0, filter_args=None, handler=None):
        self.filter_type = filter_type
        self.filter_args = filter_args
        self.handler = handler

    def process(self, hbase_table):
        if self.filter_type == self.PREFIX_FILTER_TYPE:
            return self.handler.prefixFilterValues(hbase_table, self.filter_args[self.ARG_PREFIX_STR])
        if self.filter_type == self.STARTSTOP_FILTER_TYPE:
            return self.handler.start_stop_filter_values(hbase_table, self.filter_args[self.ARG_STOPSTART_STR])
        if self.filter_type == self.FULLSCAN_FILTER_TYPE:
            return self.handler.fullscanFilterValues(hbase_table)
        raise Exception("unknown type process not implemented")

    def set(self, other_filter_type, other_filter_args, other_handler):
        self.filter_type = other_filter_type
        self.filter_args = other_filter_args
        self.handler = other_handler

    def set_filter(self, other):
        self.filter_type = other.filter_type
        self.filter_args = other.filter_args
        self.handler = other.handler

