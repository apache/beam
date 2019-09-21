import json
import base64

from executor_config import ExecutorConfig
from hbase_handler import HbaseHandler
from kafka_handler import KafkaHandler
from kafka_logger import KafkaLogger
from flume_handler import SuperFlow
from flume_handler import SuperInfo

class GroupMycreditApi(object):

    def __init__(self, config, executor_config):
        """
        :param config:
        :param executor_config:
        """
        self.config = config
        self.executor_config = executor_config if executor_config is not None else ExecutorConfig(self.config)
        self.kafka_handler = KafkaHandler(self.config, self.executor_config)
        self.kafka_logger = KafkaLogger(self.config, self.executor_config)
        self.hbase_handler = HbaseHandler(self.config, self.executor_config)
        self.log_topic = self.executor_config.logging_topic

    def local_prepare_flow(self, msg=""):
        """
        :param msg:
        :return:
        """
        return SuperFlow().with_info(SuperInfo(msg))

    def hbase_flow(self, rowkey, rowdata, src, info):
        """
        :param rowkey:
        :param rowdata:
        :param src:
        :param info:
        :return:
        """
        flow = self.local_prepare_flow(info)
        for k in src.in_vars.keys():
            flow.filedata[k] = self.hbase_handler.extract_value(rowdata, src.in_cf, src.in_vars[k])
        flow.info.headers["rowkey"] = rowkey
        return flow

    def send_success(self, info, output_flow_list, dst, log_topic):
        """
        :param info:
        :param output_flow_list:
        :param dst:
        :param log_topic:
        :return:
        """
        what = (str(info.ts))
        self.kafka_logger.log_info("SUCCESS: %s %s" % (log_topic, what))
        builder = {}
        for flow in output_flow_list:
            for out_var in flow.filedata.keys():
                self.hbase_handler.new_put(dst.out_tbl, output_flow_list[0].info.headers["rowkey"] + dst.out_mark
                                     , dst.out_cf, out_var, output_flow_list[0].filedata[out_var])
                builder["additional_info"] = json.dumps(flow)
                self.kafka_handler.save_to_kafka(log_topic, json.dumps(builder))

    def send_error(self, info, input_flow_list, e, st, err_topic):
        """
        :param info:
        :param input_flow_list:
        :param e:
        :param st:
        :param err_topic:
        :return:
        """
        what = (st(info.ts) + str(e) + str(st))
        self.kafka_logger.log_info("FAIL: %s %s" % (err_topic, what))
        final = [self.local_prepare_flow()]
        if input_flow_list is not None:
            final = input_flow_list
        for flow in final:
            builder = {}
            for key, v in flow.info.headers.items():
                builder["error_" + key] = v
            builder["error_description"] = what
            builder["error_source"] = flow.info.headers.get("source", "(null)")
            builder["error_code"] = "255"
            builder["error_time"] = info.ts
            builder["additional_info"] = json.dumps(flow)
            self.kafka_handler.save_to_kafka(err_topic, json.dumps(builder))
