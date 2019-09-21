import traceback
import sys
import time
from driver_utils import DriverUtils
from pyspark.streaming.kafka import KafkaUtils
from executor_config import ExecutorConfig
from kafka_logger import KafkaLogger
from kafka_handler import KafkaHandler
from offset_handler import OffsetHandler
from processor_runner import ProcessorRunner
from processor_params import ProcessorParams

from driver_row import DriverRow

class DriverManager:

    def __init__(self, config, label):
        """
        :param config: dictionary (on driver)
        :param label: service label
        """
        self.config = config
        self.kafka_logger = KafkaLogger(config)
        self.kafka_handler = KafkaHandler(config)
        self.kafka_brokers = self.config["kafka.brokers"]
        self.table_hbase = config.get('table.hbase.' + + label, 'services.' + label + '_plugins')
        self.reload_secs = config.get('reload.secs.' + label, 300)
        self.offset_handler = OffsetHandler(self.config, None, self.table_hbase)
        self.LABEL = label

    def start_executor(self, spark_streaming_context, cache, mirror_cache):
        """
        :param spark_streaming_context: spark object
        :param cache: main cache (all you need to send from driver to main executor)
        :param mirror_cache: mirror cache (all you need to send from driver to mirror executor)
        :return:
        """

        def transform_on_driver(rdd):
            """
            :param rdd: spark object
            :return: spark object
            """
            offsetRanges = rdd.offsetRanges()
            return rdd.map(lambda row: DriverRow(row).with_kafka_ranges(offsetRanges).as_row())

        def process_rdd_on_executor(rdd, params):
            """
            :param rdd: spark object
            :param params: processor params
            :return: nothing
            """
            runner = ProcessorRunner()
            runner.process_rdd_on_executor(rdd, params)

        def process_rdd_on_mirror(rdd, params):
            """
            :param rdd: spark object
            :param params: processor params
            :return: nothing
            """
            runner = ProcessorRunner()
            runner.process_rdd_on_mirror(rdd, params)

        for topic, plugin_tags in cache.topics_map.items():
            for plugin_tag in plugin_tags:
                cache.register_hbase_offsets_on_driver(self.offset_handler.get_all_offsets(topic, topic, plugin_tag, cache.mirror_tag))

        import datetime
        executor_params = ProcessorParams(cache, None, ExecutorConfig(self.config), self.offset_handler.offset_tbl_name
                                          , int(datetime.datetime.now().strftime('%s')), self.LABEL + "|" + str(cache.mirror_tag))

        try:
            dstream = KafkaUtils.createDirectStream(spark_streaming_context, cache.get_minimum_topics_list_on_driver()
                                                    , fromOffsets=cache.get_minimum_offsets_on_driver()
                                                    , kafkaParams={"metadata.broker.list": self.kafka_brokers})
            dstream2 = dstream.transform(lambda rdd: transform_on_driver(rdd))
            dstream2.foreachRDD(lambda rdd: process_rdd_on_executor(rdd, executor_params))
            self.kafka_logger.log_info(
                ("%s DSTREAM CREATED |1 KafkaUtils.createDirectStream(%s, %s, fromOffset=%s, " +
                 "kafkaParams={\"metadata.broker.list\": %s})") % (self.LABEL, "spark_streaming_context",
                                                                   str(cache.get_minimum_topics_list_on_driver())
                                                                   , str(cache.get_minimum_offsets_on_driver()),
                                                                   str(self.kafka_brokers)))

        except Exception as e:
            tb = sys.exc_info()[2]
            ta = traceback.format_tb(tb)
            what = str(e) + " " + " ~ ".join([ta[1] for i in range(0, len(ta))])
            self.kafka_logger.log_error(
                ("%s DSTREAM FAILED |1 KafkaUtils.createDirectStream(%s, %s, fromOffsets=%s, " +
                 "kafkaParams={\"metadata.broker.list\": %s}} %s") % (self.LABEL
                                                                      , "spark_streaming_context"
                                                                      , str(cache.get_minimum_topics_list_on_driver())
                                                                      , str(cache.get_minimum_offsets_on_driver()),
                                                                        str(self.kafka_brokers), what))
            raise e

        mirror_executor_params = ProcessorParams(mirror_cache, None
                                                 , ExecutorConfig(self.config), self.offset_handler.offset_tbl_name,
                                                 int(datetime.datetime.now().strftime("%s")),
                                                 self.LABEL + "|" + str(mirror_cache.mirror_tag))

        try:
            mirror_dstream = KafkaUtils.createDirectStream(spark_streaming_context, []
                                                           , fromOffsets={}
                                                           , kafkaParams={"metadata.broker.list": self.kafka_brokers})
            mirror_dstream2 = mirror_dstream.transform(lambda rdd: transform_on_driver(rdd))
            mirror_dstream2.foreachRDD((lambda rdd: process_rdd_on_mirror(rdd, mirror_executor_params)))
            self.kafka_logger.log_info(("%s DSTREAM CREATED |0 KafkaUtils.createDirectStream(%s, %s, fromOffsets=%s, " +
                                        "kafkaParams={\"metadata.broker.list\": %s})") % (self.LABEL
                                                                                          , "spark_streaming_context", str([]), str({}),
                                                                                          str(self.kafka_brokers)))
        except Exception as e:
            tb = sys.exc_info()[2]
            ta = traceback.format_tb(tb)
            what = str(e) + " " + " ~ ".join([ta[1] for i in range(0, len(ta))])
            self.kafka_logger.log_error(
                ("%s DSTREAM FAILED |0 KafkaUtils.createDirectStream(%s, %s, fromOffsets=%s, " +
                 "kafkaParams={\"metadata.broker.list\": %s}} %s") % (self.LABEL
                                                                      , "spark_streaming_context"
                                                                      , str([])
                                                                      , str({}),
                                                                        str(self.kafka_brokers), what))
            raise e

        spark_streaming_context.start()

        for t in range(0, 3):
            time.sleep(0.1)
            self.kafka_logger.log_info("%s WAIT SPARK KafkaUtils.createDirectStream(...)")
