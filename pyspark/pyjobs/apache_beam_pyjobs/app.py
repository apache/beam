import argparse
import threading
import socket
import sys
import traceback
import time
from yaml import load, Loader
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from socket_server import SocketServer, RequestHandler
from processor_loader import ProcessorLoader
from kafka_logger import KafkaLogger
from kafka_handler import KafkaHandler
from driver_manager import DriverManager

class App():


    def __init__(self, spark_context, config, label):
        """
        :param spark_context: pyspark object
        :param config: dictionary
        :param label: service label
        """
        self.kafka_logger = KafkaLogger(config, None, True)
        self.kafka_handler = KafkaHandler(config)
        self.spark_context = spark_context
        self.config = config
        self.LABEL = label
        self.spark_streaming_context = None
        self.socket_server = None
        self.socket_server_thread = None
        self.run_thread = None
        self.stop_event = threading.Event()
        self.restart_event = threading.Event()
        self.batch_secs = config.get('batch.secs.' + self.LABEL, 1)
        self.service_port = self.config['service.port.'+self.LABEL]
        self.build_ver = self.config['build.ver.' + self.LABEL]
        self.create_mode = True if "True" == self.config.get('kafka.create', '') else False
        self.driver_manager = DriverManager(self.config, self.LABEL)
        self.processor_loader = ProcessorLoader(self.config, self.LABEL)

    def prepare_all(self):
        """
        :return: nothing
        """
        host = "localhost"
        self.socket_server = SocketServer((host, self.service_port),
            RequestHandler, self.stop_event, self.restart_event)
        self.socket_server_thread = threading.Thread(target=self.socket_server.serve_forever)
        self.kafka_logger.log_info(self.LABEL + " LISTEN ON %s:%s" % (host, self.service_port))

    def driver_process(self):
        """
        :return: nothing
        """
        self.kafka_logger = KafkaLogger(self.config)
        self.kafka_logger.log_debug(self.LABEL + " running stream mode")

        while not self.stop_event.is_set():
            if self.spark_streaming_context is None:
                try:
                    logger = app_sc._jvm.org.apache.log4j
                    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
                    logger.LogManager.getLogger("org.apache.kafka").setLevel(logger.Level.WARN)
                    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)
                except Exception as e:
                    raise Exception(self.LABEL + " UNKNOWN sc._jvm.org.apache.log4j: " + str(e))

                self.spark_streaming_context = StreamingContext(self.spark_context, self.batch_secs)
                self.kafka_logger.log_debug(self.LABEL + " self.spark_streaming_context = %s" % self.spark_streaming_context)

                try:
                    self.processor_loader.reload_on_driver()
                except Exception as e:
                    tb = sys.exc_info()[2]
                    ta = traceback.format_tb(tb)
                    what = str(e) + " " + " ~ ".join([ta[i] for i in range(0, len(ta))])
                    self.kafka_logger.log_error(self.LABEL + " FATAL ON LOADING " + what)
                    raise e

                self.driver_manager.start_executor(self.spark_streaming_context, self.processor_loader.get_cache_on_driver().validate_on_driver(),
                    self.processor_loader.caches[0])
            else:
                if self.restart_event.is_set():
                    self.spark_streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
                    self.spark_streaming_context.awaitTermination()
                    self.spark_streaming_context = None
                    self.restart_event.clear()
                else:
                    time.sleep(0.1)
        else:
            self.spark_streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
            self.spark_streaming_context.awaitTermination()
            self.spark_streaming_context = None


    def run(self):
        """
        :return: nothing
        """
        self.prepare_all()
        self.kafka_logger.log_info(self.LABEL + " STARTED SERVICE (version " + self.build_ver + ")")
        process_thread = threading.Thread(target=self.driver_process)
        process_thread.start()
        self.kafka_logger.log_debug(self.LABEL + " after process thread.start()")
        self.socket_server_thread.start()
        self.kafka_logger.log_debug(self.LABEL + " before process_thread.join()")
        process_thread.join()
        self.kafka_logger.log_debug(self.LABEL + " " + "!" * 5 + "after join" + "!" * 5)
        self.clean_up_jvm()

    def clean_up_jvm(self):
        """
        :return: nothing
        """
        try:
            # jStreamingConextOption = StreamingContext._jvm.SparkContext.getActive() # FIXME comment out
            # if jStreamingConextOption.nonEmpty(): # FIXME
            #     jStreamingConextOption.get().stop(False) # FIXME
            # jSparkContextOption = SparkContext._jvm.SparkContext.get() # FIXME
            # if jSparkContextOption.nonEmpty(): # FIXME
            #    jSparkContextOption.get().stop() # FIXME
            pass
        except Exception as e:
            # self.log.error("exception in clean_up_jvm: " + str(e))
            pass

    def start(self):
        """
        :return: nothing
        """
        self.run_thread = threading.Thread(target=self.run)
        self.run_thread.start()

    def stop(self, await_termination=False, timeout=None):
        """
        :param await_termination: soft stop waiting flag
        :param timeout: waiting time
        :return:
        """
        self.stop_event.set()
        if not self.socket_server.is_shut_down():
            self.socket_server.shutdown()
        self.socket_server.server_close()
        if await_termination:
            self.run_thread.join(timeout)

if __name__ == '__main__':
    print("start of __main__")
    parser = argparse.ArgumentParser(description='Process custom scripts.')
    parser.add_argument('-c', '--config', default='config.yml', type=str, help="Specify config file",
                        metavar="FILE", dest='config')
    parser.add_argument('-l', '--label', default='dpe00', type=str, help="Specify engine label", metavar="STR", dest='label')
    parser.add_argument('-g', '--gz', default='python-predictor-engine.tar.gz', type=str, help="Specify gz file",
                        metavar="FILE", dest='gzball')
    args = parser.parse_args()
    with open(args.config) as config_file:
        app_config = load(config_file, Loader=Loader)
        app_config.update(vars(args))
    print(" in " + args.label)

    app_label = args.label
    app_sc = SparkContext(appName='Data Processing Engine App ' + app_label)
    app_engine = App(app_sc, app_config, app_label)
    app_engine.run()
    app_sc.stop()

