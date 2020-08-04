#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Integration test for Python cross-language pipelines for Java KafkaIO."""

from __future__ import absolute_import

import contextlib
import logging
import os
import socket
import subprocess
import sys
import time
import typing
import unittest

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.metrics import Metrics
from apache_beam.testing.test_pipeline import TestPipeline


class CrossLanguageKafkaIO(object):
  def __init__(self, bootstrap_servers, topic, expansion_service=None):
    self.bootstrap_servers = bootstrap_servers
    self.topic = topic
    self.expansion_service = expansion_service
    self.sum_counter = Metrics.counter('source', 'elements_sum')

  def build_write_pipeline(self, pipeline):
    _ = (
        pipeline
        | 'Impulse' >> beam.Impulse()
        | 'Generate' >> beam.FlatMap(lambda x: range(1000))  # pylint: disable=range-builtin-not-iterating
        | 'Reshuffle' >> beam.Reshuffle()
        | 'MakeKV' >> beam.Map(lambda x:
                               (b'', str(x).encode())).with_output_types(
                                   typing.Tuple[bytes, bytes])
        | 'WriteToKafka' >> WriteToKafka(
            producer_config={'bootstrap.servers': self.bootstrap_servers},
            topic=self.topic,
            expansion_service=self.expansion_service))

  def build_read_pipeline(self, pipeline):
    _ = (
        pipeline
        | 'ReadFromKafka' >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': self.bootstrap_servers,
                'auto.offset.reset': 'earliest'
            },
            topics=[self.topic],
            expansion_service=self.expansion_service)
        | 'Windowing' >> beam.WindowInto(
            beam.window.FixedWindows(300),
            trigger=beam.transforms.trigger.AfterProcessingTime(60),
            accumulation_mode=beam.transforms.trigger.AccumulationMode.
            DISCARDING)
        | 'DecodingValue' >> beam.Map(lambda elem: int(elem[1].decode()))
        | 'CombineGlobally' >> beam.CombineGlobally(sum).without_defaults()
        | 'SetSumCounter' >> beam.Map(self.sum_counter.inc))

  def run_xlang_kafkaio(self, pipeline):
    self.build_write_pipeline(pipeline)
    self.build_read_pipeline(pipeline)
    pipeline.run(False)


@unittest.skipUnless(
    os.environ.get('LOCAL_KAFKA_JAR'),
    "LOCAL_KAFKA_JAR environment var is not provided.")
class CrossLanguageKafkaIOTest(unittest.TestCase):
  def get_platform_localhost(self):
    if sys.platform == 'darwin':
      return 'host.docker.internal'
    else:
      return 'localhost'

  def get_open_port(self):
    s = None
    try:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:  # pylint: disable=bare-except
      # Above call will fail for nodes that only support IPv6.
      s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port

  @contextlib.contextmanager
  def local_kafka_service(self, local_kafka_jar_file):
    kafka_port = str(self.get_open_port())
    zookeeper_port = str(self.get_open_port())
    kafka_server = None
    try:
      kafka_server = subprocess.Popen(
          ['java', '-jar', local_kafka_jar_file, kafka_port, zookeeper_port])
      time.sleep(3)
      yield kafka_port
    finally:
      if kafka_server:
        kafka_server.kill()

  def test_kafkaio_write(self):
    local_kafka_jar = os.environ.get('LOCAL_KAFKA_JAR')
    with self.local_kafka_service(local_kafka_jar) as kafka_port:
      p = TestPipeline()
      p.not_use_test_runner_api = True
      xlang_kafkaio = CrossLanguageKafkaIO(
          '%s:%s' % (self.get_platform_localhost(), kafka_port),
          'xlang_kafkaio_test')
      xlang_kafkaio.build_write_pipeline(p)
      job = p.run()
      job.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
