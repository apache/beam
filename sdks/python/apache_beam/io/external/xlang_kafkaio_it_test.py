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
import uuid

import apache_beam as beam
from apache_beam.coders.coders import VarIntCoder
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.metrics import Metrics
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec

NUM_RECORDS = 1000


class CollectingFn(beam.DoFn):
  BUFFER_STATE = BagStateSpec('buffer', VarIntCoder())
  COUNT_STATE = CombiningValueStateSpec('count', sum)

  def process(
      self,
      element,
      buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
      count_state=beam.DoFn.StateParam(COUNT_STATE)):
    value = int(element[1].decode())
    buffer_state.add(value)

    count_state.add(1)
    count = count_state.read()

    if count >= NUM_RECORDS:
      yield sum(buffer_state.read())
      count_state.clear()
      buffer_state.clear()


class CrossLanguageKafkaIO(object):
  def __init__(self, bootstrap_servers, topic, expansion_service=None):
    self.bootstrap_servers = bootstrap_servers
    self.topic = topic
    self.expansion_service = expansion_service
    self.sum_counter = Metrics.counter('source', 'elements_sum')

  def build_write_pipeline(self, pipeline):
    _ = (
        pipeline
        | 'Generate' >> beam.Create(range(NUM_RECORDS))  # pylint: disable=range-builtin-not-iterating
        | 'MakeKV' >> beam.Map(lambda x:
                               (b'', str(x).encode())).with_output_types(
                                   typing.Tuple[bytes, bytes])
        | 'WriteToKafka' >> WriteToKafka(
            producer_config={'bootstrap.servers': self.bootstrap_servers},
            topic=self.topic,
            expansion_service=self.expansion_service))

  def build_read_pipeline(self, pipeline, max_num_records=None):
    kafka_records = (
        pipeline
        | 'ReadFromKafka' >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': self.bootstrap_servers,
                'auto.offset.reset': 'earliest'
            },
            topics=[self.topic],
            max_num_records=max_num_records,
            expansion_service=self.expansion_service))

    if max_num_records:
      return kafka_records

    return (
        kafka_records
        | 'CalculateSum' >> beam.ParDo(CollectingFn())
        | 'SetSumCounter' >> beam.Map(self.sum_counter.inc))

  def run_xlang_kafkaio(self, pipeline):
    self.build_write_pipeline(pipeline)
    self.build_read_pipeline(pipeline)
    pipeline.run(False)


@unittest.skipUnless(
    os.environ.get('LOCAL_KAFKA_JAR'),
    "LOCAL_KAFKA_JAR environment var is not provided.")
class CrossLanguageKafkaIOTest(unittest.TestCase):
  def test_kafkaio(self):
    kafka_topic = 'xlang_kafkaio_test_{}'.format(uuid.uuid4())
    local_kafka_jar = os.environ.get('LOCAL_KAFKA_JAR')
    with self.local_kafka_service(local_kafka_jar) as kafka_port:
      bootstrap_servers = '{}:{}'.format(
          self.get_platform_localhost(), kafka_port)
      pipeline_creator = CrossLanguageKafkaIO(bootstrap_servers, kafka_topic)

      self.run_kafka_write(pipeline_creator)
      self.run_kafka_read(pipeline_creator)

  def run_kafka_write(self, pipeline_creator):
    with TestPipeline() as pipeline:
      pipeline.not_use_test_runner_api = True
      pipeline_creator.build_write_pipeline(pipeline)

  def run_kafka_read(self, pipeline_creator):
    with TestPipeline() as pipeline:
      pipeline.not_use_test_runner_api = True
      result = pipeline_creator.build_read_pipeline(pipeline, NUM_RECORDS)
      assert_that(
          result,
          equal_to([(b'', str(i).encode()) for i in range(NUM_RECORDS)]))

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
