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

import pytest

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
from apache_beam.utils import subprocess_server
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions
)

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
  def __init__(
      self, bootstrap_servers=None, topic=None, null_key=None, expansion_service=None):
    self.bootstrap_servers = bootstrap_servers
    self.topic = topic
    self.null_key = null_key
    self.expansion_service = expansion_service
    self.sum_counter = Metrics.counter('source', 'elements_sum')

  def build_write_pipeline(self, pipeline):
    _ = (
        pipeline
        | 'Generate' >> beam.Create(range(NUM_RECORDS))  # pylint: disable=bad-option-value
        | 'MakeKV' >> beam.Map(
            lambda x: (None if self.null_key else b'key', str(x).encode())).
        with_output_types(typing.Tuple[typing.Optional[bytes], bytes])
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

  def build_read_pipeline_with_kerberos(self, p, max_num_records=None):

      jaas_config = (
          f'com.sun.security.auth.module.Krb5LoginModule required '
          f'useKeyTab=true storeKey=true '
          f'keyTab="secretValue:projects/dataflow-testing-311516/secrets/kafka-client-keytab/versions/latest" '
          f'principal="kafka-client@US-CENTRAL1-B.C.DATAFLOW-TESTING-311516.INTERNAL";'
      )

      kafka_records = (
          p
          | 'ReadFromKafka' >> ReadFromKafka(
              consumer_config={
                  'bootstrap.servers': 'fozzie-test-kafka-broker.us-central1-c.c.dataflow-testing-311516.internal:9092',
                  'auto.offset.reset': 'earliest',
                  'max_num_records': max_num_records,
                  'security.protocol': 'SASL_PLAINTEXT',
                  'sasl.mechanism': 'GSSAPI',
                  'sasl.kerberos.service.name': 'kafka',
                  'sasl.jaas.config': jaas_config
              },
              topics=['fozzie_test_kerberos_topic'],
              key_deserializer='org.apache.kafka.common.serialization.StringDeserializer',
              value_deserializer='org.apache.kafka.common.serialization.StringDeserializer',
              consumer_factory_fn_class='org.apache.beam.sdk.extensions.kafka.factories.KerberosConsumerFactoryFn',
              consumer_factory_fn_params={'krb5Location': 'gs://fozzie_testing_bucket/kerberos/krb5.conf'}))
      return kafka_records

  def run_xlang_kafkaio(self, pipeline):
    self.build_write_pipeline(pipeline)
    self.build_read_pipeline(pipeline)
    pipeline.run(False)


class CrossLanguageKafkaIOTest(unittest.TestCase):
  @unittest.skipUnless(
      os.environ.get('LOCAL_KAFKA_JAR'),
      "LOCAL_KAFKA_JAR environment var is not provided.")
  def test_local_kafkaio_populated_key(self):
    kafka_topic = 'xlang_kafkaio_test_populated_key_{}'.format(uuid.uuid4())
    local_kafka_jar = os.environ.get('LOCAL_KAFKA_JAR')
    with self.local_kafka_service(local_kafka_jar) as kafka_port:
      bootstrap_servers = '{}:{}'.format(
          self.get_platform_localhost(), kafka_port)
      pipeline_creator = CrossLanguageKafkaIO(
          bootstrap_servers, kafka_topic, False)

      self.run_kafka_write(pipeline_creator)
      self.run_kafka_read(pipeline_creator, b'key')

  @unittest.skipUnless(
      os.environ.get('LOCAL_KAFKA_JAR'),
      "LOCAL_KAFKA_JAR environment var is not provided.")
  def test_local_kafkaio_null_key(self):
    kafka_topic = 'xlang_kafkaio_test_null_key_{}'.format(uuid.uuid4())
    local_kafka_jar = os.environ.get('LOCAL_KAFKA_JAR')
    with self.local_kafka_service(local_kafka_jar) as kafka_port:
      bootstrap_servers = '{}:{}'.format(
          self.get_platform_localhost(), kafka_port)
      pipeline_creator = CrossLanguageKafkaIO(
          bootstrap_servers, kafka_topic, True)

      self.run_kafka_write(pipeline_creator)
      self.run_kafka_read(pipeline_creator, None)

  @pytest.mark.uses_io_java_expansion_service
  @unittest.skipUnless(
      os.environ.get('EXPANSION_PORT'),
      "EXPANSION_PORT environment var is not provided.")
  @unittest.skipUnless(
      os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
      "KAFKA_BOOTSTRAP_SERVER environment var is not provided.")
  def test_hosted_kafkaio_populated_key(self):
    kafka_topic = 'xlang_kafkaio_test_populated_key_{}'.format(uuid.uuid4())
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
    pipeline_creator = CrossLanguageKafkaIO(
        bootstrap_servers,
        kafka_topic,
        False,
        'localhost:%s' % os.environ.get('EXPANSION_PORT'))

    self.run_kafka_write(pipeline_creator)
    self.run_kafka_read(pipeline_creator, b'key')

  @pytest.mark.uses_io_java_expansion_service
  @unittest.skipUnless(
      os.environ.get('EXPANSION_PORT'),
      "EXPANSION_PORT environment var is not provided.")
  @unittest.skipUnless(
      os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
      "KAFKA_BOOTSTRAP_SERVER environment var is not provided.")
  def test_hosted_kafkaio_null_key(self):
    kafka_topic = 'xlang_kafkaio_test_null_key_{}'.format(uuid.uuid4())
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
    pipeline_creator = CrossLanguageKafkaIO(
        bootstrap_servers,
        kafka_topic,
        True,
        'localhost:%s' % os.environ.get('EXPANSION_PORT'))

    self.run_kafka_write(pipeline_creator)
    self.run_kafka_read(pipeline_creator, None)

  def test_hosted_kafkaio_null_key_kerberos(self):
      kafka_topic = 'xlang_kafkaio_test_null_key_{}'.format(uuid.uuid4())
      bootstrap_servers = 'fozzie-test-kafka-broker.us-central1-c.c.dataflow-testing-311516.internal:9092'
      pipeline_creator = CrossLanguageKafkaIO(
          bootstrap_servers,
          kafka_topic,
          True,
          'localhost:%s' % os.environ.get('EXPANSION_PORT'))

      self.run_kafka_read_with_kerberos(pipeline_creator)

  def run_kafka_write(self, pipeline_creator):
    with TestPipeline() as pipeline:
      pipeline.not_use_test_runner_api = True
      pipeline_creator.build_write_pipeline(pipeline)

  def run_kafka_read(self, pipeline_creator, expected_key):
    with TestPipeline() as pipeline:
      pipeline.not_use_test_runner_api = True
      result = pipeline_creator.build_read_pipeline(pipeline, NUM_RECORDS)
      assert_that(
          result,
          equal_to([(expected_key, str(i).encode())
                    for i in range(NUM_RECORDS)]))

  def run_kafka_read_with_kerberos(self, pipeline_creator):
      options_dict = {
          'runner': 'DataflowRunner',
          'project': 'dataflow-testing-311516',
          'region': 'us-central1',
          'streaming': False
      }
      options = PipelineOptions.from_dictionary(options_dict)
      expected_records = [f'test{i}' for i in range(1, 12)]
      with beam.Pipeline(options=options) as p:
          pipeline.not_use_test_runner_api = True
          result = pipeline_creator.build_read_pipeline_with_kerberos(p, max_num_records=11)
          assert_that(
              result,
              equal_to(expected_records)
          )

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
      kafka_server = subprocess.Popen([
          subprocess_server.JavaHelper.get_java(),
          '-jar',
          local_kafka_jar_file,
          kafka_port,
          zookeeper_port
      ])
      time.sleep(3)
      yield kafka_port
    finally:
      if kafka_server:
        kafka_server.kill()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
