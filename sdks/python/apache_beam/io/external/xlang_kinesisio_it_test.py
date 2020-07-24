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

"""Integration test for Python cross-language pipelines for Java KinesisIO."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import time
import unittest

import apache_beam as beam
from apache_beam.io.kinesis import InitialPositionInStream
from apache_beam.io.kinesis import ReadDataFromKinesis
from apache_beam.io.kinesis import WriteToKinesis

from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import Count

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import boto3
except ImportError:
  boto3 = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

MAX_NUM_RECORDS = 10

@unittest.skipIf(DockerContainer is None, 'testcontainers is not installed.')
@unittest.skipIf(boto3 is None, 'boto3 is not installed.')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
    None,
    'Do not run this test on precommit suites.')
class CrossLanguageKinesisIOTest(unittest.TestCase):
  def setUp(self):
    self.localstack = DockerContainer('localstack/localstack:0.8.6')\
      .with_env('SERVICES', 'kinesis')\
      .with_env('KINESIS_PORT', '4568')\
      .with_exposed_ports(4568)
    self.localstack.start()
    self.service_endpoint = 'http://{}:{}'.format(
        self.localstack.get_container_host_ip(),
        self.localstack.get_exposed_port('4568'),
    )
    self.access_key = 'accesskey'
    self.secret_key = 'secretkey'
    self.region = 'US_EAST_1'
    self.aws_region = 'us-east-1'
    self.partition_key = 'a'
    self.max_read_time = 120  # 2min
    self.initial_position_in_stream = InitialPositionInStream.TRIM_HORIZON
    self.stream_name = 'beam'
    self.create_kinesis_stream()

  def tearDown(self):
    self.localstack.stop()

  def test_kinesisio(self):
    self.run_kinesis_write()
    self.run_kinesis_read_data()

  def run_kinesis_write(self):
    with TestPipeline() as p:
      _ = (
          p
          | 'Impulse' >> beam.Impulse()
          | 'Generate' >> beam.FlatMap(lambda x: range(MAX_NUM_RECORDS))  # pylint: disable=range-builtin-not-iterating
          | 'Make bytes' >>
          beam.Map(lambda x: str(x).encode()).with_output_types(bytes)
          | 'WriteToKinesis' >> WriteToKinesis(
              stream_name=self.stream_name,
              aws_access_key=self.access_key,
              aws_secret_key=self.secret_key,
              service_endpoint=self.service_endpoint,
              region=self.region,
              partition_key=self.partition_key,
          ))

  def run_kinesis_read_data(self):
    with TestPipeline() as p:
      result = (
          p
          | 'ReadFromKinesis' >> ReadDataFromKinesis(
              stream_name=self.stream_name,
              aws_access_key=self.access_key,
              aws_secret_key=self.secret_key,
              service_endpoint=self.service_endpoint,
              region=self.region,
              max_num_records=MAX_NUM_RECORDS,
              max_read_time=self.max_read_time,
              initial_position_in_stream=self.initial_position_in_stream,
          )
          | 'Count elements' >> Count.Globally())

      assert_that(
          result, equal_to([str(i).encode() for i in range(MAX_NUM_RECORDS)]))

  def create_kinesis_stream(self):
    client = boto3.client(
        service_name='kinesis',
        region_name='us-east-1',
        endpoint_url=self.service_endpoint,
        aws_access_key_id=self.access_key,
        aws_secret_access_key=self.secret_key,

    )
    # localstack could not have initialized yet so try few times
    retries = 5
    for i in range(retries):
      try:
        client.create_stream(
            StreamName=self.stream_name,
            ShardCount=1,
        )
        time.sleep(2)
        break
      except Exception as e:
        if i == retries:
          raise e

    # ensure stream is initialized before proceeding
    stream = client.describe_stream(StreamName=self.stream_name)
    while stream['StreamDescription']['StreamStatus'] != 'ACTIVE':
      time.sleep(2)
      stream = client.describe_stream(StreamName=self.stream_name)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
