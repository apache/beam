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
import unittest

from testcontainers.core.container import DockerContainer

import apache_beam as beam
from apache_beam.io.kinesis import InitialPositionInStream
from apache_beam.io.kinesis import ReadDataFromKinesis
from apache_beam.io.kinesis import WriteToKinesis
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import Count

MAX_NUM_RECORDS = 10
MAX_READ_TIME = 120
INITIAL_POSITION_IN_STREAM = InitialPositionInStream.TRIM_HORIZON


class CrossLanguageKinesisIOTest(unittest.TestCase):
  def setUp(self):
    self.localstack = DockerContainer('localstack:localstack:0.11.3')\
      .with_env('SERVICES', 'kinesis')
    self.service_endpoint = '{}:{}'.format(
        self.localstack.get_container_host_ip(),
        self.localstack.get_exposed_port(4568),
    )
    self.access_key = 'accesskey'
    self.secret_key = 'secretkey'
    self.region = 'us-east-1'
    self.partition_key = 'a'
    self.localstack.start()

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
              region=self.region,
              partition_key=self.partition_key,
          ))

  def run_kinesis_read_data(self):
    with TestPipeline() as p:
      result = (
          p
          | 'ReadFromKinesis' >> ReadDataFromKinesis(
              stream_name=self.stream_name,
              aws_access_key=self.aws_access_key,
              aws_secret_key=self.aws_secret_key,
              region=self.region,
              max_num_records=MAX_NUM_RECORDS,
              max_read_time=MAX_READ_TIME,
              initial_position_in_stream=INITIAL_POSITION_IN_STREAM,
          )
          | 'Count elements' >> Count.Globally())

      # There could already be some records in the stream so we can't ensure
      # that the ones we read are the ones we've just written so just count
      # them to verify that requested number of messages's been read.
      assert_that(result, equal_to([MAX_NUM_RECORDS]))

  def get_kinesis_options(self):
    p = TestPipeline()

    def get_required_option(option_name):
      option = p.get_option(option_name)
      if not option:
        logging.error('--%s argument is required.', option_name)
        sys.exit(1)
      return option

    self.stream_name = get_required_option('stream_name')
    self.aws_access_key = get_required_option('aws_access_key')
    self.aws_secret_key = get_required_option('aws_secret_key')
    self.region = get_required_option('aws_region')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
