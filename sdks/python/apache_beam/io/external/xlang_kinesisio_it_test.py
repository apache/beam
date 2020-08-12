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

"""
Integration test for Python cross-language pipelines for Java KinesisIO.

If you want to run the tests on localstack then run it just with pipeline
options.

To test it on a real AWS account you need to pass some additional params, e.g.:
python setup.py nosetests \
--tests=apache_beam.io.external.xlang_kinesisio_it_test \
--test-pipeline-options="
  --use_real_aws
  --aws_kinesis_stream=<STREAM_NAME>
  --aws_access_key=<AWS_ACCESS_KEY>
  --aws_secret_key=<AWS_SECRET_KEY>
  --aws_region=<AWS_REGION>
  --runner=FlinkRunner"
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import time
import unittest
import uuid

import apache_beam as beam
from apache_beam.io.kinesis import InitialPositionInStream
from apache_beam.io.kinesis import ReadDataFromKinesis
from apache_beam.io.kinesis import WriteToKinesis
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import boto3
except ImportError:
  boto3 = None

try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

LOCALSTACK_VERSION = '0.11.3'
NUM_RECORDS = 10
NOW = time.time()
RECORD = b'record' + str(uuid.uuid4()).encode()


@unittest.skipUnless(DockerContainer, 'testcontainers is not installed.')
@unittest.skipUnless(boto3, 'boto3 is not installed.')
@unittest.skipUnless(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner,
    'Do not run this test on precommit suites.')
class CrossLanguageKinesisIOTest(unittest.TestCase):
  @unittest.skipUnless(
      TestPipeline().get_option('aws_kinesis_stream'),
      'Cannot test on real aws without pipeline options provided')
  def test_kinesis_io_roundtrip(self):
    # TODO: enable this test for localstack once BEAM-10664 is resolved
    self.run_kinesis_write()
    self.run_kinesis_read()

  @unittest.skipIf(
      TestPipeline().get_option('aws_kinesis_stream'),
      'Do not test on localstack when pipeline options were provided')
  def test_kinesis_write(self):
    # TODO: remove this test once BEAM-10664 is resolved
    self.run_kinesis_write()
    records = self.kinesis_helper.read_from_stream(self.aws_kinesis_stream)
    self.assertEqual(
        sorted(records),
        sorted([RECORD + str(i).encode() for i in range(NUM_RECORDS)]))

  def run_kinesis_write(self):
    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | 'Impulse' >> beam.Impulse()
          | 'Generate' >> beam.FlatMap(lambda x: range(NUM_RECORDS))  # pylint: disable=range-builtin-not-iterating
          | 'Map to bytes' >>
          beam.Map(lambda x: RECORD + str(x).encode()).with_output_types(bytes)
          | 'WriteToKinesis' >> WriteToKinesis(
              stream_name=self.aws_kinesis_stream,
              aws_access_key=self.aws_access_key,
              aws_secret_key=self.aws_secret_key,
              region=self.aws_region,
              service_endpoint=self.aws_service_endpoint,
              verify_certificate=(not self.use_localstack),
              partition_key='1',
              producer_properties=self.producer_properties,
          ))

  def run_kinesis_read(self):
    records = [RECORD + str(i).encode() for i in range(NUM_RECORDS)]

    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      result = (
          p
          | 'ReadFromKinesis' >> ReadDataFromKinesis(
              stream_name=self.aws_kinesis_stream,
              aws_access_key=self.aws_access_key,
              aws_secret_key=self.aws_secret_key,
              region=self.aws_region,
              service_endpoint=self.aws_service_endpoint,
              verify_certificate=not self.use_localstack,
              max_num_records=NUM_RECORDS,
              max_read_time=300,  # 5min
              initial_position_in_stream=InitialPositionInStream.AT_TIMESTAMP,
              initial_timestamp_in_stream=int(NOW),
          ).with_output_types(bytes))
      assert_that(result, equal_to(records))

  def set_localstack(self):
    self.localstack = DockerContainer('localstack/localstack:{}'
                                      .format(LOCALSTACK_VERSION))\
      .with_env('SERVICES', 'kinesis')\
      .with_env('KINESIS_PORT', '4568')\
      .with_env('USE_SSL', 'true')\
      .with_exposed_ports(4568)\
      .with_volume_mapping('/var/run/docker.sock', '/var/run/docker.sock', 'rw')

    # Repeat if ReadTimeout is raised.
    for i in range(4):
      try:
        self.localstack.start()
        break
      except Exception as e:  # pylint: disable=bare-except
        if i == 3:
          logging.error('Could not initialize localstack container')
          raise e

    self.aws_service_endpoint = 'https://{}:{}'.format(
        self.localstack.get_container_host_ip(),
        self.localstack.get_exposed_port('4568'),
    )

  def setUp(self):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--aws_kinesis_stream',
        default='beam_kinesis_xlang',
        help='Kinesis stream name',
    )
    parser.add_argument(
        '--aws_access_key',
        default='accesskey',
        help=('Aws access key'),
    )
    parser.add_argument(
        '--aws_secret_key',
        default='secretkey',
        help='Aws secret key',
    )
    parser.add_argument(
        '--aws_region',
        default='us-east-1',
        help='Aws region',
    )
    parser.add_argument(
        '--aws_service_endpoint',
        default=None,
        help='Url to external aws endpoint',
    )
    parser.add_argument(
        '--use_real_aws',
        default=False,
        dest='use_real_aws',
        action='store_true',
        help='Flag whether to use real aws for the tests purpose',
    )
    parser.add_argument(
        '--expansion_service',
        help='Url to externally launched expansion service.',
    )

    pipeline = TestPipeline()
    argv = pipeline.get_full_options_as_args()

    known_args, self.pipeline_args = parser.parse_known_args(argv)

    self.aws_kinesis_stream = known_args.aws_kinesis_stream
    self.aws_access_key = known_args.aws_access_key
    self.aws_secret_key = known_args.aws_secret_key
    self.aws_region = known_args.aws_region
    self.aws_service_endpoint = known_args.aws_service_endpoint
    self.use_localstack = not known_args.use_real_aws
    self.expansion_service = known_args.expansion_service
    self.producer_properties = {
        'CollectionMaxCount': str(NUM_RECORDS),
        'ConnectTimeout': str(MAX_READ_TIME),
    }

    if self.use_localstack:
      self.set_localstack()

    self.kinesis_helper = KinesisHelper(
        self.aws_access_key,
        self.aws_secret_key,
        self.aws_region,
        self.aws_service_endpoint.replace('https', 'http')
        if self.aws_service_endpoint else None,
    )

    if self.use_localstack:
      self.kinesis_helper.create_stream(self.aws_kinesis_stream)

  def tearDown(self):
    if self.use_localstack:
      self.kinesis_helper.delete_stream(self.aws_kinesis_stream)

      try:
        self.localstack.stop()
      except:  # pylint: disable=bare-except
        logging.error('Could not stop the localstack container')


class KinesisHelper:
  def __init__(self, access_key, secret_key, region, service_endpoint):
    self.kinesis_client = boto3.client(
        service_name='kinesis',
        region_name=region,
        endpoint_url=service_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

  def create_stream(self, stream_name):
    # localstack could not have initialized in the container yet so repeat
    retries = 10
    for i in range(retries):
      try:
        self.kinesis_client.create_stream(
            StreamName=stream_name,
            ShardCount=1,
        )
        time.sleep(2)
        break
      except:  # pylint: disable=bare-except
        if i == retries - 1:
          logging.error('Could not create kinesis stream')
          raise

    # Wait for the stream to be active
    self.get_first_shard_id(stream_name)

  def delete_stream(self, stream_name):
    self.kinesis_client.delete_stream(
        StreamName=stream_name,
        EnforceConsumerDeletion=True,
    )

  def get_first_shard_id(self, stream_name):
    retries = 10
    stream = self.kinesis_client.describe_stream(StreamName=stream_name)
    for i in range(retries):
      if stream['StreamDescription']['StreamStatus'] == 'ACTIVE':
        break
      time.sleep(2)
      if i == retries - 1:
        logging.error('Could not initialize kinesis stream')
        raise
      stream = self.kinesis_client.describe_stream(StreamName=stream_name)

    return stream['StreamDescription']['Shards'][0]['ShardId']

  def read_from_stream(self, stream_name):
    shard_id = self.get_first_shard_id(stream_name)

    shard_iterator = self.kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=InitialPositionInStream.AT_TIMESTAMP,
        Timestamp=str(NOW),
    )

    result = self.kinesis_client.get_records(
        ShardIterator=shard_iterator['ShardIterator'],
        Limit=NUM_RECORDS,
    )

    return [record['Data'] for record in result['Records']]


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
