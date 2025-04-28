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
Integration test for Python cross-language pipelines for Java SQSIO.

If you want to run the tests on localstack then run it just with pipeline
options.

To test it on a real AWS account you need to pass some additional params, e.g.:
python setup.py nosetests \
--tests=apache_beam.io.external.xlang_sqsio_it_test \
--test-pipeline-options="
  --use_real_aws
  --aws_sqs_topic=<SQS_TOPIC_URL>
  --aws_access_key=<AWS_ACCESS_KEY>
  --aws_secret_key=<AWS_SECRET_KEY>
  --aws_region=<AWS_REGION>
  --runner=DataflowRunner"
"""

import argparse
import logging
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import boto3
  from botocore.exceptions import ClientError
except ImportError:
  boto3 = None

try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

LOCALSTACK_VERSION = '3.8.1'
NUM_RECORDS = 10


@unittest.skipUnless(DockerContainer, 'testcontainers is not installed.')
@unittest.skipUnless(boto3, 'boto3 is not installed.')
@unittest.skipUnless(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner,
    'Do not run this test on precommit suites.')
class CrossLanguageSqsIOTest(unittest.TestCase):
  def test_sqs_read(self):
    messages = [f'data{str(i)}' for i in range(NUM_RECORDS)]
    self.sqs_helper.write_to_queue(messages)

    queue_url = self.sqs_helper.queue_url(self.aws_sqs_topic_name)

    provider = ExternalTransformProvider(
        BeamJarExpansionService(
            "sdks:java:io:amazon-web-services2:expansion-service:shadowJar"))

    with TestPipeline(options=self.options) as p:
      results = (
          p
          | 'Read messages from SQS' >> provider.SqsRead(
              queue_url=queue_url, max_num_records=NUM_RECORDS)
          | 'Keep only msg body' >> beam.Map(lambda msg: msg.body))
      assert_that(results, equal_to(messages))

  def set_localstack(self):
    self.localstack = (
        DockerContainer(f'localstack/localstack:{LOCALSTACK_VERSION}').
        with_exposed_ports(4566))

    # Repeat if ReadTimeout is raised.
    for i in range(4):
      try:
        self.localstack.start()
        break
      except Exception as e:  # pylint: disable=bare-except
        if i == 3:
          logging.error('Could not initialize localstack container')
          raise e

    return 'http://{}:{}'.format(
        self.localstack.get_container_host_ip(),
        self.localstack.get_exposed_port(4566))

  def setUp(self):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--aws_sqs_topic_name',
        default='beam_sqs_xlang',
        help='SQS topic name',
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

    pipeline = TestPipeline()
    argv = pipeline.get_full_options_as_args()

    known_args, _ = parser.parse_known_args(argv)

    self.aws_sqs_topic_name = known_args.aws_sqs_topic_name
    self.use_localstack = not known_args.use_real_aws

    aws_access_key = known_args.aws_access_key
    aws_secret_key = known_args.aws_secret_key
    aws_region = known_args.aws_region
    aws_service_endpoint = known_args.aws_service_endpoint

    if self.use_localstack:
      aws_service_endpoint = self.set_localstack()

    self.sqs_helper = SqsHelper(
        aws_access_key,
        aws_secret_key,
        aws_region,
        aws_service_endpoint,
    )

    if self.use_localstack:
      self.sqs_helper.create_topic(self.aws_sqs_topic_name)

    options = TestSqsReadOptions()
    options.aws_credentials_provider = {
        "@type": "StaticCredentialsProvider",
        "accessKeyId": aws_access_key,
        "secretAccessKey": aws_secret_key
    }
    options.aws_region = aws_region
    options.endpoint = aws_service_endpoint
    self.options = options

  def tearDown(self):
    if self.use_localstack:
      self.sqs_helper.delete_topic()
      try:
        self.localstack.stop()
      except:  # pylint: disable=bare-except
        logging.error('Could not stop the localstack container')


class TestSqsReadOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument("--endpoint", help="SQS URL", required=False)
    parser.add_argument(
        "--aws_credentials_provider",
        help=(
            "String JSON representation for the AWS credential provider,",
            "see Java SDK AwsOptions."),
        required=False)
    parser.add_argument(
        "--aws_region",
        help="AWS region in use, see Java SDK AwsOptions.",
        required=False)


class SqsHelper:
  def __init__(self, access_key, secret_key, region, service_endpoint):
    self.sqs_client = boto3.client(
        service_name='sqs',
        region_name=region,
        endpoint_url=service_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    self.queue = None

  def create_topic(self, queue_name):
    # localstack could not have initialized in the container yet so repeat
    retries = 10
    for i in range(retries):
      try:
        self.queue = self.sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={},
        )
        logging.info(
            "Created queue '%s' with URL=%s",
            queue_name,
            self.queue['QueueUrl'])
        break
      except Exception as e:
        if i == retries - 1:
          logging.error('Could not create SQS topic')
          raise e

  def queue_url(self, queue_name=None):
    if not self.queue:
      self.queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
    return self.queue['QueueUrl']

  def delete_topic(self):
    retries = 10
    for i in range(retries):
      try:
        self.sqs_client.delete_queue(QueueUrl=self.queue['QueueUrl'])
        break
      except Exception as e:
        if i == retries - 1:
          logging.error('Could not delete SQS topic')
          raise e

  def write_to_queue(self, messages):
    try:
      entries = [{
          "Id": str(ind),
          "MessageBody": msg,
          "MessageAttributes": {},
      } for ind,
                 msg in enumerate(messages)]
      response = self.sqs_client.send_message_batch(
          QueueUrl=self.queue['QueueUrl'], Entries=entries)
      if "Successful" in response:
        for msg_meta in response["Successful"]:
          logging.info(
              "Message sent: %s: %s", msg_meta["MessageId"], msg_meta["Id"])
      if "Failed" in response:
        for msg_meta in response["Failed"]:
          logging.warning(
              "Failed to send: %s: %s", msg_meta["MessageId"], msg_meta["Id"])
    except ClientError as error:
      logging.exception(
          "Send messages failed to queue: %s", self.queue['QueueUrl'])
      raise error
    else:
      return response


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
