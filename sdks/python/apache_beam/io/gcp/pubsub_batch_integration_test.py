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
Integration test for Google Cloud Pub/Sub WriteToPubSub in batch mode.
"""
# pytype: skip-file

import logging
import time
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing import test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.core import Create

OUTPUT_TOPIC = 'psit_batch_topic_output'
OUTPUT_SUB = 'psit_batch_subscription_output'

# How long TestDataflowRunner will wait for batch pipeline to complete
TEST_PIPELINE_DURATION_MS = 10 * 60 * 1000
# How long PubSubMessageMatcher will wait for the correct set of messages to appear
MESSAGE_MATCHER_TIMEOUT_S = 5 * 60


class PubSubBatchIntegrationTest(unittest.TestCase):
  """Integration test for WriteToPubSub in batch mode with DataflowRunner."""

  # Test data for batch processing
  INPUT_MESSAGES = [
      b'batch_data001',
      b'batch_data002',
      b'batch_data003\xab\xac',
      b'batch_data004\xab\xac'
  ]

  EXPECTED_OUTPUT_MESSAGES = [
      PubsubMessage(b'batch_data001-processed', {'batch_job': 'true'}),
      PubsubMessage(b'batch_data002-processed', {'batch_job': 'true'}),
      PubsubMessage(b'batch_data003\xab\xac-processed', {'batch_job': 'true'}),
      PubsubMessage(b'batch_data004\xab\xac-processed', {'batch_job': 'true'})
  ]

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')
    self.uuid = str(uuid.uuid4())

    # Set up PubSub environment.
    from google.cloud import pubsub
    self.pub_client = pubsub.PublisherClient()
    self.output_topic = self.pub_client.create_topic(
        name=self.pub_client.topic_path(self.project, OUTPUT_TOPIC + self.uuid))

    self.sub_client = pubsub.SubscriberClient()
    self.output_sub = self.sub_client.create_subscription(
        name=self.sub_client.subscription_path(
            self.project, OUTPUT_SUB + self.uuid),
        topic=self.output_topic.name)
    # Add a 30 second sleep after resource creation to ensure subscriptions
    # will receive messages.
    time.sleep(30)

  def tearDown(self):
    test_utils.cleanup_subscriptions(self.sub_client, [self.output_sub])
    test_utils.cleanup_topics(self.pub_client, [self.output_topic])

  def _test_batch_write(self, with_attributes):
    """Runs batch IT pipeline with WriteToPubSub.

    Args:
      with_attributes: False - Writes message data only.
        True - Writes message data and attributes.
    """
    # Set up pipeline options for batch mode
    pipeline_options = PipelineOptions(
        self.test_pipeline.get_full_options_as_args())
    pipeline_options.view_as(StandardOptions).streaming = False  # Batch mode

    expected_messages = self.EXPECTED_OUTPUT_MESSAGES
    if not with_attributes:
      expected_messages = [pubsub_msg.data for pubsub_msg in expected_messages]

    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project,
        self.output_sub.name,
        expected_messages,
        timeout=MESSAGE_MATCHER_TIMEOUT_S,
        with_attributes=with_attributes)

    with beam.Pipeline(options=pipeline_options) as p:
      # Create input data
      input_data = p | 'CreateInput' >> Create(self.INPUT_MESSAGES)

      # Process data
      if with_attributes:

        def add_batch_attributes(data):
          return PubsubMessage(data + b'-processed', {'batch_job': 'true'})

        processed_data = (
            input_data | 'AddAttributes' >> beam.Map(add_batch_attributes))
      else:
        processed_data = (
            input_data | 'ProcessData' >> beam.Map(lambda x: x + b'-processed'))

      # Write to PubSub using WriteToPubSub in batch mode
      _ = processed_data | 'WriteToPubSub' >> WriteToPubSub(
          self.output_topic.name, with_attributes=with_attributes)

    # Verify the results
    pubsub_msg_verifier.verify()

  @pytest.mark.it_postcommit
  def test_batch_write_data_only(self):
    """Test WriteToPubSub in batch mode with data only."""
    if self.runner_name != 'TestDataflowRunner':
      self.skipTest('This test is specifically for DataflowRunner batch mode')
    self._test_batch_write(with_attributes=False)

  @pytest.mark.it_postcommit
  def test_batch_write_with_attributes(self):
    """Test WriteToPubSub in batch mode with attributes."""
    if self.runner_name != 'TestDataflowRunner':
      self.skipTest('This test is specifically for DataflowRunner batch mode')
    self._test_batch_write(with_attributes=True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
