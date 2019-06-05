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
Integration test for Google Cloud Pub/Sub.
"""
from __future__ import absolute_import

import logging
import unittest
import uuid

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp import pubsub_it_pipeline
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

INPUT_TOPIC = 'psit_topic_input'
OUTPUT_TOPIC = 'psit_topic_output'
INPUT_SUB = 'psit_subscription_input'
OUTPUT_SUB = 'psit_subscription_output'

# How long TestXXXRunner will wait for pubsub_it_pipeline to run before
# cancelling it.
TEST_PIPELINE_DURATION_MS = 3 * 60 * 1000
# How long PubSubMessageMatcher will wait for the correct set of messages to
# appear.
MESSAGE_MATCHER_TIMEOUT_S = 5 * 60


class PubSubIntegrationTest(unittest.TestCase):

  ID_LABEL = 'id'
  TIMESTAMP_ATTRIBUTE = 'timestamp'
  INPUT_MESSAGES = {
      # TODO(BEAM-4275): DirectRunner doesn't support reading or writing
      # label_ids, nor writing timestamp attributes. Once these features exist,
      # TestDirectRunner and TestDataflowRunner should behave identically.
      'TestDirectRunner': [
          PubsubMessage(b'data001', {}),
          # For those elements that have the TIMESTAMP_ATTRIBUTE attribute, the
          # IT pipeline writes back the timestamp of each element (as reported
          # by Beam), as a TIMESTAMP_ATTRIBUTE + '_out' attribute.
          PubsubMessage(b'data002', {
              TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
          }),
      ],
      'TestDataflowRunner': [
          # Use ID_LABEL attribute to deduplicate messages with the same ID.
          PubsubMessage(b'data001', {ID_LABEL: 'foo'}),
          PubsubMessage(b'data001', {ID_LABEL: 'foo'}),
          PubsubMessage(b'data001', {ID_LABEL: 'foo'}),
          # For those elements that have the TIMESTAMP_ATTRIBUTE attribute, the
          # IT pipeline writes back the timestamp of each element (as reported
          # by Beam), as a TIMESTAMP_ATTRIBUTE + '_out' attribute.
          PubsubMessage(b'data002', {
              TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
          })
      ],
  }
  EXPECTED_OUTPUT_MESSAGES = {
      'TestDirectRunner': [
          PubsubMessage(b'data001-seen', {'processed': 'IT'}),
          PubsubMessage(b'data002-seen', {
              TIMESTAMP_ATTRIBUTE: '2018-07-11T02:02:50.149000Z',
              TIMESTAMP_ATTRIBUTE + '_out': '2018-07-11T02:02:50.149000Z',
              'processed': 'IT',
          }),
      ],
      'TestDataflowRunner': [
          PubsubMessage(b'data001-seen', {'processed': 'IT'}),
          PubsubMessage(b'data002-seen', {
              TIMESTAMP_ATTRIBUTE + '_out': '2018-07-11T02:02:50.149000Z',
              'processed': 'IT',
          }),
      ],
  }

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')
    self.uuid = str(uuid.uuid4())

    # Set up PubSub environment.
    from google.cloud import pubsub
    self.pub_client = pubsub.PublisherClient()
    self.input_topic = self.pub_client.create_topic(
        self.pub_client.topic_path(self.project, INPUT_TOPIC + self.uuid))
    self.output_topic = self.pub_client.create_topic(
        self.pub_client.topic_path(self.project, OUTPUT_TOPIC + self.uuid))

    self.sub_client = pubsub.SubscriberClient()
    self.input_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(self.project, INPUT_SUB + self.uuid),
        self.input_topic.name)
    self.output_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(self.project, OUTPUT_SUB + self.uuid),
        self.output_topic.name)

  def tearDown(self):
    test_utils.cleanup_subscriptions(self.sub_client,
                                     [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(self.pub_client,
                              [self.input_topic, self.output_topic])

  def _test_streaming(self, with_attributes):
    """Runs IT pipeline with message verifier.

    Args:
      with_attributes: False - Reads and writes message data only.
        True - Reads and writes message data and attributes. Also verifies
        id_label and timestamp_attribute features.
    """
    # Set on_success_matcher to verify pipeline state and pubsub output. These
    # verifications run on a (remote) worker.

    # Expect the state to be RUNNING since a streaming pipeline is usually
    # never DONE. The test runner will cancel the pipeline after verification.
    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
    expected_messages = self.EXPECTED_OUTPUT_MESSAGES[self.runner_name]
    if not with_attributes:
      expected_messages = [pubsub_msg.data.decode('utf-8')
                           for pubsub_msg in expected_messages]
    if self.runner_name == 'TestDirectRunner':
      strip_attributes = None
    else:
      strip_attributes = [self.ID_LABEL, self.TIMESTAMP_ATTRIBUTE]
    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project,
        self.output_sub.name,
        expected_messages,
        timeout=MESSAGE_MATCHER_TIMEOUT_S,
        with_attributes=with_attributes,
        strip_attributes=strip_attributes)
    extra_opts = {'input_subscription': self.input_sub.name,
                  'output_topic': self.output_topic.name,
                  'wait_until_finish_duration': TEST_PIPELINE_DURATION_MS,
                  'on_success_matcher': all_of(state_verifier,
                                               pubsub_msg_verifier)}

    # Generate input data and inject to PubSub.
    for msg in self.INPUT_MESSAGES[self.runner_name]:
      self.pub_client.publish(self.input_topic.name, msg.data, **msg.attributes)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    pubsub_it_pipeline.run_pipeline(
        argv=self.test_pipeline.get_full_options_as_args(**extra_opts),
        with_attributes=with_attributes,
        id_label=self.ID_LABEL,
        timestamp_attribute=self.TIMESTAMP_ATTRIBUTE)

  @attr('IT')
  def test_streaming_data_only(self):
    self._test_streaming(with_attributes=False)

  @attr('IT')
  def test_streaming_with_attributes(self):
    self._test_streaming(with_attributes=True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
