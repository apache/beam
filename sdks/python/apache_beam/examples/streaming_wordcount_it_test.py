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

"""End-to-end test for the streaming wordcount example."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest
import uuid
from builtins import range

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import streaming_wordcount
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

INPUT_TOPIC = 'wc_topic_input'
OUTPUT_TOPIC = 'wc_topic_output'
INPUT_SUB = 'wc_subscription_input'
OUTPUT_SUB = 'wc_subscription_output'

DEFAULT_INPUT_NUMBERS = 500
WAIT_UNTIL_FINISH_DURATION = 6 * 60 * 1000  # in milliseconds


class StreamingWordCountIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
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
        self.output_topic.name,
        ack_deadline_seconds=60)

  def _inject_numbers(self, topic, num_messages):
    """Inject numbers as test data to PubSub."""
    logging.debug('Injecting %d numbers to topic %s', num_messages, topic.name)
    for n in range(num_messages):
      self.pub_client.publish(self.input_topic.name, str(n).encode('utf-8'))

  def tearDown(self):
    test_utils.cleanup_subscriptions(
        self.sub_client, [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(
        self.pub_client, [self.input_topic, self.output_topic])

  @attr('IT')
  def test_streaming_wordcount_it(self):
    # Build expected dataset.
    expected_msg = [('%d: 1' % num).encode('utf-8')
                    for num in range(DEFAULT_INPUT_NUMBERS)]

    # Set extra options to the pipeline for test purpose
    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project, self.output_sub.name, expected_msg, timeout=400)
    extra_opts = {
        'input_subscription': self.input_sub.name,
        'output_topic': self.output_topic.name,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION,
        'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
    }

    # Generate input data and inject to PubSub.
    self._inject_numbers(self.input_topic, DEFAULT_INPUT_NUMBERS)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    streaming_wordcount.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
