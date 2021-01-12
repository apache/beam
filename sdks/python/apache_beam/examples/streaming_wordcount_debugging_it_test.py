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

"""End-to-end test for the streaming wordcount example with debug."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest
import uuid

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import streaming_wordcount_debugging
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

INPUT_TOPIC = 'wc_topic_input'
OUTPUT_TOPIC = 'wc_topic_output'
INPUT_SUB = 'wc_subscription_input'
OUTPUT_SUB = 'wc_subscription_output'

SAMPLE_MESSAGES = [
    '150', '151', '152', '153', '154', '210', '211', '212', '213', '214'
]
EXPECTED_MESSAGE = [
    '150: 1',
    '151: 1',
    '152: 1',
    '153: 1',
    '154: 1',
    '210: 1',
    '211: 1',
    '212: 1',
    '213: 1',
    '214: 1'
]
WAIT_UNTIL_FINISH_DURATION = 6 * 60 * 1000  # in milliseconds


class StreamingWordcountDebuggingIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')
    self.setup_pubsub()

  def setup_pubsub(self):
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

  def _inject_data(self, topic, data):
    """Inject numbers as test data to PubSub."""
    logging.debug('Injecting test data to topic %s', topic.name)
    for n in data:
      self.pub_client.publish(self.input_topic.name, str(n).encode('utf-8'))

  def tearDown(self):
    test_utils.cleanup_subscriptions(
        self.sub_client, [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(
        self.pub_client, [self.input_topic, self.output_topic])

  @attr('IT')
  @unittest.skip(
      "Skipped due to [BEAM-3377]: assert_that not working for streaming")
  def test_streaming_wordcount_debugging_it(self):

    # Set extra options to the pipeline for test purpose
    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
    pubsub_msg_verifier = PubSubMessageMatcher(
        self.project, self.output_sub.name, EXPECTED_MESSAGE, timeout=400)
    extra_opts = {
        'input_subscription': self.input_sub.name,
        'output_topic': self.output_topic.name,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION,
        'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
    }

    # Generate input data and inject to PubSub.
    self._inject_data(self.input_topic, SAMPLE_MESSAGES)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    streaming_wordcount_debugging.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
