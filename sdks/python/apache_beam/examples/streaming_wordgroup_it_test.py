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
"""End-to-end test for the streaming wordgroup example.

Code: beam/sdks/python/apache_beam/examples/streaming_wordgroup.py
Usage:

  python setup.py nosetests --test-pipeline-options=" \
      --runner=TestDirectRunner \
      --project=... \
      --staging_location=gs://... \
      --temp_location=gs://... \
      --sdk_location=... \

"""

from __future__ import absolute_import
from __future__ import print_function

import itertools
import json
import logging
import sys
import time
import unittest
import uuid
from builtins import range
from collections import Counter

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import streaming_wordgroup
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.tests import utils as gcp_test_utils
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

INPUT_TOPIC = 'wg_topic_input'
OUTPUT_TOPIC = 'wg_topic_output'
INPUT_SUB = 'wg_subscription_input'
OUTPUT_SUB = 'wg_subscription_output'


def generate_message(timestamp, dummy=False):
  data = (str(timestamp).replace(".", "'") if not dummy else "").encode("utf-8")
  attributes = {"ts": str(int(timestamp * 1000))}
  return PubsubMessage(data, attributes)


class PubsubMessagePublisherAndMatcher(BaseMatcher):
  """Matcher that publishes messages to a given topic and verifies messages
  from given subscription.

  This matcher can block the test and keep pulling messages from given
  subscription until all expected messages are shown or timeout.
  """

  def __init__(self, topic_path, sub_path, message_batches, expected_batches,
               timestamp_attribute, timeout):

    self.topic_path = topic_path
    self.sub_path = sub_path
    self.message_batches = message_batches
    self.expected_batches = expected_batches
    self.timestamp_attribute = timestamp_attribute
    self.timeout = timeout

    self._actual = []
    self._expected = []
    self._previous_match = None

  def _matches(self, _):
    if self._previous_match is False:
      return False

    from google.cloud import pubsub_v1

    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()

    assert len(self.message_batches) == len(self.expected_batches)
    for _ in range(len(self.message_batches)):
      inputs = self.message_batches.pop(0)
      expected = self.expected_batches.pop(0)

      gcp_test_utils.write_to_pubsub(
          pub_client, self.topic_path, inputs, with_attributes=True)

      produced = []
      start_time = time.time()
      max_timestamp = max(
          int(message.attributes[self.timestamp_attribute])
          for message in inputs)
      max_keepalive = PubsubMessage(
          data="".encode("utf-8"),
          attributes={self.timestamp_attribute: str(max_timestamp + 1000)})
      while ((time.time() - start_time) < self.timeout and
             len(produced) < len(expected)):
        gcp_test_utils.write_to_pubsub(
            pub_client, self.topic_path, [max_keepalive], with_attributes=True)
        produced += [
            m.data for m in gcp_test_utils.read_from_pubsub(
                sub_client,
                self.sub_path,
                with_attributes=True,
                number_of_elements=len(expected) - len(produced),
                timeout=5)
        ]

      self._actual = produced
      self._expected = expected
      if Counter(self._actual) != Counter(self._expected):
        self._previous_match = False
        return False
    return True

  def describe_to(self, description):
    description.append_text('Expected %d messages.' % len(self._expected))

  def describe_mismatch(self, _, mismatch_description):
    c_expected = Counter(self._expected)
    c_actual = Counter(self._actual)
    mismatch_description.append_text("Got {} messages.\n".format(
        len(self._actual)))
    for key, count in (c_actual - c_expected).items():
      mismatch_description.append_text("Actual has {} extra: {}\n".format(
          count, key))
    for key, count in (c_expected - c_actual).items():
      mismatch_description.append_text("Expected has {} extra: {}\n".format(
          count, key))


class StreamingWordGroupIT(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    self.maxDiff = None
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')
    self.result = None
    unique_suffix = "-" + uuid.uuid4().hex[:8]

    # Set up PubSub environment.
    from google.cloud import pubsub
    self.pub_client = pubsub.PublisherClient()
    self.input_topic = self.pub_client.create_topic(
        self.pub_client.topic_path(self.project, INPUT_TOPIC + unique_suffix))
    self.output_topic = self.pub_client.create_topic(
        self.pub_client.topic_path(self.project, OUTPUT_TOPIC + unique_suffix))

    self.sub_client = pubsub.SubscriberClient()
    self.input_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(self.project,
                                          INPUT_SUB + unique_suffix),
        self.input_topic.name)
    self.output_sub = self.sub_client.create_subscription(
        self.sub_client.subscription_path(self.project,
                                          OUTPUT_SUB + unique_suffix),
        self.output_topic.name,
        ack_deadline_seconds=60)

  def tearDown(self):
    if self.result is not None:
      self.result.cancel()
    test_utils.cleanup_subscriptions(self.sub_client,
                                     [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(self.pub_client,
                              [self.input_topic, self.output_topic])

  @attr('IT')
  def test_discard_late_data(self):
    window_size = 1  # In seconds, same as in a Beam pipeline

    starting_message_events = range(1000, 1010)
    starting_messages = [generate_message(i) for i in starting_message_events]
    starting_outputs = [
        json.dumps({
            "elements": [str(i)],
            "window_start_ts": int(i * 1000),
            "window_end_ts": int((i + window_size) * 1000)
        }).encode("utf-8") for i in starting_message_events
    ]

    current_message_events = range(2000, 2010)
    current_messages = [generate_message(i) for i in current_message_events]
    current_outputs = [
        json.dumps({
            "elements": [str(i)],
            "window_start_ts": int(i * 1000),
            "window_end_ts": int((i + window_size) * 1000)
        }).encode("utf-8") for i in current_message_events
    ]

    # Set extra options to the pipeline for test purpose
    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
    pubsub_msg_verifier = PubsubMessagePublisherAndMatcher(
        topic_path=self.input_topic.name,
        sub_path=self.output_sub.name,
        message_batches=[
            starting_messages, starting_messages + current_messages
        ],
        expected_batches=[starting_outputs, current_outputs],
        timestamp_attribute="ts",
        timeout=7 * 60)
    extra_opts = {
        'input_subscription': self.input_sub.name,
        'output_topic': self.output_topic.name,
        'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
    }

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    self.result = streaming_wordgroup.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))

  @attr('IT')
  def test_single_output_per_window(self):
    window_size = 1  # In seconds, same as in a Beam pipeline

    message_events = [i / 10.0 for i in range(999, 1010)]
    messages = [generate_message(i) for i in message_events]
    expected_outputs = [
        json.dumps({
            "elements": [str(e).replace(".", "'") for e in sorted(group)],
            "window_start_ts": int(i * 1000),
            "window_end_ts": int((i + window_size) * 1000)
        }).encode("utf-8") for i, group in itertools.groupby(
            message_events, key=lambda e: e // window_size)
    ]

    state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
    pubsub_msg_verifier = PubsubMessagePublisherAndMatcher(
        topic_path=self.input_topic.name,
        sub_path=self.output_sub.name,
        message_batches=[messages],
        expected_batches=[expected_outputs],
        timestamp_attribute="ts",
        timeout=7 * 60)
    extra_opts = {
        'input_subscription': self.input_sub.name,
        'output_topic': self.output_topic.name,
        'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
    }

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    self.result = streaming_wordgroup.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
