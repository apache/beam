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

"""Unit test for the streaming wordcount example with debug."""

# pytype: skip-file

import unittest

import mock
import pytest

import apache_beam as beam
from apache_beam.examples import streaming_wordcount_debugging
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where the PubSub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position


@pytest.mark.examples_postcommit
class StreamingWordcountDebugging(unittest.TestCase):
  @unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
  @mock.patch('apache_beam.io.ReadFromPubSub')
  @mock.patch('apache_beam.io.WriteToPubSub')
  def test_streaming_wordcount_debugging(self, *unused_mocks):
    def FakeReadFromPubSub(topic=None, subscription=None, values=None):
      expected_topic = topic
      expected_subscription = subscription

      def _inner(topic=None, subscription=None):
        assert topic == expected_topic
        assert subscription == expected_subscription
        return TestStream().add_elements(values)

      return _inner

    class AssertTransform(beam.PTransform):
      def __init__(self, matcher):
        self.matcher = matcher

      def expand(self, pcoll):
        assert_that(pcoll, self.matcher)

    def FakeWriteToPubSub(topic=None, values=None):
      expected_topic = topic

      def _inner(topic=None, subscription=None):
        assert topic == expected_topic
        return AssertTransform(equal_to(values))

      return _inner

    input_topic = 'projects/fake-beam-test-project/topic/intopic'
    input_values = [
        '150', '151', '152', '153', '154', '210', '211', '212', '213', '214'
    ]
    output_topic = 'projects/fake-beam-test-project/topic/outtopic'
    output_values = [
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
    beam.io.ReadFromPubSub = (
        FakeReadFromPubSub(
            topic=input_topic,
            values=list(x.encode('utf-8') for x in input_values)))
    beam.io.WriteToPubSub = (
        FakeWriteToPubSub(
            topic=output_topic,
            values=list(x.encode('utf-8') for x in output_values)))
    streaming_wordcount_debugging.run([
        '--input_topic',
        'projects/fake-beam-test-project/topic/intopic',
        '--output_topic',
        'projects/fake-beam-test-project/topic/outtopic'
    ],
                                      save_main_session=False)


if __name__ == '__main__':
  unittest.main()
