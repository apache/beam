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

"""Unit test for PubSub verifier."""

from __future__ import absolute_import

import logging
import sys
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock
from hamcrest import assert_that as hc_assert_that

from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.testing.test_utils import PullResponseMessage
from apache_beam.testing.test_utils import create_pull_response

try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None


@unittest.skipIf(pubsub is None, 'PubSub dependencies are not installed.')
@mock.patch('time.sleep', return_value=None)
@mock.patch('google.cloud.pubsub.SubscriberClient')
class PubSubMatcherTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    self.mock_presult = mock.MagicMock()

  def init_matcher(self, expected_msg=None,
                   with_attributes=False, strip_attributes=None):
    self.pubsub_matcher = PubSubMessageMatcher(
        'mock_project', 'mock_sub_name', expected_msg,
        with_attributes=with_attributes, strip_attributes=strip_attributes)

  def init_counter_matcher(self, expected_msg_len=1):
    self.pubsub_matcher = PubSubMessageMatcher(
        'mock_project', 'mock_sub_name', expected_msg_len=expected_msg_len)

  def test_message_matcher_success(self, mock_get_sub, unsued_mock):
    self.init_matcher(expected_msg=[b'a', b'b'])
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [
        create_pull_response([PullResponseMessage(b'a', {})]),
        create_pull_response([PullResponseMessage(b'b', {})]),
    ]
    hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 2)
    self.assertEqual(mock_sub.acknowledge.call_count, 2)

  def test_message_matcher_attributes_success(self, mock_get_sub, unsued_mock):
    self.init_matcher(expected_msg=[PubsubMessage(b'a', {'k': 'v'})],
                      with_attributes=True)
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [
        create_pull_response([PullResponseMessage(b'a', {'k': 'v'})])
    ]
    hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)

  def test_message_matcher_attributes_fail(self, mock_get_sub, unsued_mock):
    self.init_matcher(expected_msg=[PubsubMessage(b'a', {})],
                      with_attributes=True)
    mock_sub = mock_get_sub.return_value
    # Unexpected attribute 'k'.
    mock_sub.pull.side_effect = [
        create_pull_response([PullResponseMessage(b'a', {'k': 'v'})])
    ]
    with self.assertRaisesRegex(AssertionError, r'Unexpected'):
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)

  def test_message_matcher_strip_success(self, mock_get_sub, unsued_mock):
    self.init_matcher(expected_msg=[PubsubMessage(b'a', {'k': 'v'})],
                      with_attributes=True,
                      strip_attributes=['id', 'timestamp'])
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [create_pull_response([
        PullResponseMessage(b'a', {'id': 'foo', 'timestamp': 'bar', 'k': 'v'})
    ])]
    hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)

  def test_message_matcher_strip_fail(self, mock_get_sub, unsued_mock):
    self.init_matcher(expected_msg=[PubsubMessage(b'a', {'k': 'v'})],
                      with_attributes=True,
                      strip_attributes=['id', 'timestamp'])
    mock_sub = mock_get_sub.return_value
    # Message is missing attribute 'timestamp'.
    mock_sub.pull.side_effect = [create_pull_response([
        PullResponseMessage(b'a', {'id': 'foo', 'k': 'v'})
    ])]
    with self.assertRaisesRegex(AssertionError, r'Stripped attributes'):
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)

  def test_message_matcher_mismatch(self, mock_get_sub, unused_mock):
    self.init_matcher(expected_msg=[b'a'])
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [
        create_pull_response([PullResponseMessage(b'c', {}),
                              PullResponseMessage(b'd', {})]),
    ]
    with self.assertRaises(AssertionError) as error:
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertCountEqual([b'c', b'd'], self.pubsub_matcher.messages)
    self.assertIn(
        '\nExpected: Expected 1 messages.\n     but: Got 2 messages.',
        str(error.exception.args[0]))
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)

  def test_message_matcher_timeout(self, mock_get_sub, unused_mock):
    self.init_matcher(expected_msg=[b'a'])
    mock_sub = mock_get_sub.return_value
    mock_sub.return_value.full_name.return_value = 'mock_sub'
    self.pubsub_matcher.timeout = 0.1
    with self.assertRaisesRegex(AssertionError, r'Expected 1.*\n.*Got 0'):
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertTrue(mock_sub.pull.called)
    self.assertEqual(mock_sub.acknowledge.call_count, 0)

  def test_message_count_matcher_below_fail(self, mock_get_sub, unused_mock):
    self.init_counter_matcher(expected_msg_len=1)
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [
        create_pull_response([PullResponseMessage(b'c', {}),
                              PullResponseMessage(b'd', {})]),
    ]
    with self.assertRaises(AssertionError) as error:
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertIn(
        '\nExpected: Expected 1 messages.\n     but: Got 2 messages.',
        str(error.exception.args[0]))

  def test_message_count_matcher_above_fail(self, mock_get_sub, unused_mock):
    self.init_counter_matcher(expected_msg_len=1)
    mock_sub = mock_get_sub.return_value
    self.pubsub_matcher.timeout = 0.1
    with self.assertRaisesRegex(AssertionError, r'Expected 1.*\n.*Got 0'):
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertTrue(mock_sub.pull.called)
    self.assertEqual(mock_sub.acknowledge.call_count, 0)

  def test_message_count_matcher_success(self, mock_get_sub, unused_mock):
    self.init_counter_matcher(expected_msg_len=15)
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [create_pull_response(
        [PullResponseMessage(
            b'a', {'foo': 'bar'})
         for _ in range(15)]
        )]
    hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertEqual(mock_sub.acknowledge.call_count, 1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
