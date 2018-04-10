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

import logging
import unittest

import mock
from hamcrest import assert_that as hc_assert_that

from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher

# Protect against environments where pubsub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(pubsub is None, 'PubSub dependencies are not installed.')
class PubSubMatcherTest(unittest.TestCase):

  def setUp(self):
    self.mock_presult = mock.MagicMock()
    self.pubsub_matcher = PubSubMessageMatcher('mock_project',
                                               'mock_sub_name',
                                               ['mock_expected_msg'])

  @mock.patch('time.sleep', return_value=None)
  @mock.patch('apache_beam.io.gcp.tests.pubsub_matcher.'
              'PubSubMessageMatcher._get_subscription')
  def test_message_matcher_success(self, mock_get_sub, unsued_mock):
    self.pubsub_matcher.expected_msg = ['a', 'b']
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.side_effect = [
        [(1, pubsub.message.Message(b'a', 'unused_id'))],
        [(2, pubsub.message.Message(b'b', 'unused_id'))],
    ]
    hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 2)

  @mock.patch('time.sleep', return_value=None)
  @mock.patch('apache_beam.io.gcp.tests.pubsub_matcher.'
              'PubSubMessageMatcher._get_subscription')
  def test_message_matcher_mismatch(self, mock_get_sub, unused_mock):
    self.pubsub_matcher.expected_msg = ['a']
    mock_sub = mock_get_sub.return_value
    mock_sub.pull.return_value = [
        (1, pubsub.message.Message(b'c', 'unused_id')),
        (1, pubsub.message.Message(b'd', 'unused_id')),
    ]
    with self.assertRaises(AssertionError) as error:
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertEqual(mock_sub.pull.call_count, 1)
    self.assertItemsEqual(['c', 'd'], self.pubsub_matcher.messages)
    self.assertTrue(
        '\nExpected: Expected 1 messages.\n     but: Got 2 messages.'
        in str(error.exception.args[0]))

  @mock.patch('time.sleep', return_value=None)
  @mock.patch('apache_beam.io.gcp.tests.pubsub_matcher.'
              'PubSubMessageMatcher._get_subscription')
  def test_message_metcher_timeout(self, mock_get_sub, unused_mock):
    mock_sub = mock_get_sub.return_value
    mock_sub.return_value.full_name.return_value = 'mock_sub'
    self.pubsub_matcher.timeout = 0.1
    with self.assertRaises(AssertionError) as error:
      hc_assert_that(self.mock_presult, self.pubsub_matcher)
    self.assertTrue(mock_sub.pull.called)
    self.assertEqual(
        '\nExpected: Expected %d messages.\n     but: Got %d messages. Diffs: '
        '%s.\n' % (1, 0, ['mock_expected_msg']), str(error.exception.args[0]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
