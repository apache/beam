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

"""PubSub verifier used for end-to-end test."""

import logging
import time
from collections import Counter

from hamcrest.core.base_matcher import BaseMatcher

__all__ = ['PubSubMessageMatcher']


# Protect against environments where pubsub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position

DEFAULT_TIMEOUT = 5 * 60
MAX_MESSAGES_IN_ONE_PULL = 50


class PubSubMessageMatcher(BaseMatcher):
  """Matcher that verifies messages from given subscription.

  This matcher can block the test and keep pulling messages from given
  subscription until all expected messages are shown or timeout.
  """

  def __init__(self, project, sub_name, expected_msg, timeout=DEFAULT_TIMEOUT):
    """Initialize PubSubMessageMatcher object.

    Args:
      project: A name string of project.
      sub_name: A name string of subscription which is attached to output.
      expected_msg: A string list that contains expected message data pulled
        from the subscription.
      timeout: Timeout in seconds to wait for all expected messages appears.
    """
    if pubsub is None:
      raise ImportError(
          'PubSub dependencies are not installed.')
    if not project:
      raise ValueError('Invalid project %s.' % project)
    if not sub_name:
      raise ValueError('Invalid subscription %s.' % sub_name)
    if not isinstance(expected_msg, list):
      raise ValueError('Invalid expected messages %s.' % expected_msg)

    self.project = project
    self.sub_name = sub_name
    self.expected_msg = expected_msg
    self.timeout = timeout
    self.messages = None

  def _matches(self, _):
    if self.messages is None:
      self.messages = self._wait_for_messages(self._get_subscription(),
                                              len(self.expected_msg),
                                              self.timeout)
    return Counter(self.messages) == Counter(self.expected_msg)

  def _get_subscription(self):
    return pubsub.Client(project=self.project).subscription(self.sub_name)

  def _wait_for_messages(self, subscription, expected_num, timeout):
    """Wait for messages from given subscription."""
    logging.debug('Start pulling messages from %s', subscription.full_name)
    total_messages = []
    start_time = time.time()
    while time.time() - start_time <= timeout:
      pulled = subscription.pull(max_messages=MAX_MESSAGES_IN_ONE_PULL)
      for ack_id, message in pulled:
        total_messages.append(message.data)
        subscription.acknowledge([ack_id])
      if len(total_messages) >= expected_num:
        return total_messages
      time.sleep(1)

    logging.error('Timeout after %d sec. Received %d messages from %s.',
                  timeout, len(total_messages), subscription.full_name)
    return total_messages

  def describe_to(self, description):
    description.append_text(
        'Expected %d messages.' % len(self.expected_msg))

  def describe_mismatch(self, _, mismatch_description):
    c_expected = Counter(self.expected_msg)
    c_actual = Counter(self.messages)
    diff = (c_expected | c_actual) - (c_expected & c_actual)
    mismatch_description.append_text(
        "Got %d messages. Diffs: %s." %
        (len(self.messages), list(diff.elements())))
