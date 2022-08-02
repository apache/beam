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

# pytype: skip-file

import logging
import time
from collections import Counter

from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.io.gcp.pubsub import PubsubMessage

__all__ = ['PubSubMessageMatcher']

# Protect against environments where pubsub library is not available.
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None

DEFAULT_TIMEOUT = 5 * 60
DEFAULT_SLEEP_TIME = 1
DEFAULT_MAX_MESSAGES_IN_ONE_PULL = 50
DEFAULT_PULL_TIMEOUT = 30.0

_LOGGER = logging.getLogger(__name__)


class PubSubMessageMatcher(BaseMatcher):
  """Matcher that verifies messages from given subscription.

  This matcher can block the test and keep pulling messages from given
  subscription until all expected messages are shown or timeout.
  """
  def __init__(
      self,
      project,
      sub_name,
      expected_msg=None,
      expected_msg_len=None,
      timeout=DEFAULT_TIMEOUT,
      with_attributes=False,
      strip_attributes=None,
      sleep_time=DEFAULT_SLEEP_TIME,
      max_messages_in_one_pull=DEFAULT_MAX_MESSAGES_IN_ONE_PULL,
      pull_timeout=DEFAULT_PULL_TIMEOUT):
    """Initialize PubSubMessageMatcher object.

    Args:
      project: A name string of project.
      sub_name: A name string of subscription which is attached to output.
      expected_msg: A string list that contains expected message data pulled
        from the subscription. See also: with_attributes.
      expected_msg_len: Number of expected messages pulled from the
        subscription.
      timeout: Timeout in seconds to wait for all expected messages appears.
      with_attributes: If True, will match against both message data and
        attributes. If True, expected_msg should be a list of ``PubsubMessage``
        objects. Otherwise, it should be a list of ``bytes``.
      strip_attributes: List of strings. If with_attributes==True, strip the
        attributes keyed by these values from incoming messages.
        If a key is missing, will add an attribute with an error message as
        value to prevent a successful match.
      sleep_time: Time in seconds between which the pulls from pubsub are done.
      max_messages_in_one_pull: Maximum number of messages pulled from pubsub
        at once.
      pull_timeout: Time in seconds after which the pull from pubsub is repeated
    """
    if pubsub is None:
      raise ImportError('PubSub dependencies are not installed.')
    if not project:
      raise ValueError('Invalid project %s.' % project)
    if not sub_name:
      raise ValueError('Invalid subscription %s.' % sub_name)
    if not expected_msg_len and not expected_msg:
      raise ValueError(
          'Required expected_msg: {} or expected_msg_len: {}.'.format(
              expected_msg, expected_msg_len))
    if expected_msg and not isinstance(expected_msg, list):
      raise ValueError('Invalid expected messages %s.' % expected_msg)
    if expected_msg_len and not isinstance(expected_msg_len, int):
      raise ValueError('Invalid expected messages %s.' % expected_msg_len)

    self.project = project
    self.sub_name = sub_name
    self.expected_msg = expected_msg
    self.expected_msg_len = expected_msg_len or len(self.expected_msg)
    self.timeout = timeout
    self.messages = None
    self.messages_all_details = None
    self.with_attributes = with_attributes
    self.strip_attributes = strip_attributes
    self.sleep_time = sleep_time
    self.max_messages_in_one_pull = max_messages_in_one_pull
    self.pull_timeout = pull_timeout

  def _matches(self, _):
    if self.messages is None:
      self.messages, self.messages_all_details = self._wait_for_messages(
          self.expected_msg_len, self.timeout)
    if self.expected_msg:
      return Counter(self.messages) == Counter(self.expected_msg)
    else:
      return len(self.messages) == self.expected_msg_len

  def _wait_for_messages(self, expected_num, timeout):
    """Wait for messages from given subscription."""
    total_messages = []
    total_messages_all_details = []

    sub_client = pubsub.SubscriberClient()
    start_time = time.time()
    while time.time() - start_time <= timeout:
      response = sub_client.pull(
          subscription=self.sub_name,
          max_messages=self.max_messages_in_one_pull,
          timeout=self.pull_timeout)
      for rm in response.received_messages:
        msg = PubsubMessage._from_message(rm.message)
        full_message = (
            msg.data,
            msg.attributes,
            msg.attributes,
            msg.publish_time,
            msg.ordering_key)
        if not self.with_attributes:
          total_messages.append(msg.data)
          total_messages_all_details.append(full_message)
          continue

        if self.strip_attributes:
          for attr in self.strip_attributes:
            try:
              del msg.attributes[attr]
            except KeyError:
              msg.attributes[attr] = (
                  'PubSubMessageMatcher error: '
                  'expected attribute not found.')
        total_messages.append(msg)
        total_messages_all_details.append(full_message)

      ack_ids = [rm.ack_id for rm in response.received_messages]
      if ack_ids:
        sub_client.acknowledge(subscription=self.sub_name, ack_ids=ack_ids)
      if len(total_messages) >= expected_num:
        break
      time.sleep(self.sleep_time)

    if time.time() - start_time > timeout:
      _LOGGER.error(
          'Timeout after %d sec. Received %d messages from %s.',
          timeout,
          len(total_messages),
          self.sub_name)
    return total_messages, total_messages_all_details

  def describe_to(self, description):
    description.append_text('Expected %d messages.' % self.expected_msg_len)

  def describe_mismatch(self, _, mismatch_description):
    c_expected = Counter(self.expected_msg)
    c_actual = Counter(self.messages)
    mismatch_description.append_text("Got %d messages. " % (len(self.messages)))
    if self.expected_msg:
      expected = (c_expected - c_actual).items()
      unexpected = (c_actual - c_expected).items()
      unexpected_keys = [repr(item[0]) for item in unexpected]
      if self.with_attributes:
        unexpected_all_details = [
            x for x in self.messages_all_details
            if 'PubsubMessage(%s, %s)' % (repr(x[0]), x[1]) in unexpected_keys
        ]
      else:
        unexpected_all_details = [
            x for x in self.messages_all_details
            if repr(x[0]) in unexpected_keys
        ]
      mismatch_description.append_text(
          "Diffs (item, count):\n"
          "  Expected but not in actual: %s\n"
          "  Unexpected: %s\n"
          "  Unexpected (with all details): %s" %
          (expected, unexpected, unexpected_all_details))
    if self.with_attributes and self.strip_attributes:
      mismatch_description.append_text(
          '\n  Stripped attributes: %r' % self.strip_attributes)
