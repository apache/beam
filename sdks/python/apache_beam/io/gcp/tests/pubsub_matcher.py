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

from __future__ import absolute_import

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
MAX_MESSAGES_IN_ONE_PULL = 50


class PubSubMessageMatcher(BaseMatcher):
  """Matcher that verifies messages from given subscription.

  This matcher can block the test and keep pulling messages from given
  subscription until all expected messages are shown or timeout.
  """

  def __init__(self, project, sub_name, expected_msg,
               timeout=DEFAULT_TIMEOUT, with_attributes=False,
               strip_attributes=None):
    """Initialize PubSubMessageMatcher object.

    Args:
      project: A name string of project.
      sub_name: A name string of subscription which is attached to output.
      expected_msg: A string list that contains expected message data pulled
        from the subscription. See also: with_attributes.
      timeout: Timeout in seconds to wait for all expected messages appears.
      with_attributes: If True, will match against both message data and
        attributes. If True, expected_msg should be a list of ``PubsubMessage``
        objects. Otherwise, it should be a list of ``bytes``.
      strip_attributes: List of strings. If with_attributes==True, strip the
        attributes keyed by these values from incoming messages.
        If a key is missing, will add an attribute with an error message as
        value to prevent a successful match.
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
    self.with_attributes = with_attributes
    self.strip_attributes = strip_attributes

  def _matches(self, _):
    if self.messages is None:
      self.messages = self._wait_for_messages(len(self.expected_msg),
                                              self.timeout)
    return Counter(self.messages) == Counter(self.expected_msg)

  def _wait_for_messages(self, expected_num, timeout):
    """Wait for messages from given subscription."""
    total_messages = []

    sub_client = pubsub.SubscriberClient()
    start_time = time.time()
    while time.time() - start_time <= timeout:
      response = sub_client.pull(self.sub_name,
                                 max_messages=MAX_MESSAGES_IN_ONE_PULL,
                                 return_immediately=True)
      for rm in response.received_messages:
        msg = PubsubMessage._from_message(rm.message)
        if not self.with_attributes:
          total_messages.append(msg.data.decode('utf-8'))
          continue

        if self.strip_attributes:
          for attr in self.strip_attributes:
            try:
              del msg.attributes[attr]
            except KeyError:
              msg.attributes[attr] = ('PubSubMessageMatcher error: '
                                      'expected attribute not found.')
        total_messages.append(msg)

      ack_ids = [rm.ack_id for rm in response.received_messages]
      if ack_ids:
        sub_client.acknowledge(self.sub_name, ack_ids)
      if len(total_messages) >= expected_num:
        break
      time.sleep(1)

    if time.time() - start_time > timeout:
      logging.error('Timeout after %d sec. Received %d messages from %s.',
                    timeout, len(total_messages), self.sub_name)
    return total_messages

  def describe_to(self, description):
    description.append_text(
        'Expected %d messages.' % len(self.expected_msg))

  def describe_mismatch(self, _, mismatch_description):
    c_expected = Counter(self.expected_msg)
    c_actual = Counter(self.messages)
    mismatch_description.append_text(
        "Got %d messages. "
        "Diffs (item, count):\n"
        "  Expected but not in actual: %s\n"
        "  Unexpected: %s" % (
            len(self.messages), (c_expected - c_actual).items(),
            (c_actual - c_expected).items()))
    if self.with_attributes and self.strip_attributes:
      mismatch_description.append_text(
          '\n  Stripped attributes: %r' % self.strip_attributes)
