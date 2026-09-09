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

import logging
import unittest
from unittest.mock import MagicMock, patch

# Import the renamed class
from apache_beam.testing.pubsub_test_context import TestPubsubContext


class TestPubsubContextUnit(unittest.TestCase):

  # This patch replaces 'pubsub_v1' with a Mock object to avoid the
  # ImportError we set up in the __init__ method for environments without GCP.
  @patch('apache_beam.testing.pubsub_test_context.pubsub_v1')
  def test_context_initialization(self, mock_pubsub):
    context = TestPubsubContext(project_id="test-project", dry_run=True)

    self.assertEqual(context.project_id, "test-project")
    self.assertTrue(context.dry_run)
    self.assertEqual(context.tracked_topics, [])
    self.assertEqual(context.tracked_subscriptions, [])

  @patch('apache_beam.testing.pubsub_test_context.pubsub_v1')
  def test_register_topic_and_subscription(self, mock_pubsub):
    context = TestPubsubContext(project_id="test-project")

    context.register_topic("projects/test-project/topics/test-topic")
    context.register_subscription(
        "projects/test-project/subscriptions/test-sub")

    self.assertIn(
        "projects/test-project/topics/test-topic", context.tracked_topics)
    self.assertIn(
        "projects/test-project/subscriptions/test-sub",
        context.tracked_subscriptions)

  @patch('apache_beam.testing.pubsub_test_context.pubsub_v1')
  def test_context_manager_success_cleanup(self, mock_pubsub):
    """Tests that the manager cleans up resources if the test passes (dry_run=False)."""
    context = TestPubsubContext(project_id="test-project", dry_run=False)

    # Simulate registering a topic and a subscription
    context.register_topic("topic-1")
    context.register_subscription("sub-1")

    # Simulate GCP detecting a cascading subscription
    context.publisher.list_topic_subscriptions.return_value = ["cascade-sub-1"]

    # Execute the context without errors
    with context:
      pass

    # Verify that deletion commands were issued to GCP
    context.subscriber.delete_subscription.assert_any_call(
        request={"subscription": "sub-1"})
    context.subscriber.delete_subscription.assert_any_call(
        request={"subscription": "cascade-sub-1"})
    context.publisher.delete_topic.assert_called_with(
        request={"topic": "topic-1"})

  @patch('apache_beam.testing.pubsub_test_context.pubsub_v1')
  def test_context_manager_failure_skips_cleanup(self, mock_pubsub):
    """Tests that resources are NOT deleted if the test fails (exc_type is not None)."""
    context = TestPubsubContext(project_id="test-project", dry_run=False)
    context.register_topic("topic-1")

    try:
      with context:
        raise ValueError("Simulated test failure")
    except ValueError:
      pass

    # Since there is an error, deletion methods should NOT have been called
    context.publisher.delete_topic.assert_not_called()
    context.subscriber.delete_subscription.assert_not_called()

  @patch('apache_beam.testing.pubsub_test_context.pubsub_v1')
  def test_context_manager_stress_and_scale_cleanup(self, mock_pubsub):
    """STRESS TEST: Tests that the manager can scale, monitor, and clean up
    hundreds of concurrent topics and subscriptions safely and without leaks.
    """
    # Start the TestPubsubContext in active mode (dry_run=False) to simulate real GCP interactions
    context = TestPubsubContext(project_id="test-project", dry_run=False)

    total = 1000
    expected_topics = []
    expected_subscriptions = []

    # Bulk register 1000 topics and 1000 simulated parallel test subscriptions.
    for i in range(total):
      topic_path = f"projects/test-project/topics/stress-topic-{i}"
      sub_path = f"projects/test-project/subscriptions/stress-sub-{i}"

      context.register_topic(topic_path)
      context.register_subscription(sub_path)

      expected_topics.append(topic_path)
      expected_subscriptions.append(sub_path)

    # Verify that all resources were recorded in the monitor's memory without omissions.
    self.assertEqual(len(context.tracked_topics), total)
    self.assertEqual(len(context.tracked_subscriptions), total)
    self.assertEqual(context.tracked_topics, expected_topics)
    self.assertEqual(context.tracked_subscriptions, expected_subscriptions)

    # Configure the mock for the `list_topic_subscriptions` API to return an empty list.
    # by default to avoid infinite loops in the cascade simulation
    context.publisher.list_topic_subscriptions.return_value = []

    # Execute the mass dismantling phase
    with context:
      pass

    # VALIDATION OF MASS SUCCESSFUL DELETION IN GCP:
    # Verify that exactly 1000 unsubscribe calls have been issued.
    self.assertEqual(context.subscriber.delete_subscription.call_count, total)
    for sub in expected_subscriptions:
      context.subscriber.delete_subscription.assert_any_call(
          request={"subscription": sub})

    # Verify that exactly 1000 topic deletion calls have been issued.
    self.assertEqual(context.publisher.delete_topic.call_count, total)
    for topic in expected_topics:
      context.publisher.delete_topic.assert_any_call(request={"topic": topic})

    # Verify that the manager's memory is completely clean (0 tracked resources).
    self.assertEqual(len(context.tracked_topics), 0)
    self.assertEqual(len(context.tracked_subscriptions), 0)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
