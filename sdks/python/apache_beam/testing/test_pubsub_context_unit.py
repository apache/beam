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
        context.register_subscription("projects/test-project/subscriptions/test-sub")

        self.assertIn("projects/test-project/topics/test-topic", context.tracked_topics)
        self.assertIn("projects/test-project/subscriptions/test-sub", context.tracked_subscriptions)

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
        context.subscriber.delete_subscription.assert_any_call(request={"subscription": "sub-1"})
        context.subscriber.delete_subscription.assert_any_call(request={"subscription": "cascade-sub-1"})
        context.publisher.delete_topic.assert_called_with(request={"topic": "topic-1"})

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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()