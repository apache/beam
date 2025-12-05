#!/usr/bin/env python
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

import datetime
import unittest
import io
import sys
from unittest import mock
from stale_cleaner import (
    GoogleCloudResource,
    StaleCleaner,
    PubSubTopicCleaner,
    PubSubSubscriptionCleaner,
    FakeClock,
    DEFAULT_TIME_THRESHOLD,
    PUBSUB_TOPIC_RESOURCE,
    STORAGE_PREFIX
)


class SilencedMock(mock.MagicMock):
    """A MagicMock that doesn't print anything when called."""
    def __call__(self, *args, **kwargs):
        with mock.patch('sys.stdout', new=io.StringIO()):
            with mock.patch('sys.stderr', new=io.StringIO()):
                return super(SilencedMock, self).__call__(*args, **kwargs)

# Use this context manager to silence print statements
class SilencePrint:
    """Context manager to silence print statements."""
    def __enter__(self):
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr


class GoogleCloudResourceTest(unittest.TestCase):
    """Tests for the GoogleCloudResource class."""

    def test_init_with_default_values(self):
        """Test initialization with default values."""
        test_time_str = "2025-05-10T12:00:00"
        fake_clock = FakeClock(test_time_str)

        resource = GoogleCloudResource(resource_name="test-resource", clock=fake_clock)

        self.assertEqual(resource.resource_name, "test-resource")
        self.assertEqual(resource.creation_date, fake_clock())
        self.assertEqual(resource.last_update_date, fake_clock())

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        creation_date = datetime.datetime(2025, 1, 1, 12, 0, 0)
        update_date = datetime.datetime(2025, 5, 1, 12, 0, 0)
        fake_clock = FakeClock("2025-05-10T12:00:00")

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date,
            last_update_date=update_date,
            clock=fake_clock
        )

        self.assertEqual(resource.resource_name, "test-resource")
        self.assertEqual(resource.creation_date, creation_date)
        self.assertEqual(resource.last_update_date, update_date)

    def test_str_representation(self):
        """Test string representation of the resource."""
        resource = GoogleCloudResource(resource_name="test-resource")
        self.assertEqual(str(resource), "test-resource")

    def test_to_dict(self):
        """Test conversion to dictionary."""
        creation_date = datetime.datetime(2025, 1, 1, 12, 0, 0)
        update_date = datetime.datetime(2025, 5, 1, 12, 0, 0)

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date,
            last_update_date=update_date
        )

        expected_dict = {
            "resource_name": "test-resource",
            "creation_date": "2025-01-01T12:00:00",
            "last_update_date": "2025-05-01T12:00:00"
        }

        self.assertEqual(resource.to_dict(), expected_dict)

    def test_update(self):
        """Test update method."""
        creation_date = datetime.datetime(2025, 1, 1, 12, 0, 0)
        initial_update_date = datetime.datetime(2025, 5, 1, 12, 0, 0)
        fake_clock_initial = FakeClock("2025-01-01T12:00:00")

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date,
            last_update_date=initial_update_date,
            clock=fake_clock_initial
        )
        self.assertEqual(resource.last_update_date, initial_update_date)

        # Test with provided clock
        fake_clock_new_update = FakeClock("2025-05-10T12:00:00")
        resource.update(clock=fake_clock_new_update)
        self.assertEqual(resource.last_update_date, fake_clock_new_update())

        current_last_update = resource.last_update_date
        resource.update()
        self.assertNotEqual(resource.last_update_date, current_last_update)

    def test_time_alive(self):
        """Test time_alive method."""
        creation_time_str = "2025-01-01T12:00:00"
        current_time_str = "2025-01-02T12:00:00"  # 1 day later
        fake_creation_clock = FakeClock(creation_time_str)

        resource = GoogleCloudResource(
            resource_name="test-resource",
            clock=fake_creation_clock
        )
        # Ensure creation_date is set by the clock during init
        self.assertEqual(resource.creation_date, fake_creation_clock())

        # Test with provided clock
        fake_current_clock = FakeClock(current_time_str)
        self.assertEqual(resource.time_alive(clock=fake_current_clock), 86400)  # 1 day in seconds

class MockStaleCleaner(StaleCleaner):
    """A mock implementation of StaleCleaner for testing."""

    def __init__(self, project_id, resource_type, bucket_name, prefixes=None,
                 time_threshold=DEFAULT_TIME_THRESHOLD, active_resources=None,
                 stored_resources=None, clock=None):
        super().__init__(project_id, resource_type, bucket_name, prefixes, time_threshold, clock)
        self._active_resources_mock_data = active_resources or {}
        self._stored_resources_mock_data = stored_resources or {}
        self.deleted_resources = []

    def _active_resources(self):
        """Override the private method to return mock data."""
        processed_active_resources = {}
        for k, v in self._active_resources_mock_data.items():
            if not isinstance(v, GoogleCloudResource):
                processed_active_resources[k] = GoogleCloudResource(resource_name=k, clock=self.clock)
            else:
                processed_active_resources[k] = v
        return processed_active_resources

    def _stored_resources(self):
        """Override the private method to return mock data."""
        processed_stored_resources = {}
        for k, v_data in self._stored_resources_mock_data.items():
            if isinstance(v_data, GoogleCloudResource):
                processed_stored_resources[k] = v_data
            elif isinstance(v_data, dict) and "resource_name" in v_data and "creation_date" in v_data and "last_update_date" in v_data:
                processed_stored_resources[k] = GoogleCloudResource(
                    resource_name=v_data["resource_name"],
                    creation_date=datetime.datetime.fromisoformat(v_data["creation_date"]),
                    last_update_date=datetime.datetime.fromisoformat(v_data["last_update_date"]),
                    clock=self.clock # Ensure clock is passed if re-hydrating
                )
            else:
                processed_stored_resources[k] = GoogleCloudResource(resource_name=k, clock=self.clock)
        return processed_stored_resources

    def _write_resources(self, resources):
        """Override the private method to store resources."""
        self._stored_resources_mock_data = resources

    def _delete_resource(self, resource_name):
        """Override the private method to record deleted resources."""
        self.deleted_resources.append(resource_name)


class StaleCleanerTest(unittest.TestCase):
    """Tests for the StaleCleaner class."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project"
        self.resource_type = "test-resource"
        self.bucket_name = "test-bucket"
        self.prefixes = ["test-prefix"]
        self.time_threshold = 3600  # 1 hour

        # Create resources with different ages
        self.fake_clock = FakeClock("2025-05-10T12:00:00")

        # Resource that's fresh (30 minutes old)
        fresh_resource_time = FakeClock("2025-05-10T11:30:00")()
        self.fresh_resource = GoogleCloudResource(
            resource_name="fresh-resource",
            creation_date=fresh_resource_time,
            last_update_date=fresh_resource_time,
            clock=self.fake_clock
        )

        # Resource that's stale (2 hours old)
        stale_resource_time = FakeClock("2025-05-10T10:00:00")()
        self.stale_resource = GoogleCloudResource(
            resource_name="stale-resource",
            creation_date=stale_resource_time,
            last_update_date=stale_resource_time,
            clock=self.fake_clock
        )

        # Active resources in GCP
        self.active_resources_data = {
            "fresh-resource": self.fresh_resource,
            "stale-resource": self.stale_resource,
            "new-resource": GoogleCloudResource(
                resource_name="new-resource",
                clock=self.fake_clock
            )
        }

        # Stored resources
        self.stored_resources_data = {
            "fresh-resource": self.fresh_resource,
            "stale-resource": self.stale_resource,
            "deleted-resource": GoogleCloudResource(
                resource_name="deleted-resource",
                creation_date=stale_resource_time,
                last_update_date=stale_resource_time,
                clock=self.fake_clock
            )
        }

        self.cleaner = MockStaleCleaner(
            project_id=self.project_id,
            resource_type=self.resource_type,
            bucket_name=self.bucket_name,
            prefixes=self.prefixes,
            time_threshold=self.time_threshold,
            active_resources=self.active_resources_data,
            stored_resources=self.stored_resources_data,
            clock=self.fake_clock
        )

    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.cleaner.project_id, self.project_id)
        self.assertEqual(self.cleaner.resource_type, self.resource_type)
        self.assertEqual(self.cleaner.bucket_name, self.bucket_name)
        self.assertEqual(self.cleaner.prefixes, self.prefixes)
        self.assertEqual(self.cleaner.time_threshold, self.time_threshold)

    def test_refresh(self):
        """Test refresh method."""
        # Use SilencePrint to suppress print statements during test
        with SilencePrint():
            self.cleaner.refresh()

        # Check that deleted-resource was removed
        self.assertNotIn("deleted-resource", self.cleaner._stored_resources_mock_data)

        # Check that new-resource was added
        self.assertIn("new-resource", self.cleaner._stored_resources_mock_data)
        # Verify its creation and update times are from the fake_clock
        self.assertEqual(self.cleaner._stored_resources_mock_data["new-resource"].creation_date, self.fake_clock())
        self.assertEqual(self.cleaner._stored_resources_mock_data["new-resource"].last_update_date, self.fake_clock())


        # Check that fresh-resource was updated
        self.assertEqual(
            self.cleaner._stored_resources_mock_data["fresh-resource"].last_update_date,
            self.fake_clock()
        )

        # Check that stale-resource is still present (refresh doesn't delete, just updates times)
        self.assertIn("stale-resource", self.cleaner._stored_resources_mock_data)
        self.assertEqual(
            self.cleaner._stored_resources_mock_data["stale-resource"].last_update_date,
            self.fake_clock()
        )

    def test_refresh_with_time_advance(self):
        """Test refresh method when the clock time has advanced."""
        initial_time_str = "2025-05-10T12:00:00"
        advanced_time_str = "2025-05-10T13:00:00" # 1 hour later

        # Setup cleaner with initial clock
        tmp_clock = FakeClock(initial_time_str)
        resource_to_update = GoogleCloudResource(
            resource_name="resource-to-update",
            clock=tmp_clock
        )
        cleaner = MockStaleCleaner(
            project_id=self.project_id,
            resource_type=self.resource_type,
            bucket_name=self.bucket_name,
            active_resources={"resource-to-update": resource_to_update},
            stored_resources={"resource-to-update": resource_to_update},
            clock=tmp_clock
        )

        # Verify initial last_update_date
        self.assertEqual(
            cleaner._stored_resources_mock_data["resource-to-update"].last_update_date, tmp_clock()
        )

        # Advance the cleaner's clock
        tmp_clock.set(advanced_time_str)

        # Use SilencePrint to suppress print statements during test
        with SilencePrint():
            cleaner.refresh()

        # Check that the resource's last_update_date was updated to the new clock time
        self.assertIn("resource-to-update", cleaner._stored_resources_mock_data)
        self.assertEqual(
            cleaner._stored_resources_mock_data["resource-to-update"].last_update_date,
            cleaner.clock()
        )
        self.assertEqual(
            cleaner._stored_resources_mock_data["resource-to-update"].last_update_date,
            datetime.datetime.fromisoformat(advanced_time_str)
        )
        self.assertNotEqual(
            cleaner._stored_resources_mock_data["resource-to-update"].last_update_date,
            datetime.datetime.fromisoformat(initial_time_str)
        )


    def test_stale_resources(self):
        """Test stale_resources method."""
        stale = self.cleaner.stale_resources()

        self.assertEqual(len(stale), 2)
        self.assertIn("stale-resource", stale)
        self.assertIn("deleted-resource", stale)
        self.assertNotIn("fresh-resource", stale)
        self.assertNotIn("new-resource", stale)

    def test_fresh_resources(self):
        """Test fresh_resources method."""
        fresh = self.cleaner.fresh_resources()

        self.assertEqual(len(fresh), 1)
        self.assertIn("fresh-resource", fresh)
        self.assertNotIn("stale-resource", fresh)
        self.assertNotIn("deleted-resource", fresh)

    def test_delete_stale_dry_run(self):
        """Test delete_stale method with dry_run=True."""
        with SilencePrint():
            self.cleaner.delete_stale(dry_run=True)

        # Check that no resources were actually recorded as deleted by MockStaleCleaner
        self.assertEqual(len(self.cleaner.deleted_resources), 0)

    def test_delete_stale(self):
        """Test delete_stale method with dry_run=False."""
        self.cleaner._active_resources_mock_data = {
            "fresh-resource": self.fresh_resource,
            "stale-resource": self.stale_resource, # Stale and active
            "new-resource": GoogleCloudResource(resource_name="new-resource", clock=self.fake_clock)
        }

        with SilencePrint():
            self.cleaner.delete_stale(dry_run=False)

        # Check that only stale and active resources were "deleted"
        self.assertEqual(len(self.cleaner.deleted_resources), 1)
        self.assertIn("stale-resource", self.cleaner.deleted_resources)
        self.assertNotIn("deleted-resource", self.cleaner.deleted_resources)


class PubSubSubscriptionCleanerTest(unittest.TestCase):
    """Tests for the PubSubSubscriptionCleaner class."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project"
        self.bucket_name = "test-bucket"
        self.prefixes = ["test-prefix"]
        self.time_threshold = 86400  # 1 day
        self.fake_clock = FakeClock("2025-05-28T10:00:00")

        # Mock PubSub client
        self.mock_client_patcher = mock.patch('google.cloud.pubsub_v1.SubscriberClient')
        self.MockSubscriberClientClass = self.mock_client_patcher.start()
        self.mock_subscriber_client = self.MockSubscriberClientClass.return_value

        # Create a test cleaner
        self.cleaner = PubSubSubscriptionCleaner(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            prefixes=self.prefixes,
            time_threshold=self.time_threshold,
            clock=self.fake_clock
        )

        self.cleaner._write_resources = SilencedMock()
        self.cleaner._stored_resources = SilencedMock(return_value={})

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_client_patcher.stop()

    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.cleaner.project_id, self.project_id)
        self.assertEqual(self.cleaner.bucket_name, self.bucket_name)
        self.assertEqual(self.cleaner.prefixes, self.prefixes)
        self.assertEqual(self.cleaner.time_threshold, self.time_threshold)
        self.assertIsInstance(self.cleaner.clock, FakeClock)

    def test_active_resources(self):
        """Test _active_resources method."""
        # Mock subscriptions
        sub1 = mock.Mock()
        sub1.name = "projects/test-project/subscriptions/test-prefix-sub1"
        sub1.topic = "projects/test-project/topics/some-topic"

        sub2 = mock.Mock()
        sub2.name = "projects/test-project/subscriptions/test-prefix-sub2-detached"
        sub2.topic = "_deleted-topic_"

        sub3 = mock.Mock()
        sub3.name = "projects/test-project/subscriptions/other-prefix-sub3"
        sub3.topic = "projects/test-project/topics/another-topic"

        self.mock_subscriber_client.list_subscriptions.return_value = [sub1, sub2, sub3]

        with SilencePrint():
            active = self.cleaner._active_resources()

        self.assertIn("projects/test-project/subscriptions/test-prefix-sub1", active)
        self.assertIn("projects/test-project/subscriptions/test-prefix-sub2-detached", active)
        self.assertNotIn("projects/test-project/subscriptions/other-prefix-sub3", active)
        self.assertEqual(len(active), 2)

    def test_delete_resource(self):
        """Test _delete_resource method."""
        sub_name = "test-sub-to-delete"
        subscription_path = f"projects/{self.project_id}/subscriptions/{sub_name}"
        self.mock_subscriber_client.subscription_path.return_value = subscription_path

        with SilencePrint():
            self.cleaner._delete_resource(sub_name)

        self.mock_subscriber_client.subscription_path.assert_called_once_with(self.project_id, sub_name)
        self.mock_subscriber_client.delete_subscription.assert_called_once_with(
            request={'subscription': subscription_path}
        )


class PubSubTopicCleanerTest(unittest.TestCase):
    """Tests for the PubSubTopicCleaner class."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project"
        self.bucket_name = "test-bucket"
        self.prefixes = ["test-prefix"]
        self.time_threshold = 86400  # 1 day
        self.fake_clock = FakeClock("2025-05-28T10:00:00")

        # Mock PubSub client
        self.mock_client_patcher = mock.patch('google.cloud.pubsub_v1.PublisherClient')
        self.MockPublisherClientClass = self.mock_client_patcher.start()

        # Create a test cleaner
        self.cleaner = PubSubTopicCleaner(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            prefixes=self.prefixes,
            time_threshold=self.time_threshold,
            clock=self.fake_clock
        )

        self.cleaner._write_resources = SilencedMock()
        self.cleaner._stored_resources = SilencedMock(return_value={})

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_client_patcher.stop()

    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.cleaner.project_id, self.project_id)
        self.assertEqual(self.cleaner.resource_type, PUBSUB_TOPIC_RESOURCE)
        self.assertEqual(self.cleaner.bucket_name, self.bucket_name)
        self.assertEqual(self.cleaner.prefixes, self.prefixes)
        self.assertEqual(self.cleaner.time_threshold, self.time_threshold)
        self.assertEqual(self.cleaner.clock, self.fake_clock)
        self.MockPublisherClientClass.assert_called_once()

    def test_active_resources_with_matching_prefix(self):
        """Test __active_resources method with matching prefix."""
        # Configure mock client to return topics
        mock_topic1 = mock.MagicMock()
        mock_topic1.name = "projects/test-project/topics/test-prefix-topic1"

        mock_topic2 = mock.MagicMock()
        mock_topic2.name = "projects/test-project/topics/other-topic"

        self.cleaner.client.list_topics.return_value = [mock_topic1, mock_topic2]

        # Call the private method using name mangling
        resources = self.cleaner._active_resources()

        # Should only return the topic with matching prefix
        self.assertEqual(len(resources), 1)
        self.assertIn("projects/test-project/topics/test-prefix-topic1", resources)
        self.assertNotIn("projects/test-project/topics/other-topic", resources)

    def test_active_resources_without_prefix_filtering(self):
        """Test __active_resources method without prefix filtering."""
        # Configure cleaner with no prefixes
        self.cleaner.prefixes = []

        # Configure mock client to return topics
        mock_topic1 = mock.MagicMock()
        mock_topic1.name = "projects/test-project/topics/test-prefix-topic1"

        mock_topic2 = mock.MagicMock()
        mock_topic2.name = "projects/test-project/topics/other-topic"

        self.cleaner.client.list_topics.return_value = [mock_topic1, mock_topic2]

        # Call the private method using name mangling
        resources = self.cleaner._active_resources()

        # Should return all topics
        self.assertEqual(len(resources), 2)
        self.assertIn("projects/test-project/topics/test-prefix-topic1", resources)
        self.assertIn("projects/test-project/topics/other-topic", resources)

    def test_delete_resource(self):
        """Test __delete_resource method."""
        # Create a resource name
        resource_name = "projects/test-project/topics/test-topic"
        # Set the clock to a specific time
        self.cleaner.clock.set("2025-05-28T10:00:00")

        # Call the private method using name mangling
        with mock.patch('builtins.print') as mock_print:
            self.cleaner._delete_resource(resource_name)

            # Check that delete_topic was called
            self.cleaner.client.delete_topic.assert_called_once_with(request={'topic': resource_name})

            # Check that correct message was printed
            mock_print.assert_called_once_with(f"{self.cleaner.clock()} - Deleting PubSub topic {resource_name}")

if __name__ == '__main__':
    unittest.main()
