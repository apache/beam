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
import json
import unittest
import io
import sys
from unittest import mock
from stale_cleaner import (
    GoogleCloudResource,
    StaleCleaner,
    PubSubTopicCleaner,
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
        test_time = datetime.datetime(2025, 5, 10, 12, 0, 0)

        with mock.patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = test_time
            resource = GoogleCloudResource(resource_name="test-resource")

            self.assertEqual(resource.resource_name, "test-resource")
            self.assertEqual(resource.creation_date, test_time)
            self.assertEqual(resource.last_update_date, test_time)

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        creation_date = datetime.datetime(2025, 1, 1, 12, 0, 0)
        update_date = datetime.datetime(2025, 5, 1, 12, 0, 0)

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date,
            last_update_date=update_date
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
        update_date = datetime.datetime(2025, 5, 1, 12, 0, 0)
        new_update_date = datetime.datetime(2025, 5, 10, 12, 0, 0)

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date,
            last_update_date=update_date
        )

        # Test with provided clock
        resource.update(clock=new_update_date)
        self.assertEqual(resource.last_update_date, new_update_date)

        # Test with default clock
        with mock.patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 6, 1, 12, 0, 0)
            resource.update()
            self.assertEqual(resource.last_update_date, datetime.datetime(2025, 6, 1, 12, 0, 0))

    def test_time_alive(self):
        """Test time_alive method."""
        creation_date = datetime.datetime(2025, 1, 1, 12, 0, 0)
        current_time = datetime.datetime(2025, 1, 2, 12, 0, 0)  # 1 day later

        resource = GoogleCloudResource(
            resource_name="test-resource",
            creation_date=creation_date
        )

        # Test with provided clock
        self.assertEqual(resource.time_alive(clock=current_time), 86400)  # 1 day in seconds

class MockStaleCleaner(StaleCleaner):
    """A mock implementation of StaleCleaner for testing."""

    def __init__(self, project_id, resource_type, bucket_name, prefixes=None,
                 time_threshold=DEFAULT_TIME_THRESHOLD, active_resources=None,
                 stored_resources=None):
        super().__init__(project_id, resource_type, bucket_name, prefixes, time_threshold)
        self._active_resources_mock_data = active_resources or {}
        self._stored_resources_mock_data = stored_resources or {}
        self.deleted_resources = []

    def _active_resources(self, clock=None):
        """Override the private method to return mock data."""
        return self._active_resources_mock_data

    def _stored_resources(self):
        """Override the private method to return mock data."""
        return self._stored_resources_mock_data

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
        current_time = datetime.datetime(2025, 5, 10, 12, 0, 0)

        # Resource that's fresh (30 minutes old)
        fresh_resource_time = current_time - datetime.timedelta(minutes=30)
        self.fresh_resource = GoogleCloudResource(
            resource_name="fresh-resource",
            creation_date=fresh_resource_time,
            last_update_date=fresh_resource_time
        )

        # Resource that's stale (2 hours old)
        stale_resource_time = current_time - datetime.timedelta(hours=2)
        self.stale_resource = GoogleCloudResource(
            resource_name="stale-resource",
            creation_date=stale_resource_time,
            last_update_date=stale_resource_time
        )

        # Active resources in GCP
        self.active_resources_data = {
            "fresh-resource": self.fresh_resource,
            "stale-resource": self.stale_resource,
            "new-resource": GoogleCloudResource(
                resource_name="new-resource",
                creation_date=current_time,
                last_update_date=current_time
            )
        }

        # Stored resources
        self.stored_resources_data = {
            "fresh-resource": self.fresh_resource,
            "stale-resource": self.stale_resource,
            "deleted-resource": GoogleCloudResource(
                resource_name="deleted-resource",
                creation_date=stale_resource_time,
                last_update_date=stale_resource_time
            )
        }

        self.cleaner = MockStaleCleaner(
            project_id=self.project_id,
            resource_type=self.resource_type,
            bucket_name=self.bucket_name,
            prefixes=self.prefixes,
            time_threshold=self.time_threshold,
            active_resources=self.active_resources_data,
            stored_resources=self.stored_resources_data
        )

        self.current_time = current_time

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
            self.cleaner.refresh(clock=self.current_time)

        # Check that deleted-resource was removed
        self.assertNotIn("deleted-resource", self.cleaner._stored_resources_mock_data)

        # Check that new-resource was added
        self.assertIn("new-resource", self.cleaner._stored_resources_mock_data)

        # Check that fresh-resource was updated
        self.assertEqual(
            self.cleaner._stored_resources_mock_data["fresh-resource"].last_update_date,
            self.current_time
        )

        # Check that stale-resource is still present
        self.assertIn("stale-resource", self.cleaner._stored_resources_mock_data)

    def test_stale_resources(self):
        """Test stale_resources method."""
        stale = self.cleaner.stale_resources(clock=self.current_time)

        self.assertEqual(len(stale), 2)
        self.assertIn("stale-resource", stale)
        self.assertIn("deleted-resource", stale)
        self.assertNotIn("fresh-resource", stale)

    def test_fresh_resources(self):
        """Test fresh_resources method."""
        fresh = self.cleaner.fresh_resources(clock=self.current_time)

        self.assertEqual(len(fresh), 1)
        self.assertIn("fresh-resource", fresh)
        self.assertNotIn("stale-resource", fresh)
        self.assertNotIn("deleted-resource", fresh)

    def test_delete_stale_dry_run(self):
        """Test delete_stale method with dry_run=True."""
        with SilencePrint():
            self.cleaner.delete_stale(clock=self.current_time, dry_run=True)

        # Check that no resources were actually deleted
        self.assertEqual(len(self.cleaner.deleted_resources), 0)

    def test_delete_stale(self):
        """Test delete_stale method with dry_run=False."""
        with SilencePrint():
            self.cleaner.delete_stale(clock=self.current_time, dry_run=False)

        # Check that stale resources were deleted
        self.assertEqual(len(self.cleaner.deleted_resources), 2)
        self.assertIn("stale-resource", self.cleaner.deleted_resources)
        self.assertIn("deleted-resource", self.cleaner.deleted_resources)


class PubSubTopicCleanerTest(unittest.TestCase):
    """Tests for the PubSubTopicCleaner class."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project"
        self.bucket_name = "test-bucket"
        self.prefixes = ["test-prefix"]
        self.time_threshold = 86400  # 1 day

        # Mock PubSub client
        self.mock_client_patcher = mock.patch('google.cloud.pubsub_v1.PublisherClient')
        self.mock_client = self.mock_client_patcher.start()

        # Create a test cleaner
        self.cleaner = PubSubTopicCleaner(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            prefixes=self.prefixes,
            time_threshold=self.time_threshold
        )

        # Override __write_resources and __stored_resources
        self.cleaner._StaleCleaner__write_resources = mock.MagicMock()
        self.cleaner._StaleCleaner__stored_resources = mock.MagicMock(return_value={})

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
        self.mock_client.assert_called_once()

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

        # Call the private method using name mangling
        with mock.patch('builtins.print') as mock_print:
            self.cleaner._delete_resource(resource_name)

            # Check that delete_topic was called
            self.cleaner.client.delete_topic.assert_called_once_with(name=resource_name)

            # Check that correct message was printed
            mock_print.assert_called_once_with("Deleting PubSub topic test-topic")

if __name__ == '__main__':
    unittest.main()
