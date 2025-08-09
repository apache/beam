# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
import unittest
import time
from unittest import mock
from datetime import datetime, timezone, timedelta
from secret_manager import SecretManager, SECRET_MANAGER_LABEL, SecretManagerLoggerAdapter
from google.cloud import secretmanager
from google.api_core import exceptions

class TestSecretManagerLoggerAdapter(unittest.TestCase):
    """Unit tests for SecretManagerLoggerAdapter class."""

    def test_process_adds_prefix(self):
        """Test that the logger adapter adds the correct prefix."""
        logger = logging.getLogger("test")
        adapter = SecretManagerLoggerAdapter(logger, {})
        
        msg, kwargs = adapter.process("test message", {"key": "value"})
        
        self.assertEqual(msg, "[SecretManager] test message")
        self.assertEqual(kwargs, {"key": "value"})

class TestSecretManager(unittest.TestCase):
    """Unit tests for SecretManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project"
        self.logger = logging.getLogger("test")
        self.logger.setLevel(logging.CRITICAL)  # Suppress logging during tests
        
        # Mock the SecretManagerServiceClient
        with mock.patch('secret_manager.secretmanager.SecretManagerServiceClient'):
            self.manager = SecretManager(
                self.project_id, 
                self.logger, 
                rotation_interval=30, 
                grace_period=7, 
                max_retries=3
            )
        
        self.test_secret_id = "test-secret"
        self.test_data_id = "test-data"
        self.test_payload = b"test-payload"

    def test_init(self):
        """Test SecretManager initialization."""
        with mock.patch('secret_manager.secretmanager.SecretManagerServiceClient'):
            manager = SecretManager("test-project", self.logger, 15, 3, 5)
        
        self.assertEqual(manager.project_id, "test-project")
        self.assertEqual(manager.rotation_interval, 15)
        self.assertEqual(manager.grace_period, 3)
        self.assertEqual(manager.max_retries, 5)
        self.assertIsInstance(manager.logger, SecretManagerLoggerAdapter)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_ids(self, mock_client):
        """Test _get_secret_ids method."""
        # Mock response with secrets having the correct label
        mock_secret1 = mock.Mock()
        mock_secret1.name = "projects/test-project/secrets/secret1"
        mock_secret1.labels = {"created_by": SECRET_MANAGER_LABEL}
        
        mock_secret2 = mock.Mock()
        mock_secret2.name = "projects/test-project/secrets/secret2"
        mock_secret2.labels = {"created_by": "other"}
        
        mock_secret3 = mock.Mock()
        mock_secret3.name = "projects/test-project/secrets/secret3"
        mock_secret3.labels = {"created_by": SECRET_MANAGER_LABEL}
        
        mock_client.return_value.list_secrets.return_value = [mock_secret1, mock_secret2, mock_secret3]
        
        manager = SecretManager(self.project_id, self.logger)
        secret_ids = manager._get_secret_ids()
        
        self.assertEqual(secret_ids, ["secret1", "secret3"])
        mock_client.return_value.list_secrets.assert_called_once()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_ids_exception(self, mock_client):
        """Test _get_secret_ids method with exception."""
        mock_client.return_value.list_secrets.side_effect = Exception("API Error")
        
        manager = SecretManager(self.project_id, self.logger)
        secret_ids = manager._get_secret_ids()
        
        self.assertEqual(secret_ids, [])

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_exists_true(self, mock_client):
        """Test _secret_exists method when secret exists."""
        mock_client.return_value.get_secret.return_value = mock.Mock()
        
        manager = SecretManager(self.project_id, self.logger)
        exists = manager._secret_exists(self.test_secret_id)
        
        self.assertTrue(exists)
        mock_client.return_value.get_secret.assert_called_once()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_exists_false(self, mock_client):
        """Test _secret_exists method when secret doesn't exist."""
        mock_client.return_value.get_secret.side_effect = exceptions.NotFound("Secret not found")
        
        manager = SecretManager(self.project_id, self.logger)
        exists = manager._secret_exists(self.test_secret_id)
        
        self.assertFalse(exists)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_is_managed_true(self, mock_client):
        """Test _secret_is_managed method when secret is managed."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger)
        is_managed = manager._secret_is_managed(self.test_secret_id)
        
        self.assertTrue(is_managed)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_is_managed_false(self, mock_client):
        """Test _secret_is_managed method when secret is not managed."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": "other"}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger)
        is_managed = manager._secret_is_managed(self.test_secret_id)
        
        self.assertFalse(is_managed)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_is_managed_not_exists(self, mock_client):
        """Test _secret_is_managed method when secret doesn't exist."""
        mock_client.return_value.get_secret.side_effect = exceptions.NotFound("Secret not found")
        
        manager = SecretManager(self.project_id, self.logger)
        is_managed = manager._secret_is_managed(self.test_secret_id)
        
        self.assertFalse(is_managed)

    @mock.patch('time.sleep')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_create_secret_success(self, mock_client, mock_sleep):
        """Test create_secret method success."""
        mock_response = mock.Mock()
        mock_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        mock_client.return_value.create_secret.return_value = mock_response
        
        # Mock the sequence of get_secret calls: first raises NotFound, then succeeds
        call_count = [0]  # Use list to make it mutable in nested function
        
        def get_secret_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise exceptions.NotFound("Not found")  # _secret_is_managed returns False
            else:
                # For waiting loop - return a mock secret with proper labels
                mock_secret = mock.Mock()
                mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
                return mock_secret
        
        mock_client.return_value.get_secret.side_effect = get_secret_side_effect
        
        manager = SecretManager(self.project_id, self.logger)
        result = manager.create_secret(self.test_secret_id)
        
        self.assertEqual(result, mock_response.name)
        mock_client.return_value.create_secret.assert_called_once()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_create_secret_already_managed(self, mock_client):
        """Test create_secret method when secret already managed."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_secret.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock the secret_path method to return the expected path
        expected_path = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        mock_client.return_value.secret_path.return_value = expected_path
        
        manager = SecretManager(self.project_id, self.logger)
        result = manager.create_secret(self.test_secret_id)
        
        self.assertEqual(result, expected_path)
        mock_client.return_value.create_secret.assert_not_called()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_success(self, mock_client):
        """Test get_secret method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger)
        result = manager.get_secret(self.test_secret_id)
        
        self.assertEqual(result, mock_secret)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_not_exists(self, mock_client):
        """Test get_secret method when secret doesn't exist."""
        mock_client.return_value.get_secret.side_effect = exceptions.NotFound("Not found")
        
        manager = SecretManager(self.project_id, self.logger)
        
        with self.assertRaises(ValueError):
            manager.get_secret(self.test_secret_id)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_not_managed(self, mock_client):
        """Test get_secret method when secret is not managed."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": "other"}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger)
        
        with self.assertRaises(ValueError):
            manager.get_secret(self.test_secret_id)

    @mock.patch.object(SecretManager, '_secret_exists')
    @mock.patch.object(SecretManager, '_secret_is_managed')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_delete_secret_success(self, mock_client, mock_is_managed, mock_exists):
        """Test delete_secret method success."""
        # Mock that secret is managed
        mock_is_managed.return_value = True
        
        # Mock that secret doesn't exist after deletion
        mock_exists.return_value = False
        
        manager = SecretManager(self.project_id, self.logger)
        manager.delete_secret(self.test_secret_id)
        
        mock_client.return_value.delete_secret.assert_called_once()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_delete_secret_not_managed(self, mock_client):
        """Test delete_secret method when secret is not managed."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": "other"}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger)
        
        # The method should return early without raising exception when secret is not managed
        manager.delete_secret(self.test_secret_id)
        
        # Verify that delete_secret was not called since the secret is not managed
        mock_client.return_value.delete_secret.assert_not_called()

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_is_different_user_access_same(self, mock_client):
        """Test is_different_user_access method when access is the same."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_policy = mock.Mock()
        mock_binding = mock.Mock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:test@example.com", "user:test2@example.com"]
        mock_policy.bindings = [mock_binding]
        mock_client.return_value.get_iam_policy.return_value = mock_policy
        
        manager = SecretManager(self.project_id, self.logger)
        is_different = manager.is_different_user_access(
            self.test_secret_id, 
            ["test@example.com", "test2@example.com"]
        )
        
        self.assertFalse(is_different)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_is_different_user_access_different(self, mock_client):
        """Test is_different_user_access method when access is different."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_policy = mock.Mock()
        mock_binding = mock.Mock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:different@example.com"]
        mock_policy.bindings = [mock_binding]
        mock_client.return_value.get_iam_policy.return_value = mock_policy
        
        manager = SecretManager(self.project_id, self.logger)
        is_different = manager.is_different_user_access(
            self.test_secret_id, 
            ["test@example.com"]
        )
        
        self.assertTrue(is_different)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_update_secret_access_success(self, mock_client):
        """Test update_secret_access method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_policy = mock.Mock()
        mock_binding = mock.Mock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:old@example.com"]
        mock_policy.bindings = [mock_binding]
        mock_client.return_value.get_iam_policy.return_value = mock_policy
        
        manager = SecretManager(self.project_id, self.logger)
        manager.update_secret_access(self.test_secret_id, ["new@example.com"])
        
        mock_client.return_value.set_iam_policy.assert_called_once()
        self.assertEqual(mock_binding.members, ["user:new@example.com"])

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_secret_versions_success(self, mock_client):
        """Test _get_secret_versions method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_versions = [mock.Mock(), mock.Mock()]
        mock_client.return_value.list_secret_versions.return_value = mock_versions
        
        manager = SecretManager(self.project_id, self.logger)
        versions = manager._get_secret_versions(self.test_secret_id)
        
        self.assertEqual(versions, mock_versions)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_version_exists_true(self, mock_client):
        """Test _secret_version_exists method when version exists."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        exists = manager._secret_version_exists(self.test_secret_id, "1")
        
        self.assertTrue(exists)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_version_exists_false(self, mock_client):
        """Test _secret_version_exists method when version doesn't exist."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        exists = manager._secret_version_exists(self.test_secret_id, "1")
        
        self.assertFalse(exists)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_version_is_enabled_true(self, mock_client):
        """Test _secret_version_is_enabled method when version is enabled."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.ENABLED
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        is_enabled = manager._secret_version_is_enabled(self.test_secret_id, "1")
        
        self.assertTrue(is_enabled)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_secret_version_is_enabled_false(self, mock_client):
        """Test _secret_version_is_enabled method when version is not enabled."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.DISABLED
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        is_enabled = manager._secret_version_is_enabled(self.test_secret_id, "1")
        
        self.assertFalse(is_enabled)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_latest_secret_version_id_success(self, mock_client):
        """Test _get_latest_secret_version_id method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version1 = mock.Mock()
        mock_version1.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version1.state = secretmanager.SecretVersion.State.ENABLED
        mock_version1.create_time.timestamp.return_value = 1000
        
        mock_version2 = mock.Mock()
        mock_version2.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        mock_version2.state = secretmanager.SecretVersion.State.ENABLED
        mock_version2.create_time.timestamp.return_value = 2000
        
        # Return versions in reverse order (latest first) as Google API does
        mock_client.return_value.list_secret_versions.return_value = [mock_version2, mock_version1]
        
        manager = SecretManager(self.project_id, self.logger)
        latest_id = manager._get_latest_secret_version_id(self.test_secret_id)
        
        self.assertEqual(latest_id, "2")

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_latest_secret_version_id_no_enabled(self, mock_client):
        """Test _get_latest_secret_version_id method when no enabled versions."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.DISABLED
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        
        with self.assertRaises(ValueError):
            manager._get_latest_secret_version_id(self.test_secret_id)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_is_key_rotation_due_true(self, mock_client):
        """Test _is_key_rotation_due method when rotation is due."""
        past_date = datetime.now(timezone.utc) - timedelta(days=40)
        mock_secret = mock.Mock()
        mock_secret.labels = {
            "created_by": SECRET_MANAGER_LABEL,
            "last_version_created_at": past_date.strftime("%Y%m%d_%H%M%S")
        }
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger, rotation_interval=30)
        is_due = manager._is_key_rotation_due(self.test_secret_id)
        
        self.assertTrue(is_due)

    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_is_key_rotation_due_false(self, mock_client):
        """Test _is_key_rotation_due method when rotation is not due."""
        recent_date = datetime.now(timezone.utc) - timedelta(days=10)
        mock_secret = mock.Mock()
        mock_secret.labels = {
            "created_by": SECRET_MANAGER_LABEL,
            "last_version_created_at": recent_date.strftime("%Y%m%d_%H%M%S")
        }
        mock_client.return_value.get_secret.return_value = mock_secret
        
        manager = SecretManager(self.project_id, self.logger, rotation_interval=30)
        is_due = manager._is_key_rotation_due(self.test_secret_id)
        
        self.assertFalse(is_due)

    @mock.patch('time.sleep')
    @mock.patch('google_crc32c.Checksum')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_add_secret_version_success(self, mock_client, mock_checksum, mock_sleep):
        """Test add_secret_version method success."""
        # Mock checksum
        mock_checksum_instance = mock.Mock()
        mock_checksum_instance.hexdigest.return_value = "abcd1234"
        mock_checksum.return_value = mock_checksum_instance
        
        # Mock create_secret behavior - secret already exists
        mock_secret = mock.Mock()
        mock_secret.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock add_secret_version
        mock_response = mock.Mock()
        mock_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_client.return_value.add_secret_version.return_value = mock_response
        
        # Mock list_secret_versions for waiting and disabling
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.ENABLED
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        manager = SecretManager(self.project_id, self.logger)
        result = manager.add_secret_version(self.test_secret_id, self.test_data_id, self.test_payload)
        
        self.assertEqual(result, mock_response.name)
        mock_client.return_value.add_secret_version.assert_called_once()
        mock_client.return_value.update_secret.assert_called_once()

    @mock.patch('google_crc32c.Checksum')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_get_latest_secret_version_success(self, mock_client, mock_checksum):
        """Test get_latest_secret_version method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock latest version
        mock_version = mock.Mock()
        mock_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.ENABLED
        mock_version.create_time.timestamp.return_value = 1000
        mock_client.return_value.list_secret_versions.return_value = [mock_version]
        
        # Mock access_secret_version
        mock_response = mock.Mock()
        mock_response.payload.data = b"test-data:test-payload"
        mock_response.payload.data_crc32c = int("abcd1234", 16)
        mock_client.return_value.access_secret_version.return_value = mock_response
        
        # Mock checksum
        mock_checksum_instance = mock.Mock()
        mock_checksum_instance.hexdigest.return_value = "abcd1234"
        mock_checksum.return_value = mock_checksum_instance
        
        manager = SecretManager(self.project_id, self.logger)
        data_id, payload = manager.get_latest_secret_version(self.test_secret_id)
        
        self.assertEqual(data_id, "test-data")
        self.assertEqual(payload, b"test-payload")

    @mock.patch('time.sleep')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_enable_secret_version_success(self, mock_client, mock_sleep):
        """Test enable_secret_version method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock version exists and is not enabled initially
        mock_disabled_version = mock.Mock()
        mock_disabled_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_disabled_version.state = secretmanager.SecretVersion.State.DISABLED
        
        # Mock version becomes enabled after the operation
        mock_enabled_version = mock.Mock()
        mock_enabled_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_enabled_version.state = secretmanager.SecretVersion.State.ENABLED
        
        # First call returns disabled version, second call returns enabled version
        mock_client.return_value.list_secret_versions.side_effect = [
            [mock_disabled_version],  # Initial check
            [mock_enabled_version]    # After enabling
        ]
        
        # Mock enable response
        mock_response = mock.Mock()
        mock_response.name = mock_disabled_version.name
        mock_response.state = secretmanager.SecretVersion.State.ENABLED
        mock_client.return_value.enable_secret_version.return_value = mock_response
        
        manager = SecretManager(self.project_id, self.logger)
        manager.enable_secret_version(self.test_secret_id, "1")
        
        mock_client.return_value.enable_secret_version.assert_called_once()

    @mock.patch('time.sleep')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_disable_secret_version_success(self, mock_client, mock_sleep):
        """Test disable_secret_version method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock version exists and is enabled initially
        mock_enabled_version = mock.Mock()
        mock_enabled_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_enabled_version.state = secretmanager.SecretVersion.State.ENABLED
        
        # Mock version becomes disabled after the operation
        mock_disabled_version = mock.Mock()
        mock_disabled_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_disabled_version.state = secretmanager.SecretVersion.State.DISABLED
        
        # First call returns enabled version, second call returns disabled version
        mock_client.return_value.list_secret_versions.side_effect = [
            [mock_enabled_version],   # Initial check
            [mock_disabled_version]   # After disabling
        ]
        
        # Mock disable response
        mock_response = mock.Mock()
        mock_response.name = mock_enabled_version.name
        mock_response.state = secretmanager.SecretVersion.State.DISABLED
        mock_client.return_value.disable_secret_version.return_value = mock_response
        
        manager = SecretManager(self.project_id, self.logger)
        manager.disable_secret_version(self.test_secret_id, "1")
        
        mock_client.return_value.disable_secret_version.assert_called_once()

    @mock.patch('time.sleep')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_destroy_secret_version_success(self, mock_client, mock_sleep):
        """Test destroy_secret_version method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock version exists and is enabled initially
        mock_enabled_version = mock.Mock()
        mock_enabled_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_enabled_version.state = secretmanager.SecretVersion.State.ENABLED
        
        # Mock version becomes destroyed after the operation
        mock_destroyed_version = mock.Mock()
        mock_destroyed_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_destroyed_version.state = secretmanager.SecretVersion.State.DESTROYED
        
        # Multiple calls to list_secret_versions for different operations
        mock_client.return_value.list_secret_versions.side_effect = [
            [mock_enabled_version],   # Initial check in _secret_version_is_enabled
            [mock_enabled_version],   # Check in enable_secret_version before enabling
            [mock_enabled_version],   # After enabling check
            [mock_destroyed_version]  # After destroying check
        ]
        
        # Mock access_secret_version for getting data_id
        mock_access_response = mock.Mock()
        mock_access_response.payload.data = b"test-data:test-payload"
        mock_client.return_value.access_secret_version.return_value = mock_access_response
        
        # Mock destroy response
        mock_destroy_response = mock.Mock()
        mock_destroy_response.name = mock_enabled_version.name
        mock_destroy_response.state = secretmanager.SecretVersion.State.DESTROYED
        mock_client.return_value.destroy_secret_version.return_value = mock_destroy_response
        
        # Mock enable response (needed since version is already enabled)
        mock_enable_response = mock.Mock()
        mock_enable_response.name = mock_enabled_version.name
        mock_enable_response.state = secretmanager.SecretVersion.State.ENABLED
        mock_client.return_value.enable_secret_version.return_value = mock_enable_response
        
        manager = SecretManager(self.project_id, self.logger)
        data_id = manager.destroy_secret_version(self.test_secret_id, "1")
        
        self.assertEqual(data_id, "test-data")
        mock_client.return_value.destroy_secret_version.assert_called_once()

    @mock.patch.object(SecretManager, 'destroy_secret_version')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_purge_disabled_secret_versions_success(self, mock_client, mock_destroy):
        """Test purge_disabled_secret_versions method success."""
        mock_secret = mock.Mock()
        mock_secret.labels = {"created_by": SECRET_MANAGER_LABEL}
        mock_client.return_value.get_secret.return_value = mock_secret
        
        # Mock old disabled version
        old_time = datetime.now(timezone.utc) - timedelta(days=10)
        mock_old_version = mock.Mock()
        mock_old_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_old_version.state = secretmanager.SecretVersion.State.DISABLED
        mock_old_version.create_time.timestamp.return_value = old_time.timestamp()
        
        # Mock recent disabled version (within grace period)
        recent_time = datetime.now(timezone.utc) - timedelta(days=2)
        mock_recent_version = mock.Mock()
        mock_recent_version.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        mock_recent_version.state = secretmanager.SecretVersion.State.DISABLED
        mock_recent_version.create_time.timestamp.return_value = recent_time.timestamp()
        
        mock_client.return_value.list_secret_versions.return_value = [mock_old_version, mock_recent_version]
        
        # Mock destroy method to return data_id
        mock_destroy.return_value = "old-data"
        
        manager = SecretManager(self.project_id, self.logger, grace_period=7)
        data_ids = manager.purge_disabled_secret_versions(self.test_secret_id)
        
        self.assertEqual(data_ids, ["old-data"])
        mock_destroy.assert_called_once_with(self.test_secret_id, "1")

    @mock.patch.object(SecretManager, 'purge_disabled_secret_versions')
    @mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
    def test_cron_success(self, mock_client, mock_purge):
        """Test cron method success."""
        # Mock _get_secret_ids
        mock_secret1 = mock.Mock()
        mock_secret1.name = f"projects/{self.project_id}/secrets/secret1"
        mock_secret1.labels = {"created_by": SECRET_MANAGER_LABEL}
        
        mock_secret2 = mock.Mock()
        mock_secret2.name = f"projects/{self.project_id}/secrets/secret2"
        mock_secret2.labels = {"created_by": SECRET_MANAGER_LABEL}
        
        mock_client.return_value.list_secrets.return_value = [mock_secret1, mock_secret2]
        
        # Mock purge_disabled_secret_versions behavior
        def mock_purge_side_effect(secret_id):
            if secret_id == "secret1":
                return ["purged-data"]
            else:
                return []  # secret2 has no versions to purge
        
        mock_purge.side_effect = mock_purge_side_effect
        
        manager = SecretManager(self.project_id, self.logger, grace_period=7)
        result = manager.cron()
        
        self.assertIn("secret1", result)
        self.assertEqual(result["secret1"], ["purged-data"])
        # secret2 should not be in result since it had no purged versions
        self.assertNotIn("secret2", result)




# Integration tests (skipped unless environment variables are set)
@unittest.skipUnless(
    'GOOGLE_CLOUD_PROJECT' in os.environ,
    "Skipping tests because environment variables are not set for Google Cloud project."
)
class TestSecretManagerIntegration(unittest.TestCase):
    """Integration tests for SecretManager with real Google Cloud Secret Manager client."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        # Create a logger for integration tests
        self.logger = logging.getLogger(__name__)
        self.manager = SecretManager(self.project_id, self.logger, rotation_interval=0, grace_period=0, max_retries=3)
        self.test_secret_id = f"integration-test-secret-{int(time.time())}"
        self.test_data_id = f"integration-test-data-{int(time.time())}"
        self.test_payload = b"integration-test-payload"
        self.test_allowed_users = ["pabloem@google.com"]

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up any secrets created during tests
        try:
            if self.test_secret_id in self.manager._get_secret_ids():
                self.manager.delete_secret(self.test_secret_id)
        except Exception as e:
            self.logger.warning(f"Failed to clean up test secret: {e}")

    def test_full_secret_lifecycle(self):
        """Test creating, adding versions, rotating, and deleting a secret."""
        # Test creating a secret
        self.manager.create_secret(self.test_secret_id)
        self.assertTrue(self.manager._secret_exists(self.test_secret_id))

        # Test allowing users to access the secret
        self.manager.update_secret_access(self.test_secret_id, self.test_allowed_users)
        self.assertFalse(self.manager.is_different_user_access(self.test_secret_id, self.test_allowed_users))

        # Add first version (creates the secret)
        version1 = self.manager.add_secret_version(self.test_secret_id, self.test_data_id, self.test_payload)
        self.assertIsNotNone(version1)
        
        # Verify secret exists
        secret = self.manager.get_secret(self.test_secret_id)
        self.assertEqual(secret.labels["created_by"], SECRET_MANAGER_LABEL)
        
        # Add second version
        version2 = self.manager.add_secret_version(self.test_secret_id, f"{self.test_data_id}-v2", b"second-payload")
        self.assertIsNotNone(version2)
        
        # List versions
        versions = self.manager._get_secret_versions(self.test_secret_id)
        self.assertGreaterEqual(len(versions), 2)
        
        # Get latest version
        retrieved_payload = self.manager.get_latest_secret_version(self.test_secret_id)
        self.assertEqual(retrieved_payload, (f"{self.test_data_id}-v2", b"second-payload"))
        
        # Rotate secret
        latest_version = self.manager.add_secret_version(self.test_secret_id, f"{self.test_data_id}-rotated", b"rotated-payload")

        # Verify latest version has rotated payload
        latest_payload = self.manager.get_latest_secret_version(self.test_secret_id)
        self.assertEqual(latest_payload, (f"{self.test_data_id}-rotated", b"rotated-payload"))

        # Verify all the other versions are disabled
        versions = self.manager._get_secret_versions(self.test_secret_id)
        for version in versions:
            if version.name != latest_version:
                self.assertEqual(version.state, secretmanager.SecretVersion.State.DISABLED)

        # Try cron method (should be no-op since grace period is 0)
        cron_result = self.manager.cron()
        self.assertIn(self.test_secret_id, cron_result)
        self.assertEqual(len(cron_result[self.test_secret_id]), len(versions) - 1)  # All but the latest should be purged
        self.assertNotIn(f"{self.test_data_id}-rotated", cron_result[self.test_secret_id]) # Latest id should not be purged

        # Try to get the latest version after cron
        latest_payload_after_cron = self.manager.get_latest_secret_version(self.test_secret_id)
        self.assertEqual(latest_payload_after_cron, (f"{self.test_data_id}-rotated", b"rotated-payload"))

        # Delete secret
        self.manager.delete_secret(self.test_secret_id)
        
        # Verify secret is removed from secret_ids
        self.assertNotIn(self.test_secret_id, self.manager._get_secret_ids())

if __name__ == '__main__':
    # Configure logging to reduce noise during testing
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    # Run the tests
    unittest.main()
