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
from typing import Optional
from unittest import mock
from datetime import datetime, timezone, timedelta
from secret_manager import SecretManager
from google.cloud import secretmanager


class TestSecretManagerUnit(unittest.TestCase):
    """Unit tests for SecretManager with mocked Google Cloud Secret Manager client."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project-123"
        self.test_secret_id = "test-secret"
        self.test_payload = b"test-secret-payload"
        self.test_rotation_interval = 30
        self.test_max_versions = 5
        
        # Patch the Secret Manager client
        self.client_patcher = mock.patch('secret_manager.secretmanager.SecretManagerServiceClient')
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = self.mock_client_class.return_value
        
        # Mock the _get_secrets_ids method to avoid calling the real API during initialization
        with mock.patch.object(SecretManager, '_get_secrets_ids', return_value=[]):
            # Create a mock logger
            self.mock_logger = mock.MagicMock()
            
            # Create the secret manager
            self.manager = SecretManager(
                self.project_id, 
                self.mock_logger, 
                self.test_rotation_interval, 
                self.test_max_versions
            )

    def tearDown(self):
        """Tear down test fixtures."""
        self.client_patcher.stop()

    def _create_mock_secret(self, secret_id: str, labels: Optional[dict] = None) -> secretmanager.Secret:
        """Helper method to create a mock secret."""
        mock_secret = mock.MagicMock(spec=secretmanager.Secret)
        mock_secret.name = f"projects/{self.project_id}/secrets/{secret_id}"
        mock_secret.labels = labels or {
            "created_by": "secretmanager-service",
            "created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "rotation_interval_days": str(self.test_rotation_interval),
            "last_version_created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        }
        return mock_secret

    def _create_mock_secret_version(self, secret_id: str, version_id: str = "1", 
                                   state: secretmanager.SecretVersion.State = secretmanager.SecretVersion.State.ENABLED) -> secretmanager.SecretVersion:
        """Helper method to create a mock secret version."""
        mock_version = mock.MagicMock(spec=secretmanager.SecretVersion)
        mock_version.name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        mock_version.state = state
        mock_version.create_time = mock.MagicMock()
        mock_version.destroy_time = None
        return mock_version

    def _create_mock_access_response(self, payload: bytes) -> mock.MagicMock:
        """Helper method to create a mock access secret version response."""
        import google_crc32c
        crc32c = google_crc32c.Checksum()
        crc32c.update(payload)
        
        mock_payload = mock.MagicMock()
        mock_payload.data = payload
        mock_payload.data_crc32c = int(crc32c.hexdigest(), 16)
        
        mock_response = mock.MagicMock()
        mock_response.payload = mock_payload
        return mock_response

    def test_init(self):
        """Test SecretManager initialization."""
        self.assertEqual(self.manager.project_id, self.project_id)
        self.assertEqual(self.manager.rotation_interval, self.test_rotation_interval)
        self.assertEqual(self.manager.max_versions_to_keep, self.test_max_versions)
        self.mock_client_class.assert_called_once()

    def test_get_secrets_ids(self):
        """Test _get_secrets_ids method."""
        # Create mock secrets
        mock_secret1 = self._create_mock_secret("secret1")
        mock_secret2 = self._create_mock_secret("secret2")
        mock_secret3 = mock.MagicMock(spec=secretmanager.Secret)
        mock_secret3.name = f"projects/{self.project_id}/secrets/secret3"
        mock_secret3.labels = {"created_by": "OtherService"}  # Different creator
        
        self.mock_client.list_secrets.return_value = [mock_secret1, mock_secret2, mock_secret3]
        
        # Call the method directly
        result = self.manager._get_secrets_ids()
        
        # Should only return secrets created by SecretManager
        self.assertEqual(result, ["secret1", "secret2"])
        self.mock_client.list_secrets.assert_called_once_with(
            request={"parent": f"projects/{self.project_id}"}
        )

    def test_create_secret_new(self):
        """Test creating a new secret."""
        mock_secret = self._create_mock_secret(self.test_secret_id)
        self.mock_client.create_secret.return_value = mock_secret
        self.mock_client.secret_path.return_value = mock_secret.name
        
        with mock.patch.object(self.manager, '_secret_exists', side_effect=[False, True]):
            result = self.manager.create_secret(self.test_secret_id)
        
        self.assertEqual(result, mock_secret.name)
        self.assertIn(self.test_secret_id, self.manager.secrets_ids)
        self.mock_client.create_secret.assert_called_once()
        
        # Verify the request structure
        call_args = self.mock_client.create_secret.call_args
        request = call_args[1]['request']
        self.assertEqual(request['secret_id'], self.test_secret_id)
        self.assertEqual(request['parent'], f"projects/{self.project_id}")
        self.assertEqual(request['secret']['labels']['created_by'], "secretmanager-service")

    def test_create_secret_existing(self):
        """Test creating a secret that already exists."""
        self.manager.secrets_ids = [self.test_secret_id]
        expected_path = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.secret_path.return_value = expected_path
        
        result = self.manager.create_secret(self.test_secret_id)
        
        self.assertEqual(result, expected_path)
        self.mock_client.create_secret.assert_not_called()

    def test_get_secret_success(self):
        """Test successfully retrieving a secret."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_secret = self._create_mock_secret(self.test_secret_id)
        self.mock_client.get_secret.return_value = mock_secret
        self.mock_client.secret_path.return_value = mock_secret.name
        
        result = self.manager.get_secret(self.test_secret_id)
        
        self.assertEqual(result, mock_secret)
        self.mock_client.get_secret.assert_called_once()

    def test_get_secret_not_exists(self):
        """Test retrieving a secret that doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            with self.assertRaises(ValueError) as context:
                self.manager.get_secret(self.test_secret_id)
        
            self.assertIn("does not exist", str(context.exception))
            self.mock_client.get_secret.assert_not_called()

    def test_delete_secret_success(self):
        """Test successfully deleting a secret."""
        self.manager.secrets_ids = [self.test_secret_id]
        secret_path = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.secret_path.return_value = secret_path
        
        with mock.patch.object(self.manager, '_secret_exists', side_effect=[True, False]):
            self.manager.delete_secret(self.test_secret_id)
        
        self.assertNotIn(self.test_secret_id, self.manager.secrets_ids)
        self.mock_client.delete_secret.assert_called_once_with(request={"name": secret_path})

    def test_delete_secret_not_exists(self):
        """Test deleting a secret that doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            self.manager.delete_secret(self.test_secret_id)
        
        self.mock_client.delete_secret.assert_not_called()

    def test_add_secret_version_new_secret(self):
        """Test adding a version to a new secret."""
        mock_secret = self._create_mock_secret(self.test_secret_id)
        mock_version = self._create_mock_secret_version(self.test_secret_id, "1")
        
        self.mock_client.create_secret.return_value = mock_secret
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.add_secret_version.return_value = mock_version
        self.mock_client.list_secret_versions.return_value = [mock_version]
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager.add_secret_version(self.test_secret_id, self.test_payload)
        
        self.assertEqual(result, mock_version.name)
        self.mock_client.add_secret_version.assert_called_once()
        self.mock_client.update_secret.assert_called_once()

    def test_add_secret_version_string_payload(self):
        """Test adding a version with string payload."""
        mock_secret = self._create_mock_secret(self.test_secret_id)
        mock_version = self._create_mock_secret_version(self.test_secret_id, "1")
        
        self.mock_client.create_secret.return_value = mock_secret
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.add_secret_version.return_value = mock_version
        self.mock_client.list_secret_versions.return_value = [mock_version]
        self.mock_client.get_secret.return_value = mock_secret
        
        string_payload = "test-secret-payload"
        result = self.manager.add_secret_version(self.test_secret_id, string_payload)
        
        self.assertEqual(result, mock_version.name)
        
        # Verify that string was converted to bytes
        call_args = self.mock_client.add_secret_version.call_args
        request = call_args[1]['request']
        self.assertEqual(request['payload']['data'], string_payload.encode('utf-8'))

    def test_add_secret_version_invalid_payload(self):
        """Test adding a version with invalid payload type."""
        with mock.patch.object(self.manager, '_secret_exists', side_effect=[False, True]):
            with mock.patch.object(self.manager, 'create_secret', return_value="mocked_path"):
                with self.assertRaises(TypeError) as context:
                    self.manager.add_secret_version(self.test_secret_id, 123)  # type: ignore
        
                self.assertIn("Payload must be a bytes object", str(context.exception))

    def test_add_secret_version_exceeds_max_versions(self):
        """Test adding a version when max versions is exceeded."""
        self.manager.max_versions_to_keep = 2
        self.manager.secrets_ids = [self.test_secret_id]  # Add to existing secrets
        mock_secret = self._create_mock_secret(self.test_secret_id)
        
        # Create 2 enabled versions initially  
        mock_versions_before = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        new_version = self._create_mock_secret_version(self.test_secret_id, "3")
        
        # After adding new version, we have 3 versions (exceeds max of 2)
        mock_versions_after = mock_versions_before + [new_version]
        
        # Mock the disable response for version "1" (the first/oldest that will be purged)
        disable_response = mock.MagicMock()
        disable_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        disable_response.state = secretmanager.SecretVersion.State.DISABLED
        
        self.mock_client.create_secret.return_value = mock_secret
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.add_secret_version.return_value = new_version
        self.mock_client.list_secret_versions.return_value = mock_versions_after
        self.mock_client.get_secret.return_value = mock_secret
        self.mock_client.disable_secret_version.return_value = disable_response
        
        # Mock the helper methods
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, '_secret_version_exists', return_value=True):
                with mock.patch.object(self.manager, '_secret_version_is_enabled', side_effect=[True, False]):
                    result = self.manager.add_secret_version(self.test_secret_id, self.test_payload)
        
        self.assertEqual(result, new_version.name)
        self.mock_client.disable_secret_version.assert_called_once()

    def test_list_secret_versions_success(self):
        """Test listing secret versions successfully."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._get_secret_versions(self.test_secret_id)
        
        self.assertEqual(result, mock_versions)
        self.mock_client.list_secret_versions.assert_called_once()

    def test_list_secret_versions_not_exists(self):
        """Test listing versions for a secret that doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            result = self.manager._get_secret_versions(self.test_secret_id)
            
        self.assertEqual(result, [])

    def test_get_latest_secret_version_id(self):
        """Test getting the latest secret version ID."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._get_latest_secret_version_id(self.test_secret_id)
        
        # Should return the first enabled version (latest)
        self.assertEqual(result, "1")

    def test_get_latest_secret_version_id_no_enabled(self):
        """Test getting latest version when no enabled versions exist."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1", secretmanager.SecretVersion.State.DISABLED),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with self.assertRaises(ValueError) as context:
            self.manager._get_latest_secret_version_id(self.test_secret_id)

        self.assertIn("No enabled versions found", str(context.exception))

    def test_get_oldest_secret_version_id(self):
        """Test getting the oldest secret version ID."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
            self._create_mock_secret_version(self.test_secret_id, "3"),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions

        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._get_oldest_secret_version_id(self.test_secret_id)

        # Should return the last enabled version in reversed order (oldest)
        self.assertEqual(result, "3")

    def test_get_secret_version_specific(self):
        """Test getting a specific secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_response = self._create_mock_access_response(self.test_payload)
        self.mock_client.access_secret_version.return_value = mock_response
        
        result = self.manager.get_secret_version(self.test_secret_id, "1")
        
        self.assertEqual(result, self.test_payload)
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        self.mock_client.access_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_get_secret_version_latest(self):
        """Test getting the latest secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "2")]
        mock_response = self._create_mock_access_response(self.test_payload)
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.access_secret_version.return_value = mock_response
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.get_secret_version(self.test_secret_id, "latest")
        
        self.assertEqual(result, self.test_payload)
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        self.mock_client.access_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_get_secret_version_checksum_mismatch(self):
        """Test getting a secret version with checksum mismatch."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_response = self._create_mock_access_response(self.test_payload)
        mock_response.payload.data_crc32c = 12345  # Wrong checksum
        self.mock_client.access_secret_version.return_value = mock_response
        
        with self.assertRaises(ValueError) as context:
            self.manager.get_secret_version(self.test_secret_id, "1")
        
        self.assertIn("CRC32C checksum mismatch", str(context.exception))

    def test_disable_secret_version_specific(self):
        """Test disabling a specific secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1")]
        mock_response = mock.MagicMock()
        mock_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_response.state = secretmanager.SecretVersion.State.DISABLED
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.disable_secret_version.return_value = mock_response
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, '_secret_version_exists', return_value=True):
                with mock.patch.object(self.manager, '_secret_version_is_enabled', side_effect=[True, False]):
                    self.manager.disable_secret_version(self.test_secret_id, "1")
        
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        self.mock_client.disable_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_disable_secret_version_oldest(self):
        """Test disabling the oldest secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        mock_response = mock.MagicMock()
        mock_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        mock_response.state = secretmanager.SecretVersion.State.DISABLED
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.disable_secret_version.return_value = mock_response
        
        # Mock helper methods
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, '_secret_version_is_enabled', side_effect=[True, False]):
                self.manager.disable_secret_version(self.test_secret_id, "oldest")
        
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        self.mock_client.disable_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_disable_secret_version_latest(self):
        """Test disabling the latest secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        mock_response = mock.MagicMock()
        mock_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        mock_response.state = secretmanager.SecretVersion.State.DISABLED
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.disable_secret_version.return_value = mock_response
        
        # Mock helper methods
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, '_secret_version_is_enabled', side_effect=[True, False]):
                self.manager.disable_secret_version(self.test_secret_id, "latest")
        
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/1"
        self.mock_client.disable_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_disable_secret_version_not_exists(self):
        """Test disabling a version that doesn't exist."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1")]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with self.assertRaises(ValueError) as context:
            self.manager.disable_secret_version(self.test_secret_id, "999")
        
        self.assertIn("Version 999 does not exist", str(context.exception))

    def test_disable_secret_version_secret_not_exists(self):
        """Test disabling a version for a secret that doesn't exist."""
        # Should return silently without error
        self.manager.disable_secret_version(self.test_secret_id, "1")
        self.mock_client.disable_secret_version.assert_not_called()

    def test_rotate_secret_success(self):
        """Test successful secret rotation."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        new_version = self._create_mock_secret_version(self.test_secret_id, "3")
        mock_secret = self._create_mock_secret(self.test_secret_id)
        
        # Create proper mock response for disable_secret_version that passes validation
        disable_response = mock.MagicMock()
        disable_response.name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        disable_response.state = secretmanager.SecretVersion.State.DISABLED
        
        # Mock all the necessary calls
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.add_secret_version.return_value = new_version
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.get_secret.return_value = mock_secret
        self.mock_client.disable_secret_version.return_value = disable_response
        
        # Mock the helper methods to avoid issues
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, '_secret_version_exists', return_value=True):
                with mock.patch.object(self.manager, '_secret_version_is_enabled', side_effect=[True, False]):
                    self.manager.rotate_secret(self.test_secret_id, self.test_payload)
        
        # Verify add_secret_version was called
        self.mock_client.add_secret_version.assert_called_once()
        # Verify oldest version was disabled
        self.mock_client.disable_secret_version.assert_called_once()

    def test_rotate_secret_not_exists(self):
        """Test rotating a secret that doesn't exist."""
        with self.assertRaises(ValueError) as context:
            self.manager.rotate_secret(self.test_secret_id, self.test_payload)
        
        self.assertIn("does not exist", str(context.exception))

    def test_rotate_secret_with_exception(self):
        """Test secret rotation with exception during process."""
        self.manager.secrets_ids = [self.test_secret_id]
        self.mock_client.add_secret_version.side_effect = Exception("API Error")
        
        with self.assertRaises(Exception) as context:
            self.manager.rotate_secret(self.test_secret_id, self.test_payload)
        
        self.assertIn("API Error", str(context.exception))

    def test_secret_version_exists_true(self):
        """Test checking if a secret version exists when it does."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1")]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._secret_version_exists(self.test_secret_id, "1")
        
        self.assertTrue(result)

    def test_secret_version_exists_false(self):
        """Test checking if a secret version exists when it doesn't."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1")]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._secret_version_exists(self.test_secret_id, "999")
        
        self.assertFalse(result)

    def test_secret_version_exists_secret_not_exists(self):
        """Test checking if a secret version exists when the secret doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            result = self.manager._secret_version_exists(self.test_secret_id, "1")
        
        self.assertFalse(result)

    def test_secret_version_is_enabled_true(self):
        """Test checking if a secret version is enabled when it is."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1", secretmanager.SecretVersion.State.ENABLED)]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._secret_version_is_enabled(self.test_secret_id, "1")
        
        self.assertTrue(result)

    def test_secret_version_is_enabled_false(self):
        """Test checking if a secret version is enabled when it's disabled."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1", secretmanager.SecretVersion.State.DISABLED)]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._secret_version_is_enabled(self.test_secret_id, "1")
        
        self.assertFalse(result)

    def test_secret_version_is_enabled_not_exists(self):
        """Test checking if a secret version is enabled when it doesn't exist."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [self._create_mock_secret_version(self.test_secret_id, "1")]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._secret_version_is_enabled(self.test_secret_id, "999")
        
        self.assertFalse(result)

    def test_secret_version_is_enabled_secret_not_exists(self):
        """Test checking if a secret version is enabled when the secret doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            result = self.manager._secret_version_is_enabled(self.test_secret_id, "1")
        
        self.assertFalse(result)

    def test_get_enabled_secret_versions_success(self):
        """Test getting enabled secret versions successfully."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1", secretmanager.SecretVersion.State.ENABLED),
            self._create_mock_secret_version(self.test_secret_id, "2", secretmanager.SecretVersion.State.DISABLED),
            self._create_mock_secret_version(self.test_secret_id, "3", secretmanager.SecretVersion.State.ENABLED),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager._get_enabled_secret_versions(self.test_secret_id)
        
        # Should return only the enabled versions
        self.assertEqual(len(result), 2)
        enabled_version_ids = [v.name.split("/")[-1] for v in result]
        self.assertIn("1", enabled_version_ids)
        self.assertIn("3", enabled_version_ids)
        self.assertNotIn("2", enabled_version_ids)

    def test_purge_old_secret_versions_no_purge_needed(self):
        """Test purging old versions when no purge is needed."""
        self.manager.max_versions_to_keep = 3
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            # Should not call disable_secret_version
            with mock.patch.object(self.manager, 'disable_secret_version') as mock_disable:
                self.manager._purge_old_secret_versions(self.test_secret_id)
                mock_disable.assert_not_called()

    def test_purge_old_secret_versions_purge_needed(self):
        """Test purging old versions when purge is needed."""
        self.manager.max_versions_to_keep = 2
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
            self._create_mock_secret_version(self.test_secret_id, "3"),
        ]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            with mock.patch.object(self.manager, 'disable_secret_version') as mock_disable:
                self.manager._purge_old_secret_versions(self.test_secret_id)
                # Should disable the oldest version (version "1")
                mock_disable.assert_called_once_with(self.test_secret_id, version_id="1")

    def test_is_key_rotation_due_true(self):
        """Test key rotation due check when rotation is due."""
        self.manager.rotation_interval = 30
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Create a secret with a timestamp 31 days ago
        old_timestamp = (datetime.now(timezone.utc) - timedelta(days=31)).strftime("%Y%m%d_%H%M%S")
        mock_secret = self._create_mock_secret(self.test_secret_id, {
            "created_by": "secretmanager-service",
            "last_version_created_at": old_timestamp,
        })
        
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager._is_key_rotation_due(self.test_secret_id)
        
        self.assertTrue(result)

    def test_is_key_rotation_due_false(self):
        """Test key rotation due check when rotation is not due."""
        self.manager.rotation_interval = 30
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Create a secret with a timestamp 10 days ago
        recent_timestamp = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y%m%d_%H%M%S")
        mock_secret = self._create_mock_secret(self.test_secret_id, {
            "created_by": "secretmanager-service",
            "last_version_created_at": recent_timestamp,
        })
        
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager._is_key_rotation_due(self.test_secret_id)
        
        self.assertFalse(result)

    def test_is_key_rotation_due_no_timestamp(self):
        """Test key rotation due check when no timestamp is available."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Create a secret without last_version_created_at timestamp
        mock_secret = self._create_mock_secret(self.test_secret_id, {
            "created_by": "secretmanager-service",
        })
        
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager._is_key_rotation_due(self.test_secret_id)
        
        self.assertFalse(result)

    def test_secret_exists_true(self):
        """Test checking if a secret exists when it does exist and is managed by the service."""
        mock_secret = self._create_mock_secret(self.test_secret_id)
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager._secret_exists(self.test_secret_id)
        
        self.assertTrue(result)

    def test_secret_exists_false_not_managed(self):
        """Test checking if a secret exists when it exists but is not managed by the service."""
        mock_secret = self._create_mock_secret(self.test_secret_id, {
            "created_by": "other-service",
        })
        self.mock_client.secret_path.return_value = mock_secret.name
        self.mock_client.get_secret.return_value = mock_secret
        
        result = self.manager._secret_exists(self.test_secret_id)
        
        self.assertFalse(result)

    def test_secret_exists_false_not_found(self):
        """Test checking if a secret exists when it doesn't exist."""
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_secret.side_effect = Exception("Not found")
        
        result = self.manager._secret_exists(self.test_secret_id)
        
        self.assertFalse(result)

    def test_get_secret_version_oldest(self):
        """Test getting the oldest secret version."""
        self.manager.secrets_ids = [self.test_secret_id]
        mock_versions = [
            self._create_mock_secret_version(self.test_secret_id, "1"),
            self._create_mock_secret_version(self.test_secret_id, "2"),
        ]
        mock_response = self._create_mock_access_response(self.test_payload)
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.list_secret_versions.return_value = mock_versions
        self.mock_client.access_secret_version.return_value = mock_response
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.get_secret_version(self.test_secret_id, "oldest")
        
        self.assertEqual(result, self.test_payload)
        expected_name = f"projects/{self.project_id}/secrets/{self.test_secret_id}/versions/2"
        self.mock_client.access_secret_version.assert_called_once_with(request={"name": expected_name})

    def test_get_secret_version_secret_not_in_cache(self):
        """Test getting a secret version when secret is not in cache."""
        # secret_id is not in secrets_ids
        mock_response = self._create_mock_access_response(self.test_payload)
        self.mock_client.access_secret_version.return_value = mock_response
        
        with self.assertRaises(ValueError) as context:
            self.manager.get_secret_version(self.test_secret_id, "1")
        
        self.assertIn("does not exist", str(context.exception))

    def test_is_different_user_access_secret_not_exists(self):
        """Test is_different_user_access when secret doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            result = self.manager.is_different_user_access(self.test_secret_id, ["user@example.com"])
        
        self.assertTrue(result)

    def test_is_different_user_access_same_users(self):
        """Test is_different_user_access when users are the same."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with same users
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:user1@example.com", "user:user2@example.com"]
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = [mock_binding]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.is_different_user_access(
                self.test_secret_id, 
                ["user1@example.com", "user2@example.com"]
            )
        
        self.assertFalse(result)

    def test_is_different_user_access_different_users(self):
        """Test is_different_user_access when users are different."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with different users
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:old_user@example.com"]
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = [mock_binding]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.is_different_user_access(
                self.test_secret_id, 
                ["new_user@example.com"]
            )
        
        self.assertTrue(result)

    def test_is_different_user_access_no_accessor_binding(self):
        """Test is_different_user_access when there's no accessor binding."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with no accessor bindings
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretVersionManager"
        mock_binding.members = ["user:admin@example.com"]
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = [mock_binding]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.is_different_user_access(
                self.test_secret_id, 
                ["user@example.com"]
            )
        
        self.assertTrue(result)

    def test_is_different_user_access_api_error(self):
        """Test is_different_user_access when API call fails."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.side_effect = Exception("API Error")
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            result = self.manager.is_different_user_access(
                self.test_secret_id, 
                ["user@example.com"]
            )
        
        self.assertTrue(result)

    def test_update_secret_access_secret_not_exists(self):
        """Test update_secret_access when secret doesn't exist."""
        with mock.patch.object(self.manager, '_secret_exists', return_value=False):
            with self.assertRaises(ValueError) as context:
                self.manager.update_secret_access(self.test_secret_id, ["user@example.com"])
        
        self.assertIn("does not exist", str(context.exception))

    def test_update_secret_access_existing_binding(self):
        """Test update_secret_access when accessor binding already exists."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with existing accessor binding
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:old_user@example.com"]
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = [mock_binding]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            self.manager.update_secret_access(
                self.test_secret_id, 
                ["new_user@example.com"]
            )
        
        # Verify the binding was updated
        self.assertEqual(mock_binding.members, ["user:new_user@example.com"])
        self.mock_client.set_iam_policy.assert_called_once()

    def test_update_secret_access_new_binding(self):
        """Test update_secret_access when no accessor binding exists."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with no accessor bindings
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretVersionManager"
        mock_binding.members = ["user:admin@example.com"]
        
        # Create a mock bindings collection that supports the add() method
        mock_bindings = mock.MagicMock()
        mock_bindings.__iter__ = mock.MagicMock(return_value=iter([mock_binding]))
        mock_bindings.add = mock.MagicMock()
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = mock_bindings

        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            self.manager.update_secret_access(
                self.test_secret_id, 
                ["user@example.com"]
            )
        
        # Verify new binding was added
        mock_bindings.add.assert_called_once()
        self.mock_client.set_iam_policy.assert_called_once()

    def test_update_secret_access_multiple_users(self):
        """Test update_secret_access with multiple users."""
        self.manager.secrets_ids = [self.test_secret_id]
        
        # Mock the IAM policy with existing accessor binding
        mock_binding = mock.MagicMock()
        mock_binding.role = "roles/secretmanager.secretAccessor"
        mock_binding.members = ["user:old_user@example.com"]
        
        mock_policy = mock.MagicMock()
        mock_policy.bindings = [mock_binding]
        
        self.mock_client.secret_path.return_value = f"projects/{self.project_id}/secrets/{self.test_secret_id}"
        self.mock_client.get_iam_policy.return_value = mock_policy
        
        with mock.patch.object(self.manager, '_secret_exists', return_value=True):
            self.manager.update_secret_access(
                self.test_secret_id, 
                ["user1@example.com", "user2@example.com", "user3@example.com"]
            )
        
        # Verify the binding was updated with all users
        expected_members = ["user:user1@example.com", "user:user2@example.com", "user:user3@example.com"]
        self.assertEqual(mock_binding.members, expected_members)
        self.mock_client.set_iam_policy.assert_called_once()


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
        self.manager = SecretManager(self.project_id, self.logger, rotation_interval=30, max_versions_to_keep=3)
        self.test_secret_id = f"integration-test-secret-{int(time.time())}"
        self.test_payload = b"integration-test-payload"

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up any secrets created during tests
        try:
            if self.test_secret_id in self.manager.secrets_ids:
                self.manager.delete_secret(self.test_secret_id)
        except Exception as e:
            self.logger.warning(f"Failed to clean up test secret: {e}")

    def test_full_secret_lifecycle(self):
        """Test creating, adding versions, rotating, and deleting a secret."""
        # Add first version (creates the secret)
        version1 = self.manager.add_secret_version(self.test_secret_id, self.test_payload)
        self.assertIsNotNone(version1)
        
        # Verify secret exists
        secret = self.manager.get_secret(self.test_secret_id)
        self.assertEqual(secret.labels["created_by"], "secretmanager-service")
        
        # Add second version
        version2 = self.manager.add_secret_version(self.test_secret_id, b"second-payload")
        self.assertIsNotNone(version2)
        
        # List versions
        versions = self.manager._get_secret_versions(self.test_secret_id)
        self.assertGreaterEqual(len(versions), 2)
        
        # Get specific version
        retrieved_payload = self.manager.get_secret_version(self.test_secret_id, "latest")
        self.assertEqual(retrieved_payload, b"second-payload")
        
        # Rotate secret
        self.manager.rotate_secret(self.test_secret_id, b"rotated-payload")
        
        # Verify latest version has rotated payload
        latest_payload = self.manager.get_secret_version(self.test_secret_id, "latest")
        self.assertEqual(latest_payload, b"rotated-payload")
        
        # Delete secret
        self.manager.delete_secret(self.test_secret_id)
        
        # Verify secret is removed from secrets_ids
        self.assertNotIn(self.test_secret_id, self.manager.secrets_ids)

    def test_max_versions_enforcement(self):
        """Test that max versions limit is enforced."""
        self.manager.max_versions_to_keep = 2
        
        # Add 3 versions (should exceed max)
        self.manager.add_secret_version(self.test_secret_id, b"payload1")
        self.manager.add_secret_version(self.test_secret_id, b"payload2")
        self.manager.add_secret_version(self.test_secret_id, b"payload3")
        
        # Check that only 2 versions are enabled
        versions = self.manager._get_secret_versions(self.test_secret_id)
        enabled_versions = [v for v in versions if v.state == secretmanager.SecretVersion.State.ENABLED]
        self.assertLessEqual(len(enabled_versions), 2)


if __name__ == '__main__':
    # Configure logging to reduce noise during testing
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    # Run the tests
    unittest.main()
