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

# This is the test suite for the SecretService class, which manages secrets in Google Cloud Secret Manager.

import unittest
import io
import sys
from unittest import mock

from secret_service import SecretService
from google.cloud import secretmanager

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

class TestSecretService(unittest.TestCase):

    @mock.patch('secret_service.secretmanager.SecretManagerServiceClient')
    @mock.patch('secret_service.yaml.safe_load')
    def setUp(self, mock_safe_load, mock_secret_manager_client):
        """Set up the test environment before each test."""
        with SilencePrint():
            self.mock_config = {
                'project_id': 'test-project',
                'secret_name_prefix': 'test-prefix',
                'rotation_interval': 30,
                'max_versions_to_keep': 3,
                'bucket_name': 'test-bucket',
                'log_file_prefix': 'test-log-prefix'
            }
            mock_safe_load.return_value = self.mock_config

            self.mock_client = mock_secret_manager_client.return_value

            # Mock list_secrets to avoid calling it in init
            self.mock_client.list_secrets.return_value = []

            self.secret_service = SecretService(self.mock_config)

            # Clear secrets_names gathered during init
            self.secret_service.secrets_names = []

    def test_init_with_missing_config(self):
        """Test initialization with missing configuration parameters."""
        with self.assertRaises(ValueError):
            with SilencePrint():
                SecretService({})

    def test_get_secrets_names(self):
        """Test retrieving and filtering secret names."""
        mock_secret1 = mock.Mock()
        mock_secret1.name = 'projects/test-project/secrets/test-prefix-secret1'
        mock_secret2 = mock.Mock()
        mock_secret2.name = 'projects/test-project/secrets/other-secret'
        mock_secret3 = mock.Mock()
        mock_secret3.name = 'projects/test-project/secrets/test-prefix-secret3'

        self.mock_client.list_secrets.return_value = [mock_secret1, mock_secret2, mock_secret3]

        with SilencePrint():
            self.secret_service._get_secrets_names()

        self.assertEqual(len(self.secret_service.secrets_names), 2)
        self.assertIn('test-prefix-secret1', self.secret_service.secrets_names)
        self.assertIn('test-prefix-secret3', self.secret_service.secrets_names)

    def test_create_secret_new(self):
        """Test creating a new secret."""
        secret_id = 'new-secret'
        secret_name = f"{self.secret_service.secret_name_prefix}-{secret_id}"

        mock_response = mock.Mock()
        mock_response.name = f"projects/{self.secret_service.project_id}/secrets/{secret_name}"
        self.mock_client.create_secret.return_value = mock_response

        with SilencePrint():
            result = self.secret_service._create_secret(secret_id)

        self.mock_client.create_secret.assert_called_once()
        self.assertEqual(result, mock_response.name)
        self.assertIn(secret_name, self.secret_service.secrets_names)

    def test_create_secret_existing(self):
        """Test creating a secret that already exists."""
        secret_id = 'existing-secret'
        secret_name = f"{self.secret_service.secret_name_prefix}-{secret_id}"
        self.secret_service.secrets_names = [secret_name]

        expected_path = f"projects/{self.secret_service.project_id}/secrets/{secret_name}"
        self.mock_client.secret_path.return_value = expected_path

        with SilencePrint():
            result = self.secret_service._create_secret(secret_id)

        self.mock_client.secret_path.assert_called_once_with(self.secret_service.project_id, secret_name)
        self.mock_client.create_secret.assert_not_called()
        self.assertEqual(result, expected_path)

    @mock.patch('secret_service.google_crc32c.Checksum')
    def test_add_secret_version(self, mock_checksum):
        """Test adding a new secret version."""
        secret_name = 'my-secret'
        payload = b'my-secret-payload'

        created_secret_path = f"projects/{self.secret_service.project_id}/secrets/{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service._create_secret = mock.Mock(return_value=created_secret_path)

        mock_crc = mock_checksum.return_value
        mock_crc.hexdigest.return_value = '1234abcd'

        mock_response = mock.Mock()
        mock_response.name = f"{created_secret_path}/versions/1"
        self.mock_client.add_secret_version.return_value = mock_response

        with SilencePrint():
            result = self.secret_service.add_secret_version(secret_name, payload)

        self.secret_service._create_secret.assert_called_once_with(secret_name)
        self.mock_client.add_secret_version.assert_called_once()
        self.assertEqual(result, mock_response.name)

    @mock.patch('secret_service.google_crc32c.Checksum')
    def test_add_secret_version_with_str_payload(self, mock_checksum):
        """Test adding a new secret version with a string payload."""
        secret_name = 'my-secret'
        payload = 'my-secret-payload'

        created_secret_path = f"projects/{self.secret_service.project_id}/secrets/{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service._create_secret = mock.Mock(return_value=created_secret_path)

        mock_crc = mock_checksum.return_value
        mock_crc.hexdigest.return_value = '1234abcd'

        mock_response = mock.Mock()
        mock_response.name = f"{created_secret_path}/versions/1"
        self.mock_client.add_secret_version.return_value = mock_response

        with SilencePrint():
            self.secret_service.add_secret_version(secret_name, payload)

        payload_bytes = payload.encode('utf-8')
        mock_crc.update.assert_called_once_with(payload_bytes)

    def test_add_secret_version_with_invalid_payload(self):
        """Test adding a secret version with an invalid payload type."""
        secret_name = 'my-secret'
        payload = 12345

        created_secret_path = f"projects/{self.secret_service.project_id}/secrets/{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service._create_secret = mock.Mock(return_value=created_secret_path)

        with self.assertRaises(TypeError):
            with SilencePrint():
                self.secret_service.add_secret_version(secret_name, payload)

    @mock.patch('secret_service.google_crc32c.Checksum')
    def test_get_secret_version_latest(self, mock_checksum):
        """Test getting the latest version of a secret."""
        secret_name = 'my-secret'
        secret_full_name = f"{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service.secrets_names = [secret_full_name]

        mock_version1 = mock.Mock()
        mock_version1.name = f"projects/test-project/secrets/{secret_full_name}/versions/1"
        mock_version1.state = secretmanager.SecretVersion.State.DISABLED
        mock_version2 = mock.Mock()
        mock_version2.name = f"projects/test-project/secrets/{secret_full_name}/versions/2"
        mock_version2.state = secretmanager.SecretVersion.State.ENABLED
        self.mock_client.list_secret_versions.return_value = [mock_version1, mock_version2]

        mock_payload = mock.Mock()
        mock_payload.data = b'latest-version-data'
        mock_payload.data_crc32c = 12345

        mock_response = mock.Mock()
        mock_response.payload = mock_payload
        self.mock_client.access_secret_version.return_value = mock_response

        mock_crc = mock_checksum.return_value
        mock_crc.hexdigest.return_value = hex(12345)

        with SilencePrint():
            result = self.secret_service.get_secret_version(secret_name, version_id='latest')

        self.assertEqual(result, b'latest-version-data')
        self.mock_client.access_secret_version.assert_called_with(
            request={'name': f'projects/test-project/secrets/{secret_full_name}/versions/2'}
        )

    def test_get_secret_version_not_found(self):
        """Test getting a version of a non-existent secret."""
        with self.assertRaises(ValueError):
            with SilencePrint():
                self.secret_service.get_secret_version('non-existent-secret')

    @mock.patch('secret_service.google_crc32c.Checksum')
    def test_get_secret_version_crc32c_mismatch(self, mock_checksum):
        """Test CRC32C checksum mismatch when getting a secret."""
        secret_name = 'my-secret'
        secret_full_name = f"{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service.secrets_names = [secret_full_name]

        mock_payload = mock.Mock()
        mock_payload.data = b'data'
        mock_payload.data_crc32c = 12345

        mock_response = mock.Mock()
        mock_response.payload = mock_payload
        self.mock_client.access_secret_version.return_value = mock_response

        mock_crc = mock_checksum.return_value
        mock_crc.hexdigest.return_value = hex(54321)

        with self.assertRaises(ValueError):
            with SilencePrint():
                self.secret_service.get_secret_version(secret_name, version_id='1')

    def test_disable_secret_version(self):
        """Test disabling a secret version."""
        secret_name = 'my-secret'
        secret_full_name = f"{self.secret_service.secret_name_prefix}-{secret_name}"
        self.secret_service.secrets_names = [secret_full_name]

        mock_version = mock.Mock()
        mock_version.name = f"projects/test-project/secrets/{secret_full_name}/versions/1"
        mock_version.state = secretmanager.SecretVersion.State.ENABLED
        self.mock_client.list_secret_versions.return_value = [mock_version]

        with SilencePrint():
            self.secret_service.disable_secret_version(secret_name, version_id='latest')

        self.mock_client.disable_secret_version.assert_called_once()

    def test_disable_secret_version_not_found(self):
        """Test disabling a version of a non-existent secret."""
        with SilencePrint():
            self.secret_service.disable_secret_version('non-existent-secret')
        self.mock_client.disable_secret_version.assert_not_called()

if __name__ == '__main__':
    unittest.main()
