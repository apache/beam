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

import unittest

from unittest import mock
from service_account import ServiceAccountManager
from google.cloud.iam_admin_v1 import types
from google.api_core import exceptions

class TestServiceAccountManagerUnit(unittest.TestCase):
    """Unit tests for ServiceAccountManager with mocked Google Cloud IAM client."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = "test-project-123"
        self.test_account_id = "test-service-account"
        self.test_display_name = "Test Service Account"
        
        # Patch the IAM client
        self.iam_client_patcher = mock.patch('service_account.iam_admin_v1.IAMClient')
        self.mock_iam_client_class = self.iam_client_patcher.start()
        self.mock_iam_client = self.mock_iam_client_class.return_value
        
        # Create the service account manager
        self.manager = ServiceAccountManager(self.project_id)

    def tearDown(self):
        """Tear down test fixtures."""
        self.iam_client_patcher.stop()

    def _create_mock_service_account(self, account_id: str, disabled: bool = False) -> types.ServiceAccount:
        """Helper method to create a mock service account."""
        mock_account = types.ServiceAccount()
        mock_account.name = f"projects/{self.project_id}/serviceAccounts/{account_id}@{self.project_id}.iam.gserviceaccount.com"
        mock_account.email = f"{account_id}@{self.project_id}.iam.gserviceaccount.com"
        mock_account.display_name = account_id
        mock_account.disabled = disabled
        mock_account.project_id = self.project_id
        mock_account.unique_id = f"123456789{account_id}"
        return mock_account

    def _create_mock_service_account_key(self, account_id: str, key_id: str = "test-key-id") -> types.ServiceAccountKey:
        """Helper method to create a mock service account key."""
        mock_key = types.ServiceAccountKey()
        mock_key.name = f"projects/{self.project_id}/serviceAccounts/{account_id}@{self.project_id}.iam.gserviceaccount.com/keys/{key_id}"
        mock_key.private_key_data = b'{"type": "service_account", "project_id": "test-project"}'
        return mock_key

    def test_init(self):
        """Test ServiceAccountManager initialization."""
        self.assertEqual(self.manager.project_id, self.project_id)
        self.mock_iam_client_class.assert_called_once()

    def test_create_service_account_success(self):
        """Test successful service account creation."""
        expected_account = self._create_mock_service_account(self.test_account_id)
        self.mock_iam_client.create_service_account.return_value = expected_account

        result = self.manager.create_service_account(self.test_account_id, self.test_display_name)

        self.assertEqual(result, expected_account)
        self.mock_iam_client.create_service_account.assert_called_once()

        # Verify the request structure
        call_args = self.mock_iam_client.create_service_account.call_args
        request = call_args[1]['request']
        self.assertEqual(request.account_id, self.test_account_id)
        self.assertEqual(request.name, f"projects/{self.project_id}")
        self.assertEqual(request.service_account.display_name, self.test_display_name)

    def test_create_service_account_already_exists(self):
        """Test service account creation when account already exists."""
        existing_account = self._create_mock_service_account(self.test_account_id)
        
        # Mock the conflict exception and then successful get
        self.mock_iam_client.create_service_account.side_effect = exceptions.Conflict("Account already exists")
        self.mock_iam_client.get_service_account.return_value = existing_account

        result = self.manager.create_service_account(self.test_account_id, self.test_display_name)

        self.assertEqual(result, existing_account)
        self.mock_iam_client.create_service_account.assert_called_once()
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_enable_service_account(self):
        """Test enabling a service account."""
        enabled_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        self.mock_iam_client.get_service_account.return_value = enabled_account

        result = self.manager.enable_service_account(self.test_account_id)

        self.assertEqual(result, enabled_account)
        self.mock_iam_client.enable_service_account.assert_called_once()
        self.mock_iam_client.get_service_account.assert_called()

    def test_disable_service_account(self):
        """Test disabling a service account."""
        disabled_account = self._create_mock_service_account(self.test_account_id, disabled=True)
        self.mock_iam_client.get_service_account.return_value = disabled_account

        result = self.manager.disable_service_account(self.test_account_id)

        self.assertEqual(result, disabled_account)
        self.mock_iam_client.disable_service_account.assert_called_once()
        self.mock_iam_client.get_service_account.assert_called()

    def test_delete_service_account(self):
        """Test deleting a service account."""
        self.manager.delete_service_account(self.test_account_id)

        self.mock_iam_client.delete_service_account.assert_called_once()

        # Verify the request structure
        call_args = self.mock_iam_client.delete_service_account.call_args
        request = call_args[1]['request']
        expected_name = f"projects/{self.project_id}/serviceAccounts/{self.test_account_id}"
        self.assertEqual(request.name, expected_name)

    def test_list_service_accounts(self):
        """Test listing all service accounts in the project."""
        mock_accounts = [
            self._create_mock_service_account("account1"),
            self._create_mock_service_account("account2", disabled=True),
            self._create_mock_service_account("account3"),
        ]
        
        mock_response = mock.MagicMock()
        mock_response.accounts = mock_accounts
        # Make the mock response iterable so list(accounts) works
        mock_response.__iter__ = lambda self: iter(mock_accounts)
        self.mock_iam_client.list_service_accounts.return_value = mock_response

        result = self.manager.list_service_accounts()

        self.assertEqual(result, mock_accounts)
        self.mock_iam_client.list_service_accounts.assert_called_once()

        # Verify the request structure
        call_args = self.mock_iam_client.list_service_accounts.call_args
        request = call_args[1]['request']
        self.assertEqual(request.name, f"projects/{self.project_id}")

    def test_create_service_account_key_enabled_account(self):
        """Test creating a key for an enabled service account."""
        enabled_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        mock_key = self._create_mock_service_account_key(self.test_account_id)
        
        self.mock_iam_client.get_service_account.return_value = enabled_account
        self.mock_iam_client.create_service_account_key.return_value = mock_key

        result = self.manager.create_service_account_key(self.test_account_id)

        self.assertEqual(result, mock_key)
        self.mock_iam_client.get_service_account.assert_called_once()
        self.mock_iam_client.create_service_account_key.assert_called_once()

    def test_create_service_account_key_disabled_account(self):
        """Test creating a key for a disabled service account."""
        disabled_account = self._create_mock_service_account(self.test_account_id, disabled=True)
        enabled_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        mock_key = self._create_mock_service_account_key(self.test_account_id)
        
        # First call returns disabled account, second call in enable_service_account, third call in enable_service_account returns enabled account
        self.mock_iam_client.get_service_account.side_effect = [disabled_account, enabled_account, enabled_account]
        self.mock_iam_client.create_service_account_key.return_value = mock_key

        result = self.manager.create_service_account_key(self.test_account_id)

        self.assertEqual(result, mock_key)
        # Should call get_service_account three times (once to check status, twice in enable_service_account)
        self.assertEqual(self.mock_iam_client.get_service_account.call_count, 3)
        self.mock_iam_client.enable_service_account.assert_called_once()
        self.mock_iam_client.create_service_account_key.assert_called_once()

    def test_create_service_account_key_not_found(self):
        """Test creating a key for a non-existent service account."""
        self.mock_iam_client.get_service_account.side_effect = exceptions.NotFound("Account not found")

        with self.assertRaises(exceptions.NotFound):
            self.manager.create_service_account_key(self.test_account_id)

    def test_delete_service_account_key(self):
        """Test deleting a service account key."""
        key_id = "test-key-id"
        
        self.manager.delete_service_account_key(self.test_account_id, key_id)

        self.mock_iam_client.delete_service_account_key.assert_called_once()

    def test_list_service_account_keys(self):
        """Test listing service account keys."""
        mock_keys = [
            self._create_mock_service_account_key(self.test_account_id, "key1"),
            self._create_mock_service_account_key(self.test_account_id, "key2"),
        ]
        
        mock_response = mock.MagicMock()
        mock_response.keys = mock_keys
        self.mock_iam_client.list_service_account_keys.return_value = mock_response

        result = self.manager.list_service_account_keys(self.test_account_id)

        self.assertEqual(result, mock_keys)
        self.mock_iam_client.list_service_account_keys.assert_called_once()

    @mock.patch('service_account.service_account.Credentials.from_service_account_info')
    @mock.patch('service_account.Request')
    def test_test_service_account_key_valid(self, mock_request_class, mock_credentials_class):
        """Test testing a valid service account key."""
        mock_credentials = mock.MagicMock()
        mock_credentials_class.return_value = mock_credentials
        
        key_data = b'{"type": "service_account", "project_id": "test-project"}'
        
        result = self.manager.test_service_account_key(key_data)

        self.assertTrue(result)
        mock_credentials_class.assert_called_once()
        mock_credentials.refresh.assert_called_once()

    @mock.patch('service_account.service_account.Credentials.from_service_account_info')
    def test_test_service_account_key_invalid_json(self, mock_credentials_class):
        """Test testing an invalid JSON service account key."""
        key_data = b'invalid json'
        
        result = self.manager.test_service_account_key(key_data)

        self.assertFalse(result)
        mock_credentials_class.assert_not_called()

    @mock.patch('service_account.service_account.Credentials.from_service_account_info')
    def test_test_service_account_key_auth_error(self, mock_credentials_class):
        """Test testing a service account key with authentication error."""
        mock_credentials = mock.MagicMock()
        mock_credentials.refresh.side_effect = Exception("Authentication failed")
        mock_credentials_class.return_value = mock_credentials
        
        key_data = b'{"type": "service_account", "project_id": "test-project"}'
        
        result = self.manager.test_service_account_key(key_data)

        self.assertFalse(result)

if __name__ == '__main__':
    # Configure logging to reduce noise during testing
    import logging
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    # Run the tests
    unittest.main()
