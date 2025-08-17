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
        
        # Create a mock logger
        self.mock_logger = mock.MagicMock()
        
        # Create the service account manager
        self.manager = ServiceAccountManager(self.project_id, self.mock_logger)

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

        with mock.patch.object(self.manager, '_service_account_exists', return_value=True):
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
        
        with mock.patch.object(self.manager, '_service_account_is_enabled', return_value=True):
            self.manager.enable_service_account(self.test_account_id)

        self.mock_iam_client.enable_service_account.assert_called_once()
        
        # Verify the request structure
        call_args = self.mock_iam_client.enable_service_account.call_args
        request = call_args[1]['request']
        expected_name = f"projects/{self.project_id}/serviceAccounts/{self.test_account_id}@{self.project_id}.iam.gserviceaccount.com"
        self.assertEqual(request.name, expected_name)

    def test_disable_service_account(self):
        """Test disabling a service account."""
        disabled_account = self._create_mock_service_account(self.test_account_id, disabled=True)
        
        with mock.patch.object(self.manager, '_service_account_is_enabled', return_value=False):
            self.manager.disable_service_account(self.test_account_id)

        self.mock_iam_client.disable_service_account.assert_called_once()
        
        # Verify the request structure
        call_args = self.mock_iam_client.disable_service_account.call_args
        request = call_args[1]['request']
        expected_name = f"projects/{self.project_id}/serviceAccounts/{self.test_account_id}@{self.project_id}.iam.gserviceaccount.com"
        self.assertEqual(request.name, expected_name)

    def test_delete_service_account(self):
        """Test deleting a service account."""
        with mock.patch.object(self.manager, '_service_account_exists', return_value=False):
            self.manager.delete_service_account(self.test_account_id)

        self.mock_iam_client.delete_service_account.assert_called_once()

        # Verify the request structure
        call_args = self.mock_iam_client.delete_service_account.call_args
        request = call_args[1]['request']
        expected_name = f"projects/{self.project_id}/serviceAccounts/{self.test_account_id}@{self.project_id}.iam.gserviceaccount.com"
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

        result = self.manager._get_service_accounts()

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
        
        with mock.patch.object(self.manager, '_service_account_key_exists', return_value=True):
            result = self.manager.create_service_account_key(self.test_account_id)

        self.assertEqual(result, mock_key)
        self.mock_iam_client.get_service_account.assert_called_once()
        self.mock_iam_client.create_service_account_key.assert_called_once()

    def test_create_service_account_key_disabled_account(self):
        """Test creating a key for a disabled service account."""
        disabled_account = self._create_mock_service_account(self.test_account_id, disabled=True)
        enabled_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        mock_key = self._create_mock_service_account_key(self.test_account_id)
        
        # First call returns disabled account, then we mock the enable flow
        self.mock_iam_client.get_service_account.return_value = disabled_account
        self.mock_iam_client.create_service_account_key.return_value = mock_key

        with mock.patch.object(self.manager, '_service_account_is_enabled', return_value=True), \
             mock.patch.object(self.manager, '_service_account_key_exists', return_value=True):
            result = self.manager.create_service_account_key(self.test_account_id)

        self.assertEqual(result, mock_key)
        # Should call get_service_account once to check if it's disabled
        self.mock_iam_client.get_service_account.assert_called_once()
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
        
        with mock.patch.object(self.manager, '_service_account_key_exists', return_value=False):
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

        result = self.manager._get_service_account_keys(self.test_account_id)

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

    def test_normalize_account_email_with_email(self):
        """Test normalizing account email when input is already a full email."""
        full_email = f"{self.test_account_id}@{self.project_id}.iam.gserviceaccount.com"
        result = self.manager._normalize_account_email(full_email)
        self.assertEqual(result, full_email)

    def test_normalize_account_email_with_id(self):
        """Test normalizing account email when input is just the account ID."""
        result = self.manager._normalize_account_email(self.test_account_id)
        expected_email = f"{self.test_account_id}@{self.project_id}.iam.gserviceaccount.com"
        self.assertEqual(result, expected_email)

    def test_service_account_exists_true(self):
        """Test _service_account_exists when service account exists."""
        mock_account = self._create_mock_service_account(self.test_account_id)
        self.mock_iam_client.get_service_account.return_value = mock_account
        
        result = self.manager._service_account_exists(self.test_account_id)
        
        self.assertTrue(result)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_service_account_exists_false(self):
        """Test _service_account_exists when service account does not exist."""
        self.mock_iam_client.get_service_account.side_effect = exceptions.NotFound("Not found")
        
        result = self.manager._service_account_exists(self.test_account_id)
        
        self.assertFalse(result)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_service_account_is_enabled_true(self):
        """Test _service_account_is_enabled when service account is enabled."""
        mock_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        self.mock_iam_client.get_service_account.return_value = mock_account
        
        result = self.manager._service_account_is_enabled(self.test_account_id)
        
        self.assertTrue(result)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_service_account_is_enabled_false(self):
        """Test _service_account_is_enabled when service account is disabled."""
        mock_account = self._create_mock_service_account(self.test_account_id, disabled=True)
        self.mock_iam_client.get_service_account.return_value = mock_account
        
        result = self.manager._service_account_is_enabled(self.test_account_id)
        
        self.assertFalse(result)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_service_account_is_enabled_not_found(self):
        """Test _service_account_is_enabled when service account does not exist."""
        self.mock_iam_client.get_service_account.side_effect = exceptions.NotFound("Not found")
        
        result = self.manager._service_account_is_enabled(self.test_account_id)
        
        self.assertFalse(result)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_get_service_account_success(self):
        """Test successful retrieval of a service account."""
        mock_account = self._create_mock_service_account(self.test_account_id)
        self.mock_iam_client.get_service_account.return_value = mock_account
        
        result = self.manager.get_service_account(self.test_account_id)
        
        self.assertEqual(result, mock_account)
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_get_service_account_not_found(self):
        """Test retrieval of a non-existent service account."""
        self.mock_iam_client.get_service_account.side_effect = exceptions.NotFound("Not found")
        
        with self.assertRaises(exceptions.NotFound):
            self.manager.get_service_account(self.test_account_id)
        
        self.mock_iam_client.get_service_account.assert_called_once()

    def test_service_account_key_exists_true(self):
        """Test _service_account_key_exists when key exists."""
        key_id = "test-key-id"
        mock_key = self._create_mock_service_account_key(self.test_account_id, key_id)
        mock_response = mock.MagicMock()
        mock_response.keys = [mock_key]
        self.mock_iam_client.list_service_account_keys.return_value = mock_response
        
        result = self.manager._service_account_key_exists(self.test_account_id, key_id)
        
        self.assertTrue(result)
        self.mock_iam_client.list_service_account_keys.assert_called_once()

    def test_service_account_key_exists_false(self):
        """Test _service_account_key_exists when key does not exist."""
        key_id = "test-key-id"
        other_key = self._create_mock_service_account_key(self.test_account_id, "other-key-id")
        mock_response = mock.MagicMock()
        mock_response.keys = [other_key]
        self.mock_iam_client.list_service_account_keys.return_value = mock_response
        
        result = self.manager._service_account_key_exists(self.test_account_id, key_id)
        
        self.assertFalse(result)
        self.mock_iam_client.list_service_account_keys.assert_called_once()

    def test_delete_service_account_key_not_found(self):
        """Test deleting a non-existent service account key."""
        key_id = "non-existent-key"
        self.mock_iam_client.delete_service_account_key.side_effect = exceptions.NotFound("Key not found")
        
        with self.assertRaises(exceptions.NotFound):
            self.manager.delete_service_account_key(self.test_account_id, key_id)
        
        self.mock_iam_client.delete_service_account_key.assert_called_once()

    def test_delete_service_account_key_failed_precondition(self):
        """Test deleting a service account key with failed precondition."""
        key_id = "test-key-id"
        self.mock_iam_client.delete_service_account_key.side_effect = exceptions.FailedPrecondition("Cannot delete")
        
        with self.assertRaises(exceptions.FailedPrecondition):
            self.manager.delete_service_account_key(self.test_account_id, key_id)
        
        self.mock_iam_client.delete_service_account_key.assert_called_once()

    def test_delete_service_account_key_unexpected_error(self):
        """Test deleting a service account key with unexpected error."""
        key_id = "test-key-id"
        self.mock_iam_client.delete_service_account_key.side_effect = Exception("Unexpected error")
        
        with self.assertRaises(Exception):
            self.manager.delete_service_account_key(self.test_account_id, key_id)
        
        self.mock_iam_client.delete_service_account_key.assert_called_once()

    @mock.patch('service_account.time.sleep')
    def test_test_service_account_key_retry_success(self, mock_sleep):
        """Test service account key testing with retry logic success."""
        mock_credentials = mock.MagicMock()
        
        # First attempt fails, second succeeds
        mock_credentials.refresh.side_effect = [Exception("Auth failed"), None]
        
        with mock.patch('service_account.service_account.Credentials.from_service_account_info', return_value=mock_credentials):
            key_data = b'{"type": "service_account", "project_id": "test-project"}'
            result = self.manager.test_service_account_key(key_data)
        
        self.assertTrue(result)
        self.assertEqual(mock_credentials.refresh.call_count, 2)
        mock_sleep.assert_called_once_with(2)  # delay is doubled before sleep (1 * 2 = 2)

    @mock.patch('service_account.time.sleep')
    def test_test_service_account_key_retry_exhausted(self, mock_sleep):
        """Test service account key testing when all retries are exhausted."""
        mock_credentials = mock.MagicMock()
        mock_credentials.refresh.side_effect = Exception("Auth failed")
        
        with mock.patch('service_account.service_account.Credentials.from_service_account_info', return_value=mock_credentials):
            key_data = b'{"type": "service_account", "project_id": "test-project"}'
            result = self.manager.test_service_account_key(key_data)
        
        self.assertFalse(result)
        self.assertEqual(mock_credentials.refresh.call_count, 3)  # max_retries
        # Sleep is called with 2, then 4 (delay is doubled each time)
        self.assertEqual(mock_sleep.call_count, 2)  # 2 retry delays
        mock_sleep.assert_any_call(2)  # First retry delay (1 * 2)
        mock_sleep.assert_any_call(4)  # Second retry delay (2 * 2)

    def test_create_service_account_timeout(self):
        """Test service account creation timeout scenario."""
        expected_account = self._create_mock_service_account(self.test_account_id)
        self.mock_iam_client.create_service_account.return_value = expected_account

        # Mock the helper method to always return False (service account never exists)
        with mock.patch.object(self.manager, '_service_account_exists', return_value=False):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.create_service_account(self.test_account_id, self.test_display_name)

    def test_enable_service_account_timeout(self):
        """Test service account enabling timeout scenario."""
        # Mock the helper method to always return False (service account never gets enabled)
        with mock.patch.object(self.manager, '_service_account_is_enabled', return_value=False):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.enable_service_account(self.test_account_id)

    def test_disable_service_account_timeout(self):
        """Test service account disabling timeout scenario."""
        # Mock the helper method to always return True (service account never gets disabled)
        with mock.patch.object(self.manager, '_service_account_is_enabled', return_value=True):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.disable_service_account(self.test_account_id)

    def test_delete_service_account_timeout(self):
        """Test service account deletion timeout scenario."""
        # Mock the helper method to always return True (service account never gets deleted)
        with mock.patch.object(self.manager, '_service_account_exists', return_value=True):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.delete_service_account(self.test_account_id)

    def test_create_service_account_key_timeout(self):
        """Test service account key creation timeout scenario."""
        enabled_account = self._create_mock_service_account(self.test_account_id, disabled=False)
        mock_key = self._create_mock_service_account_key(self.test_account_id)
        
        self.mock_iam_client.get_service_account.return_value = enabled_account
        self.mock_iam_client.create_service_account_key.return_value = mock_key
        
        # Mock the helper method to always return False (key never gets created)
        with mock.patch.object(self.manager, '_service_account_key_exists', return_value=False):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.create_service_account_key(self.test_account_id)

    def test_delete_service_account_key_timeout(self):
        """Test service account key deletion timeout scenario."""
        key_id = "test-key-id"
        
        # Mock the helper method to always return True (key never gets deleted)
        with mock.patch.object(self.manager, '_service_account_key_exists', return_value=True):
            with self.assertRaises(exceptions.DeadlineExceeded):
                self.manager.delete_service_account_key(self.test_account_id, key_id)

# Run these real tests just if the environment variables are set correctly
# export GOOGLE_CLOUD_PROJECT = "your-project-id"

# Verify that the variables are set before running the tests
@unittest.skipUnless(
    'GOOGLE_CLOUD_PROJECT' in os.environ,
    "Skipping tests because environment variables are not set for Google Cloud project."
)
class TestServiceAccountManagerIntegration(unittest.TestCase):
    """Integration tests for ServiceAccountManager with real Google Cloud IAM client."""

    def setUp(self):
        """Set up test fixtures."""
        self.project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        self.logger = logging.getLogger(__name__)
        self.manager = ServiceAccountManager(self.project_id, self.logger, 5)

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up any service accounts created during tests
        try:
            accounts = self.manager._get_service_accounts()
            for account in accounts:
                if account.email.startswith("test-account-"):
                    try:
                        self.manager.delete_service_account(account.email)
                    except Exception as e:
                        self.logger.warning(f"Failed to delete service account {account.email}: {e}")
        except Exception as e:
            self.logger.warning(f"Failed to list service accounts during tearDown: {e}")

    def test_full_service_account_lifecycle(self):
        """Test creating and deleting a service account."""
        account_id = "test-account-" + str(os.getpid())
        display_name = "Test Account"

        # Create service account
        account = self.manager.create_service_account(account_id, display_name)
        service_account_email = account.email
        self.assertEqual(account.display_name, display_name)

        # Wait until service account is created (with retries)
        for i in range(5):
            if service_account_email in [a.email for a in self.manager._get_service_accounts()]:
                break
            time.sleep(i ** 2)  # Exponential backoff
        # Verify service account exists
        self.assertIn(service_account_email, [a.email for a in self.manager._get_service_accounts()])

        # Create a key for the service account
        key = self.manager.create_service_account_key(service_account_email)
        self.assertIsNotNone(key.private_key_data)

        # Test the key (now includes retry logic for propagation delays)
        key_valid = self.manager.test_service_account_key(key.private_key_data)
        self.assertTrue(key_valid)

        # List keys for the service account - with delayed check
        self.assertIn(key.name, [k.name for k in self.manager._get_service_account_keys(service_account_email)])

        # Delete the service account key
        self.manager.delete_service_account_key(service_account_email, key.name.split('/')[-1])

        # Create a new key to ensure we have multiple keys
        new_key = self.manager.create_service_account_key(service_account_email)
        new_key_valid = self.manager.test_service_account_key(new_key.private_key_data)
        self.assertTrue(new_key_valid)

        # Verify that we have 2 keys now
        all_keys = self.manager._get_service_account_keys(service_account_email)
        self.assertEqual(len(all_keys), 2)  # 1 old key + 1 new key

        # Disable the service account
        self.manager.disable_service_account(service_account_email)

        # Verify service account is disabled
        account = self.manager.get_service_account(service_account_email)
        self.assertTrue(account.disabled)

        # Enable the service account
        self.manager.enable_service_account(service_account_email)

        # Verify service account is enabled
        account = self.manager.get_service_account(service_account_email)
        self.assertFalse(account.disabled)

        # Test again the key after enabling the service account
        key_valid = self.manager.test_service_account_key(new_key.private_key_data)
        self.assertTrue(key_valid)

        # Delete the service account
        self.manager.delete_service_account(service_account_email)

        # Verify service account is deleted - using get_service_account with exception handling
        with self.assertRaises(exceptions.NotFound):
            self.manager.get_service_account(service_account_email)

if __name__ == '__main__':
    # Configure logging to reduce noise during testing
    import logging
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    
    # Run the tests
    unittest.main()
