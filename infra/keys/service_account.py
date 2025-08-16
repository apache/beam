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

import logging
import json
import time
from typing import List,Optional
from google.cloud import iam_admin_v1
from google.cloud.iam_admin_v1 import types
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.api_core import exceptions

class ServiceAccountManagerLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds a prefix to all log messages."""
    
    def process(self, msg, kwargs):
        return f"[ServiceAccountManager] {msg}", kwargs

class ServiceAccountManager:
    def __init__(self, project_id: str, logger: logging.Logger, max_retries: int = 3) -> None:
        self.project_id = project_id
        self.client = iam_admin_v1.IAMClient()
        self.logger = ServiceAccountManagerLoggerAdapter(logger, {})
        self.max_retries = max_retries
        self.logger.info(f"Initialized ServiceAccountManager for project: {self.project_id}")

    def _normalize_account_email(self, account_id: str) -> str:
        """
        Normalizes the account identifier to a full email format.
        
        Args:
            account_id (str): The unique identifier or email of the service account.
            
        Returns:
            str: The full service account email address.
        """
        # Handle both account ID and full email formats
        if "@" in account_id and account_id.endswith(".iam.gserviceaccount.com"):
            # account_id is already a full email
            return account_id
        else:
            # account_id is just the account name
            return f"{account_id}@{self.project_id}.iam.gserviceaccount.com"

    def _get_service_accounts(self) -> List[iam_admin_v1.ServiceAccount]:
        """
        Retrieves all service accounts in the specified project.

        Returns:
            List[iam_admin_v1.ServiceAccount]: A list of service account objects.
        """
        request = types.ListServiceAccountsRequest()
        request.name = f"projects/{self.project_id}"

        accounts = self.client.list_service_accounts(request=request)
        self.logger.debug(f"Listed service accounts: {[account.email for account in accounts.accounts]}")
        return list(accounts.accounts)
    
    def _service_account_exists(self, account_id: str) -> bool:
        """
        Checks if a service account with the given account_id exists in the project.

        Args:
            account_id (str): The unique identifier or email of the service account.

        Returns:
            bool: True if the service account exists, False otherwise.
        """
        try:
            self.get_service_account(account_id)
            return True
        except exceptions.NotFound:
            return False
        
    def _service_account_is_enabled(self, account_id: str) -> bool:
        """
        Checks if a service account is enabled.

        Args:
            account_id (str): The unique identifier or email of the service account.

        Returns:
            bool: True if the service account is enabled, False otherwise.
        """
        try:
            service_account = self.get_service_account(account_id)
            return not service_account.disabled
        except exceptions.NotFound:
            self.logger.error(f"Service account {account_id} not found")
            return False

    def create_service_account(self, account_id: str, display_name: Optional[str] = None) -> types.ServiceAccount:
        """
        Creates a service account in the specified project.
        If the service account already exists, returns the existing account (idempotent operation).

        Args:
            account_id (str): The unique identifier for the service account.
            display_name (Optional[str]): A human-readable name for the service account.
        Returns:
            types.ServiceAccount: The created or existing service account object.
        """
        request = types.CreateServiceAccountRequest()
        request.account_id = account_id
        request.name = f"projects/{self.project_id}"

        service_account = types.ServiceAccount()
        service_account.display_name = display_name or account_id
        request.service_account = service_account

        try:
            account = self.client.create_service_account(request=request)

            # Wait for the service account to be created
            delay = 1
            for _ in range(self.max_retries):
                if self._service_account_exists(account_id):
                    break
                time.sleep(delay)
                delay *= 2
            else:
                self.logger.error(f"Service account {account_id} creation timed out after {self.max_retries} retries.")
                raise exceptions.DeadlineExceeded(f"Service account {account_id} creation timed out.")

            self.logger.info(f"Created service account: {account.email}")
            return account
        except exceptions.Conflict:
            existing_account = self.get_service_account(account_id)
            self.logger.info(f"Service account already exists: {existing_account.email}")
            return existing_account
    
    def get_service_account(self, account_id: str) -> types.ServiceAccount:
        """
        Retrieves a service account by its unique identifier or email.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            types.ServiceAccount: The service account object.
        """
        service_account_email = self._normalize_account_email(account_id)

        request = types.GetServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"

        try:
            service_account = self.client.get_service_account(request=request)
            self.logger.info(f"Retrieved service account: {service_account.email}")
            return service_account
        except exceptions.NotFound:
            self.logger.error(f"Service account {account_id} not found")
            raise
    
    def enable_service_account(self, account_id: str) -> None:
        """
        Enables a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to enable.
        """
        service_account_email = self._normalize_account_email(account_id)
        request = types.EnableServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"

        self.client.enable_service_account(request=request)

        # Wait for the service account to be enabled
        delay = 1
        for _ in range(self.max_retries):
            if self._service_account_is_enabled(account_id):
                break
            time.sleep(delay)
            delay *= 2
        else:
            self.logger.error(f"Service account {account_id} enabling timed out after {self.max_retries} retries.")
            raise exceptions.DeadlineExceeded(f"Service account {account_id} enabling timed out.")
        
        self.logger.info(f"Enabled service account: {account_id}")

    def disable_service_account(self, account_id: str) -> None:
        """
        Disables a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to disable.
        """
        service_account_email = self._normalize_account_email(account_id)
        request = types.DisableServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"

        self.client.disable_service_account(request=request)

        # Wait for the service account to be disabled
        delay = 1
        for _ in range(self.max_retries):
            if not self._service_account_is_enabled(account_id):
                break
            time.sleep(delay)
            delay *= 2
        else:
            self.logger.error(f"Service account {account_id} disabling timed out after {self.max_retries} retries.")
            raise exceptions.DeadlineExceeded(f"Service account {account_id} disabling timed out.")
        
        self.logger.info(f"Disabled service account: {account_id}")

    def delete_service_account(self, account_id: str) -> None:
        """
        Deletes a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to delete.
        """
        service_account_email = self._normalize_account_email(account_id)
        request = types.DeleteServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"

        self.client.delete_service_account(request=request)

        # Wait for the service account to be deleted
        delay = 1
        for _ in range(self.max_retries):
            if not self._service_account_exists(account_id):
                break
            time.sleep(delay)
            delay *= 2
        else:
            self.logger.error(f"Service account {account_id} deletion timed out after {self.max_retries} retries.")
            raise exceptions.DeadlineExceeded(f"Service account {account_id} deletion timed out.")

        self.logger.info(f"Deleted service account: {account_id}")

    def _get_service_account_keys(self, account_id: str) -> List[iam_admin_v1.ServiceAccountKey]:
        """
        Retrieves all keys for the specified service account.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            List[iam_admin_v1.ServiceAccountKey]: A list of service account key objects.
        """
        service_account_email = self._normalize_account_email(account_id)
        request = types.ListServiceAccountKeysRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"

        response = self.client.list_service_account_keys(request=request)
        self.logger.debug(f"Listed keys for service account: {account_id}")
        return list(response.keys)

    def _service_account_key_exists(self, account_id: str, key_id: str) -> bool:
        """
        Checks if a service account key exists for the specified service account.

        Args:
            account_id (str): The unique identifier or email of the service account.
            key_id (str): The ID of the service account key to check.
        
        Returns:
            bool: True if the key exists, False otherwise.
        """
        keys = self._get_service_account_keys(account_id)
        return any(key.name.split('/')[-1] == key_id for key in keys)
    
    def create_service_account_key(self, account_id: str) -> types.ServiceAccountKey:
        """
        Creates a key for the specified service account.
        Remember the private key ID is only returned once.
        If the service account is disabled, it will be enabled first.
        Includes retry logic to handle service account propagation delays.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            types.ServiceAccountKey: The created service account key object.
            str: The private key ID of the created key.
        """
        service_account_email = self._normalize_account_email(account_id)
        
        # Retry logic for service account access and key creation
        delay = 1
        for attempt in range(self.max_retries):
            try:
                # Check if service account exists and get its state
                get_request = types.GetServiceAccountRequest()
                get_request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"
                
                service_account = self.client.get_service_account(request=get_request)
                if service_account.disabled:
                    self.logger.info(f"Service account {account_id} is disabled. Enabling it first.")
                    self.enable_service_account(account_id)
                
                # Create the key
                request = types.CreateServiceAccountKeyRequest()
                request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"
                
                key = self.client.create_service_account_key(request=request)
                
                # Wait for the key to be created and available
                key_delay = 1
                for _ in range(self.max_retries):
                    if self._service_account_key_exists(account_id, key.name.split('/')[-1]):
                        break
                    time.sleep(key_delay)
                    key_delay *= 2
                else:
                    self.logger.error(f"Service account key creation for {account_id} timed out after {self.max_retries} retries.")
                    raise exceptions.DeadlineExceeded(f"Service account key creation for {account_id} timed out.")

                self.logger.info(f"Created service account key for {account_id}")
                return key
                
            except exceptions.NotFound as e:
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"Service account {account_id} not found (attempt {attempt + 1}/{self.max_retries}), retrying in {delay}s. This may be due to propagation delay.")
                    time.sleep(delay)
                    delay *= 2
                else:
                    self.logger.error(f"Service account {account_id} not found after {self.max_retries} attempts")
                    raise
            except Exception as e:
                # For other exceptions, don't retry
                self.logger.error(f"Error creating service account key for {account_id}: {e}")
                raise
        
        # This should not be reached due to the raise in the except block
        raise exceptions.NotFound(f"Service account {account_id} not found after {self.max_retries} attempts")
    
    def delete_service_account_key(self, account_id: str, key_id: str) -> None:
        """
        Deletes a key for the specified service account.

        Args:
            account_id (str): The unique identifier or email of the service account.
            key_id (str): The ID of the key to delete.
        
        Raises:
            exceptions.NotFound: If the key does not exist.
            exceptions.FailedPrecondition: If the key cannot be deleted due to constraints.
        """
        service_account_email = self._normalize_account_email(account_id)
        request = types.DeleteServiceAccountKeyRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}/keys/{key_id}"

        try:
            self.client.delete_service_account_key(request=request)
        except exceptions.NotFound:
            self.logger.warning(f"Service account key {key_id} not found for account: {account_id} (may have been already deleted)")
            raise
        except exceptions.FailedPrecondition as e:
            self.logger.warning(f"Failed to delete service account key {key_id} for account: {account_id}. Error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error deleting service account key {key_id} for account: {account_id}. Error: {e}")
            raise

        # Wait for the key to be deleted
        delay = 1
        for _ in range(self.max_retries):
            if not self._service_account_key_exists(account_id, key_id):
                break
            time.sleep(delay)
            delay *= 2
        else:
            self.logger.error(f"Service account key deletion for {account_id} timed out after {self.max_retries} retries.")
            raise exceptions.DeadlineExceeded(f"Service account key deletion for {account_id} timed out.")

        self.logger.info(f"Deleted service account key: {key_id} for account: {account_id}")

    def test_service_account_key(self, key_data: bytes) -> bool:
        """
        Tests if a service account key is valid by attempting to authenticate and make an API call.
        Includes retry logic to handle key propagation delays.

        Args:
            key_data (bytes): The private key data from the service account key.
        
        Returns:
            bool: True if the key is valid and can authenticate, False otherwise.
        """
        try:
            key_info = json.loads(key_data.decode('utf-8'))
        except json.JSONDecodeError as json_error:
            self.logger.error(f"Invalid JSON in service account key: {json_error}")
            return False

        delay = 1
        for attempt in range(self.max_retries):
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    key_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                
                request = Request()
                credentials.refresh(request)

                self.logger.info(f"Service account key is valid and can authenticate")
                return True
                    
            except Exception as auth_error:
                if attempt < self.max_retries - 1:  # Don't log on the last attempt
                    delay *= 2
                    self.logger.warning(f"Authentication attempt {attempt + 1} failed (will retry in {delay}s): {auth_error}")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Authentication failed with service account key after {self.max_retries} attempts: {auth_error}")
                    return False
        
        return False
    