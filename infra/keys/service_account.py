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

class ServiceAccountManager:
    def __init__(self, project_id: str, logger: logging.Logger) -> None:
        self.project_id = project_id
        self.client = iam_admin_v1.IAMClient()
        self.logger = logger
        self.logger.info(f"Initialized ServiceAccountManager for project: {self.project_id}")

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
            self.logger.info(f"Created service account: {account.email}")
            return account
        except exceptions.Conflict:
            service_account_email = f"{account_id}@{self.project_id}.iam.gserviceaccount.com"
            get_request = types.GetServiceAccountRequest()
            get_request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"
            
            existing_account = self.client.get_service_account(request=get_request)
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
        request = types.GetServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"

        try:
            service_account = self.client.get_service_account(request=request)
            self.logger.info(f"Retrieved service account: {service_account.email}")
            return service_account
        except exceptions.NotFound:
            self.logger.error(f"Service account {account_id} not found")
            raise
    
    def enable_service_account(self, account_id: str) -> types.ServiceAccount:
        """
        Enables a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to enable.
        
        Returns:
            types.ServiceAccount: The updated service account object.
        """
        request = types.EnableServiceAccountRequest()
        name = f"projects/{self.project_id}/serviceAccounts/{account_id}"
        request.name = name

        self.client.enable_service_account(request=request)
        time.sleep(5)

        service_account = self.get_service_account(account_id)
        if not service_account.disabled:
            self.logger.info(f"Enabled service account: {account_id}")
        else:
            self.logger.warning(f"Failed to enable service account: {account_id}")
        return service_account

    def disable_service_account(self, account_id: str) -> types.ServiceAccount:
        """
        Disables a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to disable.
        
        Returns:
            types.ServiceAccount: The updated service account object.
        """
        request = types.DisableServiceAccountRequest()
        name = f"projects/{self.project_id}/serviceAccounts/{account_id}"
        request.name = name

        self.client.disable_service_account(request=request)
        time.sleep(5)

        get_request = types.GetServiceAccountRequest()
        get_request.name = name

        service_account = self.client.get_service_account(request=get_request)
        if service_account.disabled:
            self.logger.info(f"Disabled service account: {account_id}")
        else:
            self.logger.warning(f"Failed to disable service account: {account_id}")
        return service_account

    def delete_service_account(self, account_id: str) -> None:
        """
        Deletes a service account in the specified project.

        Args:
            account_id (str): The unique identifier or email of the service account to delete.
        """
        request = types.DeleteServiceAccountRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"

        self.client.delete_service_account(request=request)
        self.logger.info(f"Deleted service account: {account_id}")

    def list_service_accounts(self) -> List[iam_admin_v1.ServiceAccount]:
        """
        Lists all service accounts in the specified project.

        Returns:
            List[iam_admin_v1.ServiceAccount]: A list of service account objects.
        """
        request = types.ListServiceAccountsRequest()
        request.name = f"projects/{self.project_id}"

        accounts = self.client.list_service_accounts(request=request)
        self.logger.info(f"Listed service accounts: {[account.email for account in accounts.accounts]}")
        return list(accounts.accounts)

    def create_service_account_key(self, account_id: str) -> types.ServiceAccountKey:
        """
        Creates a key for the specified service account.
        Remember the private key ID is only returned once.
        If the service account is disabled, it will be enabled first.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            types.ServiceAccountKey: The created service account key object.
            str: The private key ID of the created key.
        """
        get_request = types.GetServiceAccountRequest()
        get_request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"
        
        try:
            service_account = self.client.get_service_account(request=get_request)
            if service_account.disabled:
                self.logger.info(f"Service account {account_id} is disabled. Enabling it first.")
                self.enable_service_account(account_id)
        except exceptions.NotFound:
            self.logger.error(f"Service account {account_id} not found")
            raise

        request = types.CreateServiceAccountKeyRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"

        key = self.client.create_service_account_key(request=request)
        self.logger.info(f"Created service account key for {account_id}")
        return key
    
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
        request = types.DeleteServiceAccountKeyRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}/keys/{key_id}"

        try:
            self.client.delete_service_account_key(request=request)
            self.logger.info(f"Deleted service account key: {key_id} for account: {account_id}")
        except exceptions.NotFound:
            self.logger.warning(f"Service account key {key_id} not found for account: {account_id} (may have been already deleted)")
            raise
        except exceptions.FailedPrecondition as e:
            self.logger.warning(f"Failed to delete service account key {key_id} for account: {account_id}. Error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error deleting service account key {key_id} for account: {account_id}. Error: {e}")
            raise

    def delete_oldest_service_account_keys(self, account_id: str, max_keys: int = 2) -> List[types.ServiceAccountKey]:
        """
        Deletes the oldest key for the specified service account.
        If no keys exist, returns None.

        Args:
            account_id (str): The unique identifier or email of the service account.
            max_keys (int): The maximum number of keys to keep. Defaults to 2.
        
        Returns:
            List[types.ServiceAccountKey]: A list of service account keys that were deleted.
        """
        keys = self.list_service_account_keys(account_id)
        if not keys:
            self.logger.info(f"No keys found for service account: {account_id}")
            return []
        
        # Sort keys by creation time (oldest first)
        keys.sort(key=lambda k: k.valid_after_time)

        # If the number of keys is less than or equal to max_keys, do not delete any keys
        if len(keys) <= max_keys:
            self.logger.info(f"Service account {account_id} has {len(keys)} keys, not deleting any.")
            return []
        
        deleted_keys = []
        failed_deletions = []
        while len(keys) > max_keys:
            oldest_key = keys.pop(0)
            try:
                self.delete_service_account_key(account_id, oldest_key.name.split('/')[-1])
                deleted_keys.append(oldest_key)
                self.logger.info(f"Deleted oldest service account key: {oldest_key.name} for account: {account_id}")
            except (exceptions.NotFound, exceptions.FailedPrecondition) as e:
                self.logger.warning(f"Could not delete key {oldest_key.name}: {e}. Continuing with other keys.")
                failed_deletions.append(oldest_key)
            except Exception as e:
                self.logger.error(f"Unexpected error deleting key {oldest_key.name}: {e}. Stopping deletion process.")
                break

        if failed_deletions:
            self.logger.info(f"Failed to delete {len(failed_deletions)} keys for service account: {account_id}")

        return deleted_keys

    def list_service_account_keys(self, account_id: str) -> List[iam_admin_v1.ServiceAccountKey]:
        """
        Lists all keys for the specified service account.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            List[iam_admin_v1.ServiceAccountKey]: A list of service account key objects.
        """
        request = types.ListServiceAccountKeysRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"

        response = self.client.list_service_account_keys(request=request)
        self.logger.info(f"Listed keys for service account: {account_id}")
        return list(response.keys)
    
    def test_service_account_key(self, key_data: bytes, max_retries: int = 5, initial_delay: float = 1.0) -> bool:
        """
        Tests if a service account key is valid by attempting to authenticate and make an API call.
        Includes retry logic to handle key propagation delays.

        Args:
            key_data (bytes): The private key data from the service account key.
            max_retries (int): Maximum number of retry attempts (default: 5).
            initial_delay (float): Initial delay between retries in seconds (default: 1.0).
        
        Returns:
            bool: True if the key is valid and can authenticate, False otherwise.
        """
        try:
            key_info = json.loads(key_data.decode('utf-8'))
        except json.JSONDecodeError as json_error:
            self.logger.error(f"Invalid JSON in service account key: {json_error}")
            return False
        
        for attempt in range(max_retries):
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
                if attempt < max_retries - 1:  # Don't log on the last attempt
                    delay = initial_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.warning(f"Authentication attempt {attempt + 1} failed (will retry in {delay}s): {auth_error}")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Authentication failed with service account key after {max_retries} attempts: {auth_error}")
                    return False
        
        return False
    