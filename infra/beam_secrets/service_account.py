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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServiceAccountManager:
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = iam_admin_v1.IAMClient()

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
            logger.info(f"Created service account: {account.email}")
            return account
        except exceptions.Conflict:
            service_account_email = f"{account_id}@{self.project_id}.iam.gserviceaccount.com"
            get_request = types.GetServiceAccountRequest()
            get_request.name = f"projects/{self.project_id}/serviceAccounts/{service_account_email}"
            
            existing_account = self.client.get_service_account(request=get_request)
            logger.info(f"Service account already exists: {existing_account.email}")
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
            logger.info(f"Retrieved service account: {service_account.email}")
            return service_account
        except exceptions.NotFound:
            logger.error(f"Service account {account_id} not found")
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
            logger.info(f"Enabled service account: {account_id}")
        else:
            logger.warning(f"Failed to enable service account: {account_id}")
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
            logger.info(f"Disabled service account: {account_id}")
        else:
            logger.warning(f"Failed to disable service account: {account_id}")
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
        logger.info(f"Deleted service account: {account_id}")

    def list_service_accounts(self) -> List[iam_admin_v1.ServiceAccount]:
        """
        Lists all service accounts in the specified project.

        Returns:
            List[iam_admin_v1.ServiceAccount]: A list of service account objects.
        """
        request = types.ListServiceAccountsRequest()
        request.name = f"projects/{self.project_id}"

        accounts = self.client.list_service_accounts(request=request)
        logger.info(f"Listed service accounts: {[account.email for account in accounts.accounts]}")
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
                logger.info(f"Service account {account_id} is disabled. Enabling it first.")
                self.enable_service_account(account_id)
        except exceptions.NotFound:
            logger.error(f"Service account {account_id} not found")
            raise

        request = types.CreateServiceAccountKeyRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}"

        key = self.client.create_service_account_key(request=request)
        logger.info(f"Created service account key for {account_id}")
        return key
    
    def delete_service_account_key(self, account_id: str, key_id: str) -> None:
        """
        Deletes a key for the specified service account.

        Args:
            account_id (str): The unique identifier or email of the service account.
            key_id (str): The ID of the key to delete.
        """
        request = types.DeleteServiceAccountKeyRequest()
        request.name = f"projects/{self.project_id}/serviceAccounts/{account_id}/keys/{key_id}"

        self.client.delete_service_account_key(request=request)
        logger.info(f"Deleted service account key: {key_id} for account: {account_id}")
        return
    
    def delete_oldest_service_account_key(self, account_id: str) -> Optional[types.ServiceAccountKey]:
        """
        Deletes the oldest key for the specified service account.
        If no keys exist, returns None.

        Args:
            account_id (str): The unique identifier or email of the service account.
        
        Returns:
            Optional[types.ServiceAccountKey]: The deleted service account key object, or None if no keys exist.
        """
        keys = self.list_service_account_keys(account_id)
        if not keys:
            logger.info(f"No keys found for service account: {account_id}")
            return None
        
        # Sort keys by creation time (oldest first)
        keys.sort(key=lambda k: k.valid_after_time)
        oldest_key = keys[0]

        self.delete_service_account_key(account_id, oldest_key.name.split('/')[-1])
        logger.info(f"Deleted oldest key for service account: {account_id}")
        return oldest_key
    
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
        logger.info(f"Listed keys for service account: {account_id}")
        return list(response.keys)
    
    def test_service_account_key(self, key_data: bytes) -> bool:
        """
        Tests if a service account key is valid by attempting to authenticate and make an API call.

        Args:
            key_data (bytes): The private key data from the service account key.
        
        Returns:
            bool: True if the key is valid and can authenticate, False otherwise.
        """
        try:
            key_info = json.loads(key_data.decode('utf-8'))
            
            credentials = service_account.Credentials.from_service_account_info(
                key_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            
            request = Request()
            credentials.refresh(request)
            
            logger.info(f"Service account key is valid and can authenticate")
            return True
                
        except json.JSONDecodeError as json_error:
            logger.error(f"Invalid JSON in service account key: {json_error}")
            return False
        except Exception as auth_error:
            logger.error(f"Authentication failed with service account key: {auth_error}")
            return False
    