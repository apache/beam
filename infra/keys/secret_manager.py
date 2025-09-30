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

import google_crc32c
import logging
import time
from datetime import datetime, timezone, timedelta
from google.cloud import secretmanager
from typing import List, Union, Tuple, Dict

# What the "created_by" label is set to for secrets created by this service.
SECRET_MANAGER_LABEL = "beam-infra-secret-manager"

class SecretManagerLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds a prefix to all log messages."""
    
    def process(self, msg, kwargs):
        return f"[SecretManager] {msg}", kwargs

class SecretManager:
    """Service to manage GCP API keys rotation."""

    project_id: str # The GCP project ID where secrets are managed
    rotation_interval: int # The interval (in days) at which to rotate secrets
    grace_period: int # The grace period (in days) before a secret is considered for rotation
    max_retries: int # The maximum number of retries for API calls
    client: secretmanager.SecretManagerServiceClient # GCP Secret Manager client
    logger: Union[logging.Logger, logging.LoggerAdapter] # Logger for logging messages

    def __init__(self, project_id: str, logger: logging.Logger, rotation_interval: int = 30, grace_period: int = 7, max_retries: int = 3) -> None:
        self.project_id = project_id
        self.rotation_interval = rotation_interval
        self.grace_period = grace_period
        self.max_retries = max_retries
        self.client = secretmanager.SecretManagerServiceClient()
        self.logger = SecretManagerLoggerAdapter(logger, {})
        self.logger.info(f"Initialized SecretManager for project '{self.project_id}'")

    def _get_secret_ids(self) -> List[str]:
        """
        Retrieves the list of secrets from the Secret Manager and populates the `secrets_ids` list.
        This method filters secrets based on a specific label indicating they were created by this service.

        Returns:
            List[str]: A list of secret IDs that were created by this service.
        """
        self.logger.debug(f"Retrieving secrets with the label from project '{self.project_id}'")
        secret_ids = []

        try:
            for secret in self.client.list_secrets(request={"parent": f"projects/{self.project_id}"}):
                secret_id = secret.name.split("/")[-1]
                if "created_by" in secret.labels and secret.labels["created_by"] == SECRET_MANAGER_LABEL:
                    secret_ids.append(secret_id)
        except Exception as e:
            self.logger.error(f"Error retrieving secrets: {e}")

        self.logger.debug(f"Found {len(secret_ids)} secrets created by {SECRET_MANAGER_LABEL} in project '{self.project_id}'")
        return secret_ids

    def _secret_exists(self, secret_id: str) -> bool:
        """
        Checks if a secret with the given ID exists.

        Args:
            secret_id (str): The ID of the secret to check.
        Returns:
            bool: True if the secret exists, False otherwise.
        """
        self.logger.debug(f"Checking if secret '{secret_id}' exists")
        try:
            name = self.client.secret_path(self.project_id, secret_id)
            self.client.get_secret(request={"name": name})
            self.logger.debug(f"Secret '{secret_id}' exists")
            return True
        except Exception as e:
            self.logger.debug(f"Secret '{secret_id}' does not exist: {e}")
            return False

    def _secret_is_managed(self, secret_id: str) -> bool:
        """
        Checks if a secret with the given ID exists and is managed by this service.

        Args:
            secret_id (str): The ID of the secret to check.
        Returns:
            bool: True if the secret is managed by this service, False otherwise.
        """
        self.logger.debug(f"Checking if secret '{secret_id}' exists and is managed by {SECRET_MANAGER_LABEL}")
        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, cannot be managed")
            return False

        name = self.client.secret_path(self.project_id, secret_id)
        secret = self.client.get_secret(request={"name": name})

        is_managed = "created_by" in secret.labels and secret.labels["created_by"] == SECRET_MANAGER_LABEL
        self.logger.debug(f"Secret '{secret_id}' is managed by {SECRET_MANAGER_LABEL}: {is_managed}")
        return is_managed

    def create_secret(self, secret_id: str) -> str:
        """
        Create a new secret with the given name. A secret is a logical wrapper
        around a collection of secret versions. Secret versions hold the actual
        secret material. This method creates a new secret with automatic replication
        and labels for tracking.

        Args:
            secret_id (str): The ID to assign to the new secret. This ID must be unique within the project.
        Returns:
            str: The secret path of the newly created secret.
        """
        if self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' already exists, returning existing secret path")
            name = self.client.secret_path(self.project_id, secret_id)
            return name

        self.logger.info(f"Creating new secret '{secret_id}' with rotation interval of {self.rotation_interval} days")
        response = self.client.create_secret(
            request={
                "parent": f"projects/{self.project_id}",
                "secret_id": f"{secret_id}",
                "secret": {
                    "replication": {
                        "automatic": {}
                    },
                    "labels": {
                        "created_by": SECRET_MANAGER_LABEL,
                        "created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                        "rotation_interval_days": str(self.rotation_interval),
                        "grace_period_days": str(self.grace_period),
                        "last_version_created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                    }
                }
            }
        )

        # created_by : This label is used to identify secrets created by this service.
        # created_at : This label stores the timestamp when the secret was created.
        # rotation_interval_days : This label specifies the rotation interval for the secret.
        # grace_period_days : This label specifies the grace period for the secret.
        # last_version_created_at : This label stores the timestamp when the last version of the secret was created, this
        #   helps with the rotation and grace period calculations.

        # Wait for the secret to be created
        self.logger.debug(f"Waiting for secret '{secret_id}' to be created")
        delay = 1
        for _ in range(self.max_retries):
            if self._secret_is_managed(secret_id):
                self.logger.debug(f"Secret '{secret_id}' is now available")
                break
            self.logger.debug(f"Secret '{secret_id}' not found, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify creation of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully created secret '{secret_id}'")
        return response.name

    def get_secret(self, secret_id: str) -> secretmanager.Secret:
        """
        Retrieves the specified secret by its ID.

        Args:
            secret_id (str): The ID of the secret to retrieve.
        Returns:
            secretmanager.Secret: The requested secret.
        """
        self.logger.info(f"Retrieving secret '{secret_id}'")

        if not self._secret_exists(secret_id):
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} is not managed by this service."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        name = self.client.secret_path(self.project_id, secret_id)
        return self.client.get_secret(request={"name": name})

    def delete_secret(self, secret_id: str) -> None:
        """
        Deletes the specified secret and all its versions.

        Args:
            secret_id (str): The ID of the secret to delete.
        """
        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, cannot delete")
            return

        self.logger.info(f"Deleting secret '{secret_id}' and all its versions")
        name = self.client.secret_path(self.project_id, secret_id)
        self.client.delete_secret(request={"name": name})
        
        # Wait for the secret to be deleted
        self.logger.debug(f"Waiting for secret '{secret_id}' to be deleted")
        delay = 1 
        for _ in range(self.max_retries):
            if not self._secret_exists(secret_id):
                self.logger.debug(f"Secret '{secret_id}' is now deleted")
                break
            self.logger.debug(f"Secret '{secret_id}' still exists, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify deletion of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully deleted secret '{secret_id}'")

    def is_different_user_access(self, secret_id: str, allowed_users: List[str]) -> bool:
        """
        Checks if the current access policy of a secret allows only the specified users to read it.
        This is used to determine if an update is needed.

        Args:
            secret_id (str): The ID of the secret to check access for.
            allowed_users (List[str]): A list of user emails to check against the current access policy.
        Returns:
            bool: True if the current access policy is different from the specified users, False otherwise.
        """
        self.logger.debug(f"Checking if access for secret '{secret_id}' differs from allowed users: {allowed_users}")

        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, cannot check access")
            return True
        
        accessor_role = "roles/secretmanager.secretAccessor"
        resource_name = self.client.secret_path(self.project_id, secret_id)
        
        try:
            policy = self.client.get_iam_policy(request={"resource": resource_name})
        except Exception as e:
            self.logger.error(f"Failed to get IAM policy for secret '{secret_id}': {e}")
            return True

        current_members = set()
        for binding in policy.bindings:
            if binding.role == accessor_role:
                current_members.update(binding.members)

        allowed_members = {f"user:{user_email}" for user_email in allowed_users}
        
        is_different = current_members != allowed_members
        self.logger.debug(f"Current members: {current_members}")
        self.logger.debug(f"Allowed members: {allowed_members}")
        self.logger.debug(f"Access for secret '{secret_id}' differs: {is_different}")
        return is_different

    def update_secret_access(self, secret_id: str, allowed_users: List[str]) -> None:
        """
        Updates the access policy of a secret to allow only the specified users to read it.
        Any existing users will be removed and replaced with the new list.

        Args:
            secret_id (str): The ID of the secret to update access for.
            allowed_users (List[str]): A list of user emails to grant read access to.
        """
        self.logger.debug(f"Updating access for secret '{secret_id}' to allow users: {allowed_users}")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} is not managed by this service, cannot update access."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        accessor_role = "roles/secretmanager.secretAccessor"
        resource_name = self.client.secret_path(self.project_id, secret_id)
        policy = self.client.get_iam_policy(request={"resource": resource_name})

        members = [f"user:{user_email}" for user_email in allowed_users]

        binding_found = False
        for binding in policy.bindings:
            if binding.role == accessor_role:
                binding.members[:] = members
                self.logger.debug(f"Replaced members for role '{accessor_role}' in secret '{secret_id}' with: {allowed_users}")
                binding_found = True
                break
        
        if not binding_found:
            policy.bindings.add(
                role=accessor_role,
                members=members
            )
            self.logger.debug(f"Created new binding for role '{accessor_role}' in secret '{secret_id}'")

        self.client.set_iam_policy(
            request={
                "resource": resource_name,
                "policy": policy
            }
        )

        self.logger.info(f"Successfully updated access for secret '{secret_id}' to allow users: {allowed_users}")

    def _get_secret_versions(self, secret_id: str) -> List[secretmanager.SecretVersion]:
        """
       Retrieves all versions of a secret.

        Args:
            secret_id (str): The ID of the secret to list versions for.
        Returns:
            List[secretmanager.SecretVersion]: A list of secret versions.
        """
        self.logger.debug(f"Retrieving versions for secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, cannot retrieve versions")
            return []

        parent = self.client.secret_path(self.project_id, secret_id)
        versions = list(self.client.list_secret_versions(request={"parent": parent}))
        self.logger.debug(f"Found {len(versions)} versions for secret '{secret_id}'")
        return versions

    def _secret_version_exists(self, secret_id: str, version_id: str) -> bool:
        """
        Checks if a specific version of a secret exists.

        Args:
            secret_id (str): The ID of the secret to check.
            version_id (str): The ID of the version to check.
        Returns:
            bool: True if the version exists, False otherwise.
        """
        self.logger.debug(f"Checking if version '{version_id}' exists for secret '{secret_id}'")
        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, cannot check version existence")
            return False
        
        versions = self._get_secret_versions(secret_id)
        exists = any(version.name.split("/")[-1] == version_id for version in versions)
        self.logger.debug(f"Version '{version_id}' exists: {exists}")
        return exists
    
    def _secret_version_is_enabled(self, secret_id: str, version_id: str) -> bool:
        """
        Checks if a specific version of a secret is enabled.

        Args:
            secret_id (str): The ID of the secret to check.
            version_id (str): The ID of the version to check.
        Returns:
            bool: True if the version is enabled, False otherwise.
        """
        self.logger.debug(f"Checking if version '{version_id}' of secret '{secret_id}' is enabled")
        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, version cannot be enabled")
            return False
        
        versions = self._get_secret_versions(secret_id)
        for version in versions:
            if version.name.split("/")[-1] == version_id:
                is_enabled = version.state == secretmanager.SecretVersion.State.ENABLED
                self.logger.debug(f"Version '{version_id}' is enabled: {is_enabled}")
                return is_enabled
        self.logger.debug(f"Version '{version_id}' does not exist for secret '{secret_id}'")
        return False
    
    def _secret_version_is_destroyed(self, secret_id: str, version_id: str) -> bool:
        """
        Checks if a specific version of a secret is destroyed.
        
        Args:
            secret_id (str): The ID of the secret to check.
            version_id (str): The ID of the version to check.
        Returns:
            bool: True if the version is destroyed, False otherwise.
        """
        self.logger.debug(f"Checking if version '{version_id}' of secret '{secret_id}' is destroyed")
        if not self._secret_is_managed(secret_id):
            self.logger.debug(f"Secret '{secret_id}' is not managed by this service, version cannot be destroyed")
            return False
        
        versions = self._get_secret_versions(secret_id)
        for version in versions:
            if version.name.split("/")[-1] == version_id:
                is_destroyed = version.state == secretmanager.SecretVersion.State.DESTROYED
                self.logger.debug(f"Version '{version_id}' is destroyed: {is_destroyed}")
                return is_destroyed
        self.logger.debug(f"Version '{version_id}' does not exist for secret '{secret_id}'")
        return False

    def _get_latest_secret_version_id(self, secret_id: str) -> str:
        """
        Retrieves the latest enabled version of a secret.

        Args:
            secret_id (str): The ID of the secret to retrieve the latest version for.
        Returns:
            str: The name of the latest secret version.
        """
        self.logger.debug(f"Retrieving latest enabled version of secret '{secret_id}'")
        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot retrieve latest version."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        for version in self._get_secret_versions(secret_id):
            if version.state == secretmanager.SecretVersion.State.ENABLED:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Found latest enabled version '{version_id}' for secret '{secret_id}'")
                return version_id
        error_msg = f"No enabled versions found for secret {secret_id}."
        self.logger.error(error_msg)
        raise ValueError(error_msg)
    
    def _is_key_rotation_due(self, secret_id: str) -> bool:
        """
        Checks if the key rotation is due based on the last version created timestamp.

        Args:
            secret_id (str): The ID of the secret to check.
        Returns:
            bool: True if the key rotation is due, False otherwise.
        """
        self.logger.debug(f"Checking if key rotation is due for secret '{secret_id}'")
        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot check rotation."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        secret = self.get_secret(secret_id)
        last_version_created_at = secret.labels["last_version_created_at"]
        last_version_date = datetime.strptime(last_version_created_at, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        due_date = last_version_date + timedelta(days=self.rotation_interval)
        
        is_due = datetime.now(timezone.utc) >= due_date
        self.logger.debug(f"Key rotation due for secret '{secret_id}': {is_due}")
        return is_due
    
    def add_secret_version(self, secret_id: str, data_id: str, payload: Union[bytes, str]) -> str:
        """
        Adds a new version to the specified secret with the given data ID and payload.
        If the secret does not exist, it will be created first. All previous versions will be disabled.

        Args:
            secret_id (str): The ID of the secret to which the version will be added.
            data_id (str): The ID of the data to be stored in the new version.
            payload (bytes): The secret data to be stored in the new version.
        Returns:
            str: The name of the newly created secret version.
        """
        self.logger.info(f"Adding new version to secret '{secret_id}'")

        secret_path = self.create_secret(secret_id)

        if not isinstance(payload, (bytes, str)):
            error_msg = "Payload must be a bytes object or a string that can be encoded to bytes."
            self.logger.error(error_msg)
            raise TypeError(error_msg)
        
        # Join data_id and payload to form the payload
        if isinstance(payload, str):
            payload = f"{data_id}:{payload}"
        else:
            payload = f"{data_id}:{payload.decode('utf-8')}" if isinstance(payload, bytes) else payload

        # Ensure payload is bytes
        payload_bytes = payload.encode('utf-8') if isinstance(payload, str) else payload

        crc32c = google_crc32c.Checksum()
        crc32c.update(payload_bytes)

        self.logger.debug(f"Creating secret version with CRC32C checksum")
        response = self.client.add_secret_version(
            request={
                "parent": secret_path,
                "payload": {
                    "data": payload_bytes,
                    "data_crc32c": int(crc32c.hexdigest(), 16),
                }
            }
        )

        version_id = response.name.split("/")[-1]

        # Update the last version created timestamp
        self.logger.debug(f"Updating last version created timestamp for secret '{secret_id}'")
        secret_obj = self.get_secret(secret_id)
        labels = dict(secret_obj.labels)
        labels["last_version_created_at"] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        secret = {"name": secret_obj.name, "labels": labels}
        update_mask = {"paths": ["labels"]}
        self.client.update_secret(request={"secret": secret, "update_mask": update_mask})

        # Wait for the new version to be available
        self.logger.debug(f"Waiting for new version '{version_id}' of secret '{secret_id}' to be available")
        delay = 1
        for _ in range(self.max_retries):
            if self._secret_version_exists(secret_id, version_id):
                self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' is now available")
                break
            self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' not found, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify creation of secret version '{version_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        # Disable all the previous versions except the newly created one
        for ver in self._get_secret_versions(secret_id):
            if ver.name != response.name and ver.state == secretmanager.SecretVersion.State.ENABLED:
                self.logger.debug(f"Disabling previous version '{ver.name}' of secret '{secret_id}'")
                self.disable_secret_version(secret_id, ver.name.split("/")[-1])

        self.logger.info(f"Successfully added version '{version_id}' to secret '{secret_id}'")
        return response.name

    def get_latest_secret_version(self, secret_id: str) -> Tuple[str, bytes]:
        """
        Retrieves the latest enabled version of a secret.

        Args:
            secret_id (str): The ID of the secret from which to retrieve the version.

        Returns:
            Tuple[str, bytes]: A tuple containing the data ID and the payload of the latest secret version.
        """
        self.logger.info(f"Retrieving latest version of secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot retrieve latest version."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        version_id = self._get_latest_secret_version_id(secret_id)
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"

        self.logger.debug(f"Accessing secret version '{version_id}' of secret '{secret_id}'")
        response = self.client.access_secret_version(request={"name": name})

        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)

        if int(crc32c.hexdigest(), 16) != response.payload.data_crc32c:
            error_msg = "CRC32C checksum mismatch. The data may be corrupted."
            self.logger.error(f"{error_msg} for secret '{secret_id}' version '{version_id}'")
            raise ValueError(error_msg)

        self.logger.info(f"Successfully retrieved version '{version_id}' of secret '{secret_id}'")

        data_str = response.payload.data.decode('utf-8')
        data_id, payload = data_str.split(":", 1)
        return data_id, payload.encode('utf-8')

    def enable_secret_version(self, secret_id: str, version_id: str) -> None:
        """
        Enables a specific version of a secret.

        Args:
            secret_id (str): The ID of the secret from which to enable the version.
            version_id (str): The version ID to enable.
        """
        self.logger.info(f"Enabling version '{version_id}' of secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot enable version."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.debug(f"Verifying version '{version_id}' exists for secret '{secret_id}'")
        version_exists = any(
            version.name.split("/")[-1] == version_id and version.state == secretmanager.SecretVersion.State.DISABLED
            for version in self._get_secret_versions(secret_id)
        )
        if not version_exists:
            error_msg = f"Version {version_id} does not exist or is not disabled for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        self.logger.debug(f"Enabling version '{version_id}' of secret '{secret_id}'")
        response = self.client.enable_secret_version(request={"name": name})

        if response.name.split("/")[-1] != version_id or response.state != secretmanager.SecretVersion.State.ENABLED:
            error_msg = f"Failed to enable secret version {version_id} for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Wait for the version to be enabled
        self.logger.debug(f"Waiting for version '{version_id}' of secret '{secret_id}' to be enabled")
        delay = 1
        for _ in range(self.max_retries):
            if self._secret_version_is_enabled(secret_id, version_id):
                self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' is now enabled")
                break
            self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' still disabled, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify enabling of version '{version_id}' of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        self.logger.info(f"Successfully enabled version '{version_id}' of secret '{secret_id}'")

    def disable_secret_version(self, secret_id: str, version_id: str) -> None:
        """
        Disables a specific version of a secret.

        Args:
            secret_id (str): The ID of the secret from which to delete the version.
            version_id (str): The version ID to delete.
        """
        self.logger.info(f"Disabling version '{version_id}' of secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot disable version."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.debug(f"Verifying version '{version_id}' exists for secret '{secret_id}'")

        version_exists = any(
            version.name.split("/")[-1] == version_id
            for version in self._get_secret_versions(secret_id)
        )
        if not version_exists:
            error_msg = f"Version {version_id} does not exist for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        self.logger.debug(f"Disabling version '{version_id}' of secret '{secret_id}'")
        response = self.client.disable_secret_version(request={"name": name})

        if response.name.split("/")[-1] != version_id or response.state != secretmanager.SecretVersion.State.DISABLED:
            error_msg = f"Failed to disable secret version {version_id} for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Wait for the version to be disabled
        self.logger.debug(f"Waiting for version '{version_id}' of secret '{secret_id}' to be disabled")
        delay = 1
        for _ in range(self.max_retries):
            if not self._secret_version_is_enabled(secret_id, version_id):
                self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' is now disabled")
                break
            self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' still enabled, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify disabling of version '{version_id}' of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully disabled version '{version_id}' of secret '{secret_id}'")

    def destroy_secret_version(self, secret_id: str, version_id: str) -> str:
        """
        Destroys a specific version of a secret.

        Args:
            secret_id (str): The ID of the secret from which to delete the version.
            version_id (str): The version ID to delete.
        Returns:
            str: The data ID of the destroyed version.
        """
        self.logger.info(f"Destroying version '{version_id}' of secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot destroy version."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.debug(f"Verifying version '{version_id}' exists for secret '{secret_id}'")

        version_exists = any(
            version.name.split("/")[-1] == version_id
            for version in self._get_secret_versions(secret_id)
        )
        if not version_exists:
            error_msg = f"Version {version_id} does not exist for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Enable the version before destroying it to get the data ID
        if not self._secret_version_is_enabled(secret_id, version_id):
            self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' is not enabled, enabling it before destruction")
            self.enable_secret_version(secret_id, version_id)
        
        # Get the data ID from the specific version we're about to destroy
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(request={"name": name})
        data_str = response.payload.data.decode('utf-8')
        data_id, _ = data_str.split(":", 1)
        self.logger.debug(f"Data ID for version '{version_id}' of secret '{secret_id}': {data_id}")
    
        # Now destroy the version
        self.logger.debug(f"Destroying version '{version_id}' of secret '{secret_id}'")
        response = self.client.destroy_secret_version(request={"name": name})

        if response.name.split("/")[-1] != version_id or response.state != secretmanager.SecretVersion.State.DESTROYED:
            error_msg = f"Failed to destroy secret version {version_id} for secret {secret_id}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Wait for the version to be destroyed
        self.logger.debug(f"Waiting for version '{version_id}' of secret '{secret_id}' to be destroyed")
        delay = 1
        for _ in range(self.max_retries):
            if self._secret_version_is_destroyed(secret_id, version_id):
                self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' is now destroyed")
                break
            self.logger.debug(f"Version '{version_id}' of secret '{secret_id}' still not destroyed, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Could not verify destruction of version '{version_id}' of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully destroyed version '{version_id}' of secret '{secret_id}'")
        return data_id

    def purge_disabled_secret_versions(self, secret_id: str) -> List[str]:
        """
        Purges (destroys) all disabled versions of a secret that are older than the grace period.
        To determine if a version is older than the grace period, it checks the creation time of each version,
        if the latest version was created more than the grace period ago, it will purge the disabled versions.

        Args:
            secret_id (str): The ID of the secret for which to purge disabled versions.
        Returns:
            List[str]: A list of data IDs of the destroyed versions.
        """
        self.logger.info(f"Purging disabled versions of secret '{secret_id}'")

        if not self._secret_is_managed(secret_id):
            error_msg = f"Secret {secret_id} does not exist or is not managed by this service, cannot purge versions."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        data_ids = []

        for version in self._get_secret_versions(secret_id):
            if version.state == secretmanager.SecretVersion.State.DISABLED:
                version_id = version.name.split("/")[-1]
                create_time = datetime.fromtimestamp(version.create_time.timestamp(), tz=timezone.utc)  # type: ignore
                if create_time < datetime.now(timezone.utc) - timedelta(days=self.grace_period):
                    self.logger.debug(f"Destroying disabled version '{version_id}' of secret '{secret_id}'")
                    data_ids.append(self.destroy_secret_version(secret_id, version_id))
                else:
                    self.logger.debug(f"Skipping version '{version_id}' of secret '{secret_id}' as it is within the grace period")

        return data_ids

    def cron(self) -> Dict[str, List[str]]:
        """
        Performs periodic maintenance tasks:
        - Purges disabled secret versions that are older than the grace period.

        Returns:
            Dict[str, List[str]]: A dictionary with secret IDs as keys and lists of destroyed data IDs as values.
        """
        self.logger.info("Starting periodic maintenance tasks (cron)")
        destroyed_secret_ids = {}

        for secret_id in self._get_secret_ids():
            self.logger.debug(f"Processing secret '{secret_id}' for maintenance")
            purged_data_ids = self.purge_disabled_secret_versions(secret_id)
            if purged_data_ids:
                self.logger.info(f"Purged disabled versions of secret '{secret_id}': {purged_data_ids}")
                destroyed_secret_ids[secret_id] = purged_data_ids

        return destroyed_secret_ids