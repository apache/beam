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
from typing import List, Union

class SecretManagerLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds a prefix to all log messages."""
    
    def process(self, msg, kwargs):
        return f"[SecretManager] {msg}", kwargs

class SecretManager:
    """Service to manage GCP API keys rotation."""

    project_id: str # The GCP project ID where secrets are managed
    rotation_interval: int # The interval (in days) at which to rotate secrets
    max_versions_to_keep: int # The maximum number of secret versions to keep
    max_retries: int # The maximum number of retries for API calls
    client: secretmanager.SecretManagerServiceClient # GCP Secret Manager client
    logger: Union[logging.Logger, logging.LoggerAdapter] # Logger for logging messages
    secret_ids: List[str] # List of secret IDs managed by this service

    def __init__(self, project_id: str, logger: logging.Logger, rotation_interval: int = 30, max_versions_to_keep: int = 5, max_retries: int = 3) -> None:
        self.project_id = project_id
        self.rotation_interval = rotation_interval
        self.max_versions_to_keep = max_versions_to_keep
        self.max_retries = max_retries
        self.client = secretmanager.SecretManagerServiceClient()
        self.logger = SecretManagerLoggerAdapter(logger, {})
        self.logger.info(f"Initialized SecretManager for project '{self.project_id}'")
        self.secret_ids = self._get_secrets_ids()

    def _get_secrets_ids(self) -> List[str]:
        """
        Retrieves the list of secrets from the Secret Manager and populates the `secrets_ids` list.
        This method filters secrets based on a specific label indicating they were created by this service.

        Returns:
            List[str]: A list of secret IDs that were created by this service.
        """
        self.logger.debug(f"Retrieving secrets with the label from project '{self.project_id}'")
        secrets_ids = []

        try:
            for secret in self.client.list_secrets(request={"parent": f"projects/{self.project_id}"}):
                secret_id = secret.name.split("/")[-1]
                if "created_by" in secret.labels and secret.labels["created_by"] == "secretmanager-service":
                    secrets_ids.append(secret_id)
        except Exception as e:
            self.logger.error(f"Error retrieving secrets: {e}")

        self.logger.debug(f"Found {len(secrets_ids)} secrets created by secretmanager-service in project '{self.project_id}'")
        return secrets_ids

    def _secret_exists(self, secret_id: str) -> bool:
        """
        Checks if a secret with the given ID exists in the Secret Manager GCP.

        Args:
            secret_id (str): The ID of the secret to check.
        Returns:
            bool: True if the secret exists, False otherwise.
        """
        self.logger.debug(f"Checking if secret '{secret_id}' exists")
        try:
            name = self.client.secret_path(self.project_id, secret_id)
            secret = self.client.get_secret(request={"name": name})

            if "created_by" in secret.labels and secret.labels["created_by"] == "secretmanager-service":
                self.logger.debug(f"Secret '{secret_id}' exists and is managed by secretmanager-service")
                return True
            else:
                self.logger.debug(f"Secret '{secret_id}' exists but is not managed by secretmanager-service")
                return False
        except Exception as e:
            self.logger.debug(f"Secret '{secret_id}' does not exist: {e}")
            return False

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
        if secret_id in self.secret_ids:
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
                        "created_by": "secretmanager-service",
                        "created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                        "rotation_interval_days": str(self.rotation_interval),
                        "last_version_created_at": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                    }
                }
            }
        )

        # Wait for the secret to be created
        self.logger.debug(f"Waiting for secret '{secret_id}' to be created")
        delay = 1
        for _ in range(self.max_retries):
            if self._secret_exists(secret_id):
                self.logger.debug(f"Secret '{secret_id}' is now available")
                self.secret_ids.append(secret_id)
                break
            self.logger.debug(f"Secret '{secret_id}' not found, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Failed to create secret '{secret_id}' after {self.max_retries} retries."
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

        if secret_id not in self.secret_ids:
            self.logger.debug(f"Secret '{secret_id}' not in cached secrets, checking existence")
            if not self._secret_exists(secret_id):
                self.logger.error(f"Secret '{secret_id}' does not exist")
                raise ValueError(f"Secret {secret_id} does not exist. Please create it first.")
            else:
                self.logger.debug(f"Secret '{secret_id}' exists but is not in cached secrets, updating cache")
                self.secret_ids.append(secret_id)

        name = self.client.secret_path(self.project_id, secret_id)
        return self.client.get_secret(request={"name": name})

    def delete_secret(self, secret_id: str) -> None:
        """
        Deletes the specified secret and all its versions.

        Args:
            secret_id (str): The ID of the secret to delete.
        """
        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, nothing to delete")
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
                if secret_id in self.secret_ids:
                    self.logger.debug(f"Removing '{secret_id}' from cached secrets")
                    self.secret_ids.remove(secret_id)
                break
            self.logger.debug(f"Secret '{secret_id}' still exists, retrying in {delay} seconds")
            time.sleep(delay)
            delay *= 2
        else:
            error_msg = f"Failed to delete secret '{secret_id}' after {self.max_retries} retries."
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

        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, cannot check access")
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

        if not self._secret_exists(secret_id):
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
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

        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, cannot retrieve versions")
            return []

        parent = self.client.secret_path(self.project_id, secret_id)
        versions = list(self.client.list_secret_versions(request={"parent": parent}))
        self.logger.debug(f"Found {len(versions)} versions for secret '{secret_id}'")
        return versions

    def _get_enabled_secret_versions(self, secret_id: str) -> List[secretmanager.SecretVersion]:
        """
        Retrieves all enabled versions of a secret.

        Args:
            secret_id (str): The ID of the secret to list enabled versions for.
        Returns:
            List[secretmanager.SecretVersion]: A list of enabled secret versions.
        """
        self.logger.debug(f"Retrieving enabled versions for secret '{secret_id}'")
        versions = self._get_secret_versions(secret_id)
        enabled_versions = [version for version in versions if version.state == secretmanager.SecretVersion.State.ENABLED]
        self.logger.debug(f"Found {len(enabled_versions)} enabled versions for secret '{secret_id}'")
        return enabled_versions

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
        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, version cannot exist")
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
        if not self._secret_exists(secret_id):
            self.logger.debug(f"Secret '{secret_id}' does not exist, version cannot be enabled")
            return False
        
        versions = self._get_secret_versions(secret_id)
        for version in versions:
            if version.name.split("/")[-1] == version_id:
                is_enabled = version.state == secretmanager.SecretVersion.State.ENABLED
                self.logger.debug(f"Version '{version_id}' is enabled: {is_enabled}")
                return is_enabled
        self.logger.debug(f"Version '{version_id}' does not exist for secret '{secret_id}'")
        return False
    
    def _purge_old_secret_versions(self, secret_id: str) -> None:
        """
        Purges old secret versions that are not enabled and exceed the maximum allowed versions.

        Args:
            secret_id (str): The ID of the secret to purge old versions from.
        """
        self.logger.debug(f"Purging old versions for secret '{secret_id}'")
        versions = self._get_secret_versions(secret_id)
        enabled_versions = [v for v in versions if v.state == secretmanager.SecretVersion.State.ENABLED]

        if len(enabled_versions) > self.max_versions_to_keep:
            versions_to_purge = enabled_versions[:len(enabled_versions) - self.max_versions_to_keep]
            self.logger.debug(f"Found {len(versions_to_purge)} versions to purge for secret '{secret_id}'")
            for version in versions_to_purge:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Disabling version '{version_id}' of secret '{secret_id}'")
                self.disable_secret_version(secret_id, version_id=version_id)
        else:
            self.logger.debug(f"No versions to purge for secret '{secret_id}', current count: {len(enabled_versions)}")

    
    def _get_latest_secret_version_id(self, secret_id: str) -> str:
        """
        Retrieves the latest enabled version of a secret.

        Args:
            secret_id (str): The ID of the secret to retrieve the latest version for.
        Returns:
            str: The name of the latest secret version.
        """
        self.logger.debug(f"Retrieving latest enabled version of secret '{secret_id}'")

        for version in self._get_secret_versions(secret_id):
            if version.state == secretmanager.SecretVersion.State.ENABLED:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Found latest enabled version '{version_id}' for secret '{secret_id}'")
                return version_id
        error_msg = f"No enabled versions found for secret {secret_id}."
        self.logger.error(error_msg)
        raise ValueError(error_msg)
    
    def _get_oldest_secret_version_id(self, secret_id: str) -> str:
        """
        Retrieves the oldest version of a secret.

        Args:
            secret_id (str): The ID of the secret to retrieve the oldest version for.
        Returns:
            str: The name of the oldest secret version.
        """

        self.logger.debug(f"Retrieving oldest version of secret '{secret_id}'")
        for version in reversed(self._get_secret_versions(secret_id)):
            if version.state == secretmanager.SecretVersion.State.ENABLED:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Found oldest enabled version '{version_id}' for secret '{secret_id}'")
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
        secret = self.get_secret(secret_id)
        last_version_created_at = secret.labels.get("last_version_created_at")
        
        if not last_version_created_at:
            self.logger.debug(f"No last version created timestamp found for secret '{secret_id}'")
            return False
        
        last_version_date = datetime.strptime(last_version_created_at, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        due_date = last_version_date + timedelta(days=self.rotation_interval)
        
        is_due = datetime.now(timezone.utc) >= due_date
        self.logger.debug(f"Key rotation due for secret '{secret_id}': {is_due}")
        return is_due

    def add_secret_version(self, secret_id: str, payload: Union[bytes, str]) -> str:
        """
        Adds a new version to the specified secret with the given payload.
        If the secret does not exist, it will be created first.

        Args:
            secret_id (str): The ID of the secret to which the version will be added.
            payload (bytes): The secret data to be stored in the new version.
        Returns:
            str: The name of the newly created secret version.
        """
        self.logger.info(f"Adding new version to secret '{secret_id}'")

        secret_path = self.create_secret(secret_id)
        payload_bytes = payload.encode('utf-8') if isinstance(payload, str) else payload

        if not isinstance(payload_bytes, bytes):
            error_msg = "Payload must be a bytes object or a string that can be encoded to bytes."
            self.logger.error(error_msg)
            raise TypeError(error_msg)

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

        # Purge old versions if necessary
        self._purge_old_secret_versions(secret_id)

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
            error_msg = f"Failed to add version '{version_id}' to secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully added version '{version_id}' to secret '{secret_id}'")
        return response.name

    def get_secret_version(self, secret_id: str, version_id: str = "latest") -> bytes:
        """
        Retrieves the specified version of a secret. If version_id is "latest" or not provided,
        it retrieves the latest enabled secret version.

        Args:
            secret_id (str): The ID of the secret from which to retrieve the version.
            version_id (str): The version ID to retrieve. Defaults to "latest", "oldest" can also be used.
        Returns:
            bytes: The payload of the specified secret version.
        """
        self.logger.info(f"Retrieving version '{version_id}' of secret '{secret_id}'")

        if secret_id not in self.secret_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if version_id == "latest":
            version_id = self._get_latest_secret_version_id(secret_id)
            self.logger.debug(f"Using latest version '{version_id}' for secret '{secret_id}'")
        elif version_id == "oldest":
            version_id = self._get_oldest_secret_version_id(secret_id)
            self.logger.debug(f"Using oldest version '{version_id}' for secret '{secret_id}'")

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
        return response.payload.data

    def disable_secret_version(self, secret_id: str, version_id: str = "oldest") -> None:
        """
        Disables a specific version of a secret.

        Args:
            secret_id (str): The ID of the secret from which to delete the version.
            version_id (str): The version ID to delete. Defaults to "oldest", "latest" can also be used.
        """
        self.logger.info(f"Disabling version '{version_id}' of secret '{secret_id}'")

        if secret_id not in self.secret_ids:
            self.logger.warning(f"Attempt to disable version of non-existent secret '{secret_id}'")
            return

        if version_id == "latest":
            version_id = self._get_latest_secret_version_id(secret_id)
            self.logger.debug(f"Using latest version '{version_id}' for secret '{secret_id}'")
        elif version_id == "oldest":
            version_id = self._get_oldest_secret_version_id(secret_id)
            self.logger.debug(f"Using oldest version '{version_id}' for secret '{secret_id}'")

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
            error_msg = f"Failed to disable version '{version_id}' of secret '{secret_id}' after {self.max_retries} retries."
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"Successfully disabled version '{version_id}' of secret '{secret_id}'")

    def rotate_secret(self, secret_id: str, new_version_payload: Union[bytes, str]) -> None:
        """
        Rotates the specified secret by creating a new version of it and disabling the oldest enabled version.

        Args:
            secret_id (str): The ID of the secret to rotate.
            new_version_payload (bytes|str): The payload for the new secret version.
        """
        if secret_id not in self.secret_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"Starting rotation for secret '{secret_id}'")
        try:
            self.logger.debug(f"Adding new version to secret '{secret_id}'")
            new_version = self.add_secret_version(secret_id, new_version_payload)

            enabled_versions = self._get_enabled_secret_versions(secret_id)
            if len(enabled_versions) > 1:
                self.logger.debug(f"Disabling oldest version of secret '{secret_id}'")
                self.disable_secret_version(secret_id, version_id="oldest")
            self.logger.info(f"Successfully rotated secret '{secret_id}'. New version: {new_version.split('/')[-1]}")
        except Exception as e:
            self.logger.error(f"Failed to rotate secret '{secret_id}': {str(e)}")
            raise

