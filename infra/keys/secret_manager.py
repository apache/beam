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
from datetime import datetime, timezone
from google.cloud import secretmanager
from typing import List, Union


class SecretManager:
    """Service to manage GCP API keys rotation."""

    def __init__(self, project_id: str,  logger: logging.Logger, rotation_interval: int = 30, max_versions_to_keep: int = 5) -> None:
        self.project_id = project_id
        self.rotation_interval = rotation_interval
        self.max_versions_to_keep = max_versions_to_keep
        self.client = secretmanager.SecretManagerServiceClient()
        self.logger = logger
        self.logger.info(f"Initialized SecretManager for project '{self.project_id}'")
        self.secrets_ids: List[str] = self._get_secrets_ids()

    def _get_secrets_ids(self) -> List[str]:
        """Retrieves the list of secrets from the Secret Manager and populates the `secrets_ids` list."""
        self.logger.info(f"Retrieving secrets with the label from project '{self.project_id}'")
        secrets_ids = []
        for secret in self.client.list_secrets(request={"parent": f"projects/{self.project_id}"}):
            secret_id = secret.name.split("/")[-1]
            # Check if the secret has the label indicating it was created by this service
            if "created_by" in secret.labels and secret.labels["created_by"] == "secretmanager-service":
                secrets_ids.append(secret_id)
        self.logger.info(f"Found {len(secrets_ids)} secrets created by secretmanager-service in project '{self.project_id}'")
        return secrets_ids

    def create_secret(self, secret_id: str) -> str:
        """
        Create a new secret with the given name. A secret is a logical wrapper
        around a collection of secret versions. Secret versions hold the actual
        secret material. This method creates a new secret with automatic replication
        and a specified time-to-live (TTL) for the secret. Do not use this method
        directly; use the `add_secret` method instead.

        Args:
            secret_id (str): The ID to assign to the new secret. This ID must be unique within the project.
        Returns:
            str: The secret path of the newly created secret.
        """
        if secret_id in self.secrets_ids:
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

        self.secrets_ids.append(secret_id)
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

        if secret_id not in self.secrets_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
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
        if secret_id not in self.secrets_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"Deleting secret '{secret_id}' and all its versions")
        name = self.client.secret_path(self.project_id, secret_id)
        self.client.delete_secret(request={"name": name})
        self.secrets_ids.remove(secret_id)
        self.logger.info(f"Successfully deleted secret '{secret_id}'")

    def rotate_secret(self, secret_id: str, new_version_payload: Union[bytes, str]) -> None:
        """
        Rotates the specified secret by creating a new version of it and disabling the oldest enabled version.

        Args:
            secret_id (str): The ID of the secret to rotate.
            new_version_payload (bytes|str): The payload for the new secret version.
        """
        if secret_id not in self.secrets_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"Starting rotation for secret '{secret_id}'")
        try:
            self.logger.debug(f"Adding new version to secret '{secret_id}'")
            new_version = self.add_secret_version(secret_id, new_version_payload)

            enabled_versions = [v for v in self.list_secret_versions(secret_id)
                               if v.state == secretmanager.SecretVersion.State.ENABLED]
            if len(enabled_versions) > 1:
                # Disable the oldest enabled version
                self.logger.debug(f"Disabling oldest version of secret '{secret_id}'")
                self.disable_secret_version(secret_id, version_id="oldest")

            self.logger.info(f"Successfully rotated secret '{secret_id}'. New version: {new_version.split('/')[-1]}")
        except Exception as e:
            self.logger.error(f"Failed to rotate secret '{secret_id}': {str(e)}")
            raise

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

        # Check if the secret has more than the maximum allowed versions,
        # and disable the oldest versions if necessary
        self.logger.debug(f"Checking if secret '{secret_id}' has more than {self.max_versions_to_keep} versions enabled")
        all_versions = self.list_secret_versions(secret_id)
        enabled_versions = [v for v in all_versions if v.state == secretmanager.SecretVersion.State.ENABLED]
        if len(enabled_versions) > self.max_versions_to_keep:
            exceeding_versions = enabled_versions[self.max_versions_to_keep:]
            self.logger.debug(f"Disabling {len(exceeding_versions)} versions of secret '{secret_id}'")
            for version in exceeding_versions:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Disabling version '{version_id}' of secret '{secret_id}'")
                self.disable_secret_version(secret_id, version_id=version_id)

        # Update the last version created timestamp
        self.logger.debug(f"Updating last version created timestamp for secret '{secret_id}'")
        secret_obj = self.get_secret(secret_id)
        labels = dict(secret_obj.labels)
        labels["last_version_created_at"] = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        secret = {"name": secret_obj.name, "labels": labels}
        update_mask = {"paths": ["labels"]}
        self.client.update_secret(request={"secret": secret, "update_mask": update_mask})

        version_id = response.name.split("/")[-1]
        self.logger.info(f"Successfully added version '{version_id}' to secret '{secret_id}'")
        return response.name
    
    def list_secret_versions(self, secret_id: str) -> List[secretmanager.SecretVersion]:
        """
        Lists all versions of a secret.

        Args:
            secret_id (str): The ID of the secret to list versions for.
        Returns:
            List[secretmanager.SecretVersion]: A list of secret versions.
        """
        self.logger.info(f"Listing versions for secret '{secret_id}'")

        if secret_id not in self.secrets_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        parent = self.client.secret_path(self.project_id, secret_id)
        versions = list(self.client.list_secret_versions(request={"parent": parent}))
        self.logger.info(f"Found {len(versions)} versions for secret '{secret_id}'")
        return versions

    
    def get_latest_secret_version_id(self, secret_id: str) -> str:
        """
        Retrieves the latest enabled version of a secret.

        Args:
            secret_id (str): The ID of the secret to retrieve the latest version for.
        Returns:
            str: The name of the latest secret version.
        """
        self.logger.info(f"Retrieving latest enabled version of secret '{secret_id}'")

        for version in self.list_secret_versions(secret_id):
            if version.state == secretmanager.SecretVersion.State.ENABLED:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Found latest enabled version '{version_id}' for secret '{secret_id}'")
                return version_id
        error_msg = f"No enabled versions found for secret {secret_id}."
        self.logger.error(error_msg)
        raise ValueError(error_msg)
    
    def get_oldest_secret_version_id(self, secret_id: str) -> str:
        """
        Retrieves the oldest version of a secret.

        Args:
            secret_id (str): The ID of the secret to retrieve the oldest version for.
        Returns:
            str: The name of the oldest secret version.
        """

        self.logger.info(f"Retrieving oldest version of secret '{secret_id}'")
        for version in reversed(self.list_secret_versions(secret_id)):
            if version.state == secretmanager.SecretVersion.State.ENABLED:
                version_id = version.name.split("/")[-1]
                self.logger.debug(f"Found oldest enabled version '{version_id}' for secret '{secret_id}'")
                return version_id
        error_msg = f"No enabled versions found for secret {secret_id}."
        self.logger.error(error_msg)
        raise ValueError(error_msg)

    def get_secret_version(self, secret_id: str, version_id: str = "latest") -> bytes:
        """
        Retrieves the specified version of a secret. If version_id is "latest" or not provided,
        it retrieves the latest enabled secret version.

        Args:
            secret_id (str): The ID of the secret from which to retrieve the version.
            version_id (str): The version ID to retrieve. Defaults to "latest".
        Returns:
            bytes: The payload of the specified secret version.
        """
        self.logger.info(f"Retrieving version '{version_id}' of secret '{secret_id}'")

        if secret_id not in self.secrets_ids:
            error_msg = f"Secret {secret_id} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if version_id == "latest":
            version_id = self.get_latest_secret_version_id(secret_id)
            self.logger.debug(f"Using latest version '{version_id}' for secret '{secret_id}'")

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

        if secret_id not in self.secrets_ids:
            self.logger.warning(f"Attempt to disable version of non-existent secret '{secret_id}'")
            return

        if version_id == "latest":
            version_id = self.get_latest_secret_version_id(secret_id)
            self.logger.debug(f"Using latest version '{version_id}' for secret '{secret_id}'")
        elif version_id == "oldest":
            version_id = self.get_oldest_secret_version_id(secret_id)
            self.logger.debug(f"Using oldest version '{version_id}' for secret '{secret_id}'")

        self.logger.debug(f"Verifying version '{version_id}' exists for secret '{secret_id}'")

        version_exists = any(
            version.name.split("/")[-1] == version_id
            for version in self.list_secret_versions(secret_id)
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

        self.logger.info(f"Successfully disabled version '{version_id}' of secret '{secret_id}'")
