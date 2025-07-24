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

# This script is used to generate, rotated, disable and delete secrets in the Apache Beam
# project. It uses the Google Cloud secret manager to store the keys securely and the Google
# Cloud storage to keep track of the logging information.
# It is configured by the `config.yaml` file in the same directory.

import yaml
import google_crc32c
from google.cloud import secretmanager
from datetime import timedelta

# --- Configuration ---
CONFIG_FILE = 'config.yaml'

def load_config():
    """Loads the configuration from the YAML file."""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

class SecretService:
    """Service to manage GCP API keys rotation."""

    def __init__(self, config):
        """
        Initializes the SecretService with the provided configuration.

        Args:
            config (dict): Configuration dictionary containing the necessary parameters.
        Raises:
            ValueError: If any required configuration parameter is missing.
        """
        self.project_id = config.get('project_id')
        self.secret_name_prefix = config.get('secret_name_prefix')
        self.rotation_interval = config.get('rotation_interval')
        self.max_versions_to_keep = config.get('max_versions_to_keep')
        self.bucket_name = config.get('bucket_name')
        self.log_file_prefix = config.get('log_file_prefix')

        if not all([self.project_id, self.secret_name_prefix, self.rotation_interval,
                    self.max_versions_to_keep, self.bucket_name, self.log_file_prefix]):
            if not self.project_id:
                raise ValueError("Configuration is missing 'project_id'.")
            if not self.secret_name_prefix:
                raise ValueError("Configuration is missing 'secret_name_prefix'.")
            if not self.rotation_interval:
                raise ValueError("Configuration is missing 'rotation_interval'.")
            if not self.max_versions_to_keep:
                raise ValueError("Configuration is missing 'max_versions_to_keep'.")
            if not self.bucket_name:
                raise ValueError("Configuration is missing 'bucket_name'.")
            if not self.log_file_prefix:
                raise ValueError("Configuration is missing 'log_file_prefix'.")

        self.client = secretmanager.SecretManagerServiceClient() # Initialize the Secret Manager client
        self.parent = f"projects/{self.project_id}"
        self.secrets_names = []  # List to hold the names of the secrets
        self._get_secrets_names()  # Retrieve existing secrets

    def _get_secrets_names(self) -> None:
        """Retrieves the list of secrets from the Secret Manager and populates the `secrets_names` list."""
        for secret in self.client.list_secrets(request={"parent": self.parent}):
            secret_id = secret.name.split("/")[-1]
            if secret_id.startswith(self.secret_name_prefix):
                self.secrets_names.append(secret_id)

    def _create_secret(self, secret_id: str) -> str:
        """
        Create a new secret with the given name. A secret is a logical wrapper
        around a collection of secret versions. Secret versions hold the actual
        secret material. This method creates a new secret with automatic replication
        and a specified time-to-live (TTL) for the secret. Do not use this method
        directly; use the `add_secret` method instead.

        Args:
            secret_id (str): The ID to assign to the new secret. This ID must be unique within the project.
        Returns:
            str: The id of the created secret.
        """
        secret_name = f"{self.secret_name_prefix}-{secret_id}"
        if secret_name in self.secrets_names:
            name = self.client.secret_path(self.project_id, secret_name)
            return name

        response = self.client.create_secret(
            request={
                "parent": self.parent,
                "secret_id": f"{secret_name}",
                "secret": {
                    "replication": {
                        "automatic": {}
                    },
                    "ttl": timedelta(days=self.rotation_interval),
                }
            }
        )

        self.secrets_names.append(secret_name)
        return response.name

    def _rotate_secret(self, secret_name: str, new_version_payload: bytes|str) -> None:
        """
        Rotates the specified secret by creating a new version of it and disabling the oldest enabled version.

        Args:
            secret_name (str): The name of the secret to rotate.
            new_version_payload (bytes|str): The payload for the new secret version.
        """
        secret_id = self._create_secret(secret_name)

        self.add_secret_version(secret_name, new_version_payload)

        # Disable the oldest enabled version
        self.disable_secret_version(secret_name, version_id="oldest")
    
    def _delete_secret(self, secret_name: str) -> None:
        """
        Deletes the specified secret and all its versions.

        Args:
            secret_name (str): The name of the secret to delete.
        """
        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"
        if secret_full_name not in self.secrets_names:
            raise ValueError(f"Secret {secret_name} does not exist. Please create it first.")

        name = self.client.secret_path(self.project_id, secret_full_name)
        self.client.delete_secret(request={"name": name})
        self.secrets_names.remove(secret_full_name)

    def add_secret_version(self, secret_name: str, payload: bytes|str) -> str:
        """
        Adds a new version to the specified secret with the given payload.
        If the secret does not exist, it will be created first.

        Args:
            secret_name (str): The name of the secret to which the version will be added.
            payload (bytes): The secret data to be stored in the new version.
        Returns:
            str: The name of the newly created secret version.
        """
        secret_id = self._create_secret(secret_name)
        payload_bytes = payload.encode('utf-8') if isinstance(payload, str) else payload

        if not isinstance(payload_bytes, (bytes, str)):
            raise TypeError("Payload must be a bytes object or a string that can be encoded to bytes.")

        crc32c = google_crc32c.Checksum()
        crc32c.update(payload_bytes)

        response = self.client.add_secret_version(
            request={
                "parent": secret_id,
                "payload": {
                    "data": payload_bytes,
                    "data_crc32c": int(crc32c.hexdigest(), 16),
                }
            }
        )

        versions = list(self.client.list_secret_versions(request={"parent": secret_id}))
        if len(versions) > self.max_versions_to_keep:
            versions = [v for v in versions if v.state != secretmanager.SecretVersion.State.DESTROYED]
            if not versions:
                raise ValueError("No enabled secret versions found to destroy.")
            oldest_version = versions[0]
            self.client.destroy_secret_version(request={"name": oldest_version.name})

        return response.name

    def get_secret_version(self, secret_name: str, version_id: str = "latest") -> bytes:
        """
        Retrieves the specified version of a secret. If version_id is "latest",
        it retrieves the latest enabled secret version.

        Args:
            secret_name (str): The name of the secret from which to retrieve the version.
            version_id (str): The version ID to retrieve. Defaults to "latest".
        Returns:
            bytes: The payload of the specified secret version.
        """

        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"
        if secret_full_name not in self.secrets_names:
            raise ValueError(f"Secret {secret_name} does not exist. Please create it first.")

        if version_id == "latest":
            parent = self.client.secret_path(self.project_id, secret_full_name)
            for version in self.client.list_secret_versions(request={"parent": parent}):
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    break
            else:
                raise ValueError(f"No enabled versions found for secret {secret_name}.")

        name = f"projects/{self.project_id}/secrets/{secret_full_name}/versions/{version_id}"

        response = self.client.access_secret_version(request={"name": name})

        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)

        if int(crc32c.hexdigest(), 16) != response.payload.data_crc32c:
            raise ValueError("CRC32C checksum mismatch. The data may be corrupted.")

        return response.payload.data

    def disable_secret_version(self, secret_name: str, version_id: str = "oldest") -> None:
        """
        Disables a specific version of a secret. If the version is "latest", it disables the latest enabled version.

        Args:
            secret_name (str): The name of the secret from which to delete the version.
            version_id (str): The version ID to delete. Defaults to "oldest", "latest" can also be used.
        """
        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"

        if secret_full_name not in self.secrets_names:
            return

        parent = self.client.secret_path(self.project_id, secret_full_name)
        versions = self.client.list_secret_versions(request={"parent": parent})

        if version_id == "latest":
            for version in versions:
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    break
            else:
                raise ValueError(f"No enabled versions found for secret {secret_name}.")
        elif version_id == "oldest":
            for version in reversed(list(versions)):
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    break
            else:
                raise ValueError(f"No enabled versions found for secret {secret_name}.")
        else:
            version_exists = any(version.name.split("/")[-1] == version_id for version in versions)
            if not version_exists:
                raise ValueError(f"Version {version_id} does not exist for secret {secret_name}.")

        name = f"projects/{self.project_id}/secrets/{secret_full_name}/versions/{version_id}"
        response = self.client.disable_secret_version(request={"name": name})

        if response.name.split("/")[-1] != version_id and response.state != secretmanager.SecretVersion.State.DISABLED:
            raise ValueError(f"Failed to disable secret version {version_id} for secret {secret_name}.")

if __name__ == "__main__":
    config = load_config()
    secret_service = SecretService(config)

    # Example usage
    secret_name = "example-secret"

    # Deleting the secret
    print(f"Deleting secret: {secret_name}")
    secret_service._delete_secret(secret_name)
    print(f"Secret {secret_name} deleted successfully.")

    # Adding multiple versions of a secret
    for i in range(1, 4):
        secret_version = secret_service.add_secret_version(secret_name, f"This is test secret version {i}")
        print(f"Added secret version: {secret_version}")

    # Retrieving the latest version of the secret, it should return 4
    retrieved_secret = secret_service.get_secret_version(secret_name)
    print(f"Retrieved secret: {retrieved_secret.decode('utf-8')}")

    secret_service.disable_secret_version(secret_name, version_id="latest")
    print(f"Disabled secret latest version for: {secret_name}")

    print("Attempting to retrieve the latest version after disabling...")
    try:
        retrieved_secret = secret_service.get_secret_version(secret_name)
        # This should retrieve the latest enabled version, which is now 3
        print(f"Retrieved secret after disabling: {retrieved_secret.decode('utf-8')}")
    except Exception as e:
        print(f"Error retrieving secret after disabling: {e}")
    
    # Rotate the secret
    new_version_payload = "This is a new secret version payload"
    print(f"Rotating secret {secret_name} with new payload: {new_version_payload}")
    secret_service._rotate_secret(secret_name, new_version_payload)

    retrieved_secret = secret_service.get_secret_version(secret_name)
    print(f"Retrieved secret after rotation: {retrieved_secret.decode('utf-8')}")



