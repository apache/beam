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

import google_crc32c
import io
import logging
import yaml
from datetime import timedelta, datetime
from google.cloud import secretmanager
from google.cloud import storage
from typing import List, Optional, Union, Dict, TypedDict
from service_account import ServiceAccountManager


# --- Configuration ---
CONFIG_FILE = 'config.yaml'

class ConfigDict(TypedDict):
    project_id: str
    secret_name_prefix: str
    rotation_interval: int
    max_versions_to_keep: int
    bucket_name: str
    log_file_prefix: str
    logging_level: str

def load_config() -> ConfigDict:
    """Loads the configuration from the YAML file."""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

class GCSLogHandler(logging.Handler):
    """Custom logging handler that writes logs to Google Cloud Storage."""

    bucket_name: str
    log_file_prefix: str
    project_id: str
    storage_client: storage.Client
    bucket: storage.Bucket
    log_buffer: io.StringIO
    log_entries_count: int
    max_buffer_size: int
    session_logs: List[str]
    blob_name: str
    
    def __init__(self, bucket_name: str, log_file_prefix: str, project_id: str) -> None:
        """
        Initialize the GCS log handler.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            log_file_prefix (str): Prefix for log file names
            project_id (str): Google Cloud project ID
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_prefix = log_file_prefix
        self.project_id = project_id
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)
        self.log_buffer = io.StringIO()
        self.log_entries_count = 0
        self.max_buffer_size = 100
        
        # Create a session-based filename that stays consistent
        session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.blob_name = f"{self.log_file_prefix}/secret_service_{session_timestamp}.log"
        self.session_logs = []  # Store all logs for the session

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the buffer and flush to GCS when buffer is full.
        
        Args:
            record: Log record to emit
        """
        try:
            log_entry = self.format(record)
            self.log_buffer.write(log_entry + '\n')
            self.session_logs.append(log_entry)
            self.log_entries_count += 1
            
            # Flush to GCS when buffer reaches max size
            if self.log_entries_count >= self.max_buffer_size:
                self.flush_to_gcs()
                
        except Exception:
            self.handleError(record)

    def flush_to_gcs(self) -> None:
        """Flush the log buffer to Google Cloud Storage."""
        if self.log_entries_count == 0:
            return
            
        try:
            # Upload all session logs to GCS
            blob = self.bucket.blob(self.blob_name)
            complete_log_content = '\n'.join(self.session_logs) + '\n'
            blob.upload_from_string(complete_log_content, content_type='text/plain')
            
            self.log_buffer = io.StringIO()
            self.log_entries_count = 0
            
        except Exception as e:
            # If GCS upload fails, print to stderr to avoid losing logs
            import sys
            print(f"Failed to upload logs to GCS: {e}", file=sys.stderr)
            print(f"Log file would be: {self.blob_name}", file=sys.stderr)
    
    def close(self) -> None:
        """Close the handler and flush remaining logs to GCS."""
        self.flush_to_gcs()
        super().close()

    def get_log_file_path(self) -> str:
        """Get the GCS path for the current log file."""
        return f"gs://{self.bucket_name}/{self.blob_name}"


class SecretService:
    """Service to manage GCP API keys rotation."""

    # Configuration
    project_id: str
    secret_name_prefix: str
    rotation_interval: int
    max_versions_to_keep: int
    secrets_names: List[str]

    # Clients
    client: secretmanager.SecretManagerServiceClient
    logger: logging.Logger
    manager: ServiceAccountManager

    def __init__(self, config: ConfigDict) -> None:
        """
        Initializes the SecretService with the provided configuration.

        Args:
            config (ConfigDict): Configuration dictionary containing:
                - project_id: GCP project ID
                - secret_name_prefix: Prefix for secret names
                - rotation_interval: Interval in days for secret rotation 
                - max_versions_to_keep: Maximum number of secret versions to keep
                - bucket_name: GCS bucket name for logging
                - log_file_prefix: Prefix for log file names
                - logging_level: Logging level (e.g., 'INFO', 'DEBUG')
        Raises:
            ValueError: If any required configuration parameter is missing.
        """

        self.project_id = config['project_id']
        self.secret_name_prefix = config['secret_name_prefix']
        self.rotation_interval = config['rotation_interval']
        self.max_versions_to_keep = config['max_versions_to_keep']
        bucket_name = config['bucket_name']
        log_file_prefix = config['log_file_prefix']
        logging_level = config.get('logging_level', 'INFO').upper()

        if not self.project_id.strip():
            raise ValueError("Configuration 'project_id' cannot be empty.")
        if not self.secret_name_prefix.strip():
            raise ValueError("Configuration 'secret_name_prefix' cannot be empty.")
        if self.rotation_interval <= 0:
            raise ValueError("Configuration 'rotation_interval' must be positive.")
        if self.max_versions_to_keep <= 0:
            raise ValueError("Configuration 'max_versions_to_keep' must be positive.")
        if not bucket_name.strip():
            raise ValueError("Configuration 'bucket_name' cannot be empty.")
        if not log_file_prefix.strip():
            raise ValueError("Configuration 'log_file_prefix' cannot be empty.")

        self.client = secretmanager.SecretManagerServiceClient() # Initialize the Secret Manager client
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging_level)
        
        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Add GCS handler for persistent logging
        try:
            gcs_handler = GCSLogHandler(bucket_name, log_file_prefix, self.project_id)
            gcs_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
            gcs_handler.setFormatter(gcs_formatter)
            self.logger.addHandler(gcs_handler)
            self.logger.info("GCS logging handler initialized successfully")
            self.logger.info(f"Logs will be stored at: {gcs_handler.get_log_file_path()}")
        except Exception as e:
            self.logger.warning(f"Failed to initialize GCS logging handler: {e}. Logs will only be written to console.")
        
        self.manager = ServiceAccountManager(self.project_id, self.logger)

        self.secrets_names = []  # List to hold the names of the secrets
        self._get_secrets_names()  # Retrieve existing secrets

        self.logger.info(f"Initialized SecretService for project: {self.project_id}")

    def __del__(self) -> None:
        """Destructor to ensure logs are flushed when the service is destroyed."""
        self.flush_logs()

    def flush_logs(self) -> None:
        """Manually flush all logs to Google Cloud Storage."""
        for handler in self.logger.handlers:
            if isinstance(handler, GCSLogHandler):
                handler.flush_to_gcs()
                self.logger.info("Logs flushed to Google Cloud Storage")

    def get_log_file_path(self) -> Optional[str]:
        """Get the GCS path where logs are being stored."""
        for handler in self.logger.handlers:
            if isinstance(handler, GCSLogHandler):
                return handler.get_log_file_path()
        return None

    def _get_secrets_names(self) -> None:
        """Retrieves the list of secrets from the Secret Manager and populates the `secrets_names` list."""
        self.logger.info(f"Retrieving secrets with prefix '{self.secret_name_prefix}' from project '{self.project_id}'")
        secret_count = 0
        for secret in self.client.list_secrets(request={"parent": f"projects/{self.project_id}"}):
            secret_id = secret.name.split("/")[-1]
            if secret_id.startswith(self.secret_name_prefix):
                self.secrets_names.append(secret_id)
                secret_count += 1
        self.logger.info(f"Found {secret_count} secrets with prefix '{self.secret_name_prefix}'")

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
            self.logger.debug(f"Secret '{secret_name}' already exists, returning existing secret path")
            name = self.client.secret_path(self.project_id, secret_name)
            return name

        self.logger.info(f"Creating new secret '{secret_name}' with TTL of {self.rotation_interval} days")
        response = self.client.create_secret(
            request={
                "parent": f"projects/{self.project_id}",
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
        self.logger.info(f"Successfully created secret '{secret_name}'")
        return response.name

    def _rotate_secret(self, secret_name: str, new_version_payload: Union[bytes, str]) -> None:
        """
        Rotates the specified secret by creating a new version of it and disabling the oldest enabled version.

        Args:
            secret_name (str): The name of the secret to rotate.
            new_version_payload (bytes|str): The payload for the new secret version.
        """
        self.logger.info(f"Starting rotation for secret '{secret_name}'")
        try:
            self.logger.debug(f"Adding new version to secret '{secret_name}'")
            new_version = self.add_secret_version(secret_name, new_version_payload)

            # Disable the oldest enabled version
            self.logger.debug(f"Disabling oldest version of secret '{secret_name}'")
            self.disable_secret_version(secret_name, version_id="oldest")
            self.logger.info(f"Successfully rotated secret '{secret_name}'. New version: {new_version.split('/')[-1]}")
        except Exception as e:
            self.logger.error(f"Failed to rotate secret '{secret_name}': {str(e)}")
            raise
    
    def _delete_secret(self, secret_name: str) -> None:
        """
        Deletes the specified secret and all its versions.

        Args:
            secret_name (str): The name of the secret to delete.
        """
        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"
        if secret_full_name not in self.secrets_names:
            self.logger.warning(f"Attempt to delete non-existent secret '{secret_name}'")
            raise ValueError(f"Secret {secret_name} does not exist. Please create it first.")

        self.logger.info(f"Deleting secret '{secret_name}' and all its versions")
        name = self.client.secret_path(self.project_id, secret_full_name)
        self.client.delete_secret(request={"name": name})
        self.secrets_names.remove(secret_full_name)
        self.logger.info(f"Successfully deleted secret '{secret_name}'")

    def add_secret_version(self, secret_name: str, payload: Union[bytes, str]) -> str:
        """
        Adds a new version to the specified secret with the given payload.
        If the secret does not exist, it will be created first.

        Args:
            secret_name (str): The name of the secret to which the version will be added.
            payload (bytes): The secret data to be stored in the new version.
        Returns:
            str: The name of the newly created secret version.
        """
        self.logger.info(f"Adding new version to secret '{secret_name}'")
        secret_id = self._create_secret(secret_name)
        payload_bytes = payload.encode('utf-8') if isinstance(payload, str) else payload

        if not isinstance(payload_bytes, (bytes, str)):
            error_msg = "Payload must be a bytes object or a string that can be encoded to bytes."
            self.logger.error(error_msg)
            raise TypeError(error_msg)

        crc32c = google_crc32c.Checksum()
        crc32c.update(payload_bytes)

        self.logger.debug(f"Creating secret version with CRC32C checksum")
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
        self.logger.debug(f"Secret '{secret_name}' now has {len(versions)} versions")
        
        if len(versions) > self.max_versions_to_keep:
            self.logger.info(f"Secret '{secret_name}' has {len(versions)} versions, exceeding max of {self.max_versions_to_keep}. Destroying oldest version.")
            versions = [v for v in versions if v.state != secretmanager.SecretVersion.State.DESTROYED]
            if not versions:
                error_msg = "No enabled secret versions found to destroy."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            oldest_version = versions[0]
            oldest_version_id = oldest_version.name.split("/")[-1]
            self.logger.debug(f"Destroying oldest version '{oldest_version_id}' of secret '{secret_name}'")
            self.client.destroy_secret_version(request={"name": oldest_version.name})

        version_id = response.name.split("/")[-1]
        self.logger.info(f"Successfully added version '{version_id}' to secret '{secret_name}'")
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
        self.logger.info(f"Retrieving version '{version_id}' of secret '{secret_name}'")

        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"
        if secret_full_name not in self.secrets_names:
            error_msg = f"Secret {secret_name} does not exist. Please create it first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if version_id == "latest":
            self.logger.debug(f"Finding latest enabled version for secret '{secret_name}'")
            parent = self.client.secret_path(self.project_id, secret_full_name)
            for version in self.client.list_secret_versions(request={"parent": parent}):
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    self.logger.debug(f"Found latest enabled version '{version_id}' for secret '{secret_name}'")
                    break
            else:
                error_msg = f"No enabled versions found for secret {secret_name}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        name = f"projects/{self.project_id}/secrets/{secret_full_name}/versions/{version_id}"

        self.logger.debug(f"Accessing secret version '{version_id}' of secret '{secret_name}'")
        response = self.client.access_secret_version(request={"name": name})

        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)

        if int(crc32c.hexdigest(), 16) != response.payload.data_crc32c:
            error_msg = "CRC32C checksum mismatch. The data may be corrupted."
            self.logger.error(f"{error_msg} for secret '{secret_name}' version '{version_id}'")
            raise ValueError(error_msg)

        self.logger.info(f"Successfully retrieved version '{version_id}' of secret '{secret_name}'")
        return response.payload.data

    def disable_secret_version(self, secret_name: str, version_id: str = "oldest") -> None:
        """
        Disables a specific version of a secret. If the version is "latest", it disables the latest enabled version.

        Args:
            secret_name (str): The name of the secret from which to delete the version.
            version_id (str): The version ID to delete. Defaults to "oldest", "latest" can also be used.
        """
        self.logger.info(f"Disabling version '{version_id}' of secret '{secret_name}'")
        secret_full_name = f"{self.secret_name_prefix}-{secret_name}"

        if secret_full_name not in self.secrets_names:
            self.logger.warning(f"Attempt to disable version of non-existent secret '{secret_name}'")
            return

        parent = self.client.secret_path(self.project_id, secret_full_name)
        versions = self.client.list_secret_versions(request={"parent": parent})

        if version_id == "latest":
            self.logger.debug(f"Finding latest enabled version to disable for secret '{secret_name}'")
            for version in versions:
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    self.logger.debug(f"Found latest enabled version '{version_id}' for secret '{secret_name}'")
                    break
            else:
                error_msg = f"No enabled versions found for secret {secret_name}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
        elif version_id == "oldest":
            self.logger.debug(f"Finding oldest enabled version to disable for secret '{secret_name}'")
            for version in reversed(list(versions)):
                if version.state == secretmanager.SecretVersion.State.ENABLED:
                    version_id = version.name.split("/")[-1]
                    self.logger.debug(f"Found oldest enabled version '{version_id}' for secret '{secret_name}'")
                    break
            else:
                error_msg = f"No enabled versions found for secret {secret_name}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            self.logger.debug(f"Verifying version '{version_id}' exists for secret '{secret_name}'")
            version_exists = any(version.name.split("/")[-1] == version_id for version in versions)
            if not version_exists:
                error_msg = f"Version {version_id} does not exist for secret {secret_name}."
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        name = f"projects/{self.project_id}/secrets/{secret_full_name}/versions/{version_id}"
        self.logger.debug(f"Disabling version '{version_id}' of secret '{secret_name}'")
        response = self.client.disable_secret_version(request={"name": name})

        if response.name.split("/")[-1] != version_id and response.state != secretmanager.SecretVersion.State.DISABLED:
            error_msg = f"Failed to disable secret version {version_id} for secret {secret_name}."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info(f"Successfully disabled version '{version_id}' of secret '{secret_name}'")

if __name__ == "__main__":
    config = load_config()
    secret_service = SecretService(config)

    # Show where logs are being stored
    log_path = secret_service.get_log_file_path()
    if log_path:
        print(f"Logs are being stored at: {log_path}")
    else:
        print("GCS logging is not available - logs will only appear in console")

    try:
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
        
    finally:
        # Ensure all logs are flushed to GCS before exiting
        secret_service.flush_logs()
        if log_path:
            print(f"All logs have been saved to: {log_path}")
        else:
            print("Logs were only written to console")



