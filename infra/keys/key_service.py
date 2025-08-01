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

import io
import logging
import yaml
from base64 import b64encode, b64decode
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from typing import List, TypedDict

from secret_manager import SecretManager
from service_account import ServiceAccountManager


# --- Configuration ---
CONFIG_FILE = 'config.yaml'
KEYS_FILE = 'keys.yaml'

class ConfigDict(TypedDict):
    project_id: str
    rotation_interval: int
    max_versions_to_keep: int
    bucket_name: str
    log_file_prefix: str
    logging_level: str

class AuthorizedUser(TypedDict):
    email: str

class ServiceAccount(TypedDict):
    account_id: str
    display_name: str
    authorized_users: List[AuthorizedUser]

class ServiceAccountsConfig(TypedDict):
    service_accounts: List[ServiceAccount]

def load_config() -> ConfigDict:
    """Loads the configuration from the YAML file."""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)
    
def load_service_accounts_config() -> ServiceAccountsConfig:
    """Loads the service accounts configuration from the YAML file."""
    with open(KEYS_FILE, 'r') as f:
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


class KeyService:
    """Service to manage GCP API keys rotation."""

    # Configuration
    project_id: str
    service_accounts: List[ServiceAccount]

    # Clients
    secret_manager_client: SecretManager
    service_account_manager: ServiceAccountManager
    logger: logging.Logger

    def __init__(self, config: ConfigDict, service_accounts_config: ServiceAccountsConfig) -> None:
        """
        Initializes the KeyService with the provided configuration.

        Args:
            config (ConfigDict): Configuration dictionary containing:
                - project_id: GCP project ID
                - rotation_interval: Interval in days for secret rotation 
                - max_versions_to_keep: Maximum number of secret versions to keep
                - bucket_name: GCS bucket name for logging
                - log_file_prefix: Prefix for log file names
                - logging_level: Logging level (e.g., 'INFO', 'DEBUG')
        Raises:
            ValueError: If any required configuration parameter is missing.
        """

        self.project_id = config['project_id']
        rotation_interval = config['rotation_interval']
        max_versions_to_keep = config['max_versions_to_keep']
        bucket_name = config['bucket_name']
        log_file_prefix = config['log_file_prefix']
        logging_level = config.get('logging_level', 'INFO').upper()

        if not self.project_id.strip():
            raise ValueError("Configuration 'project_id' cannot be empty.")
        if rotation_interval <= 0:
            raise ValueError("Configuration 'rotation_interval' must be positive.")
        if max_versions_to_keep <= 0:
            raise ValueError("Configuration 'max_versions_to_keep' must be positive.")
        if not bucket_name.strip():
            raise ValueError("Configuration 'bucket_name' cannot be empty.")
        if not log_file_prefix.strip():
            raise ValueError("Configuration 'log_file_prefix' cannot be empty.")
        
        # Save the service accounts configuration
        self.service_accounts = service_accounts_config['service_accounts']

        # Initialize logger
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
        
        # Initialize SecretManager and ServiceAccountManager
        self.secret_manager_client = SecretManager(self.project_id, self.logger, rotation_interval, max_versions_to_keep)
        self.service_account_manager = ServiceAccountManager(self.project_id, self.logger)

        self.logger.info(f"Initialized KeyService for project: {self.project_id}")

    def __del__(self) -> None:
        """Manually flush all logs to Google Cloud Storage."""
        for handler in self.logger.handlers:
            if isinstance(handler, GCSLogHandler):
                handler.flush_to_gcs()
                self.logger.info("Logs flushed to Google Cloud Storage")

    def rotate_all_secrets(self) -> None:
        """
        Rotate secrets in the secret manager.
        
        1. From the configuration, fetch all service accounts.
        2. For each service account, check if it exists, if not, create it.
        3. For each service account, check if it has a secret, if not, create one.
        4. Rotate it, the payload is a new service account key.
        """
        self.logger.info("Starting secret rotation process...")

        # Load service accounts from configuration
        for service_account in self.service_accounts:
            account_id = service_account['account_id']
            display_name = service_account['display_name']

            self.logger.info(f"Processing service account: {account_id} ({display_name})")

            # Check if service account exists, if not create it
            service_account_email = f"{account_id}@{self.project_id}.iam.gserviceaccount.com"
            for account in self.service_account_manager.list_service_accounts():
                self.logger.debug(f"Checking existing service account: {account.email}")
                if account.email == service_account_email:
                    self.logger.info(f"Service account {account_id} already exists.")
                    break
            else:
                self.logger.info(f"Creating service account: {account_id} ({display_name})")
                self.service_account_manager.create_service_account(account_id, display_name)

            # Check if secret exists, if not create it
            if not account_id in self.secret_manager_client.secrets_ids:
                self.logger.info(f"Creating secret for service account: {account_id}")
                secret_id = self.secret_manager_client.create_secret(account_id)

            # Rotate the secret
            self.logger.info(f"Rotating secret for service account: {account_id}")
            new_key = self.service_account_manager.create_service_account_key(service_account_email)
            # Extract the private key data from the service account key
            # The private_key_data is already base64 encoded JSON
            self.secret_manager_client.rotate_secret(account_id, new_key.private_key_data.decode('utf-8'))
            self.logger.info(f"Secret for service account {account_id} rotated successfully.")

        self.logger.info("Secret rotation process completed.")

    def cron_rotate_allsecrets(self) -> None:
        """
        Cron job to rotate secrets periodically.
        
        This will just rotate the secrets that are due for rotation based on the configured interval.
        
        1. Fetch all secrets from the secret manager.
        2. For each secret, check if it is due for rotation based on the configured interval.
        3. If due, rotate the secret.
        """
        self.logger.info("Starting cron job for secret rotation...")

        # Fetch all secrets from the secret manager
        secrets = self.secret_manager_client.secrets_ids

        for secret_id in secrets:
            self.logger.info(f"Checking secret: {secret_id}")

            secret = self.secret_manager_client.get_secret(secret_id)
            if secret:
                self.logger.info(f"Found secret: {secret_id}")
            else:
                self.logger.warning(f"Secret {secret_id} not found.")

            # Check if the secret is due for rotation
            last_version_created_at_datetime = datetime.strptime(secret.labels["last_version_created_at"], "%Y%m%d_%H%M%S")
            self.logger.debug(f"Last version created at: {last_version_created_at_datetime}")
            self.logger.debug(f"Current time: {datetime.now(timezone.utc)}")

            if datetime.now(timezone.utc) - last_version_created_at_datetime >= timedelta(days=self.secret_manager_client.rotation_interval):
                self.logger.info(f"Secret {secret_id} is due for rotation.")
                
                # Rotate the secret
                new_key = self.service_account_manager.create_service_account_key(secret_id)
                # Extract the private key data from the service account key
                # The private_key_data is already base64 encoded JSON
                self.secret_manager_client.rotate_secret(secret_id, new_key.private_key_data.decode('utf-8'))
                self.logger.info(f"Secret {secret_id} rotated successfully.")
            else:
                self.logger.info(f"Secret {secret_id} is not due for rotation yet. Skipping.")

def main():
    """
    Main function to run the KeyService.
    
    Loads configuration, initializes the KeyService, and starts the secret rotation process.
    """
    try:
        config = load_config()
        service_accounts_config = load_service_accounts_config()
        
        key_service = KeyService(config, service_accounts_config)
        key_service.rotate_all_secrets()
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()