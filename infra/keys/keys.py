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

import traceback
import yaml
import logging
import argparse
import sys
from typing import List, TypedDict
from google.api_core.exceptions import PermissionDenied
# Importing custom modules
from secret_manager import SecretManager
from service_account import ServiceAccountManager


# --- Configuration ---
CONFIG_FILE = 'config.yaml'
KEYS_FILE = 'keys.yaml'

class ConfigDict(TypedDict):
    project_id: str
    rotation_interval: int
    grace_period: int
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
        config = yaml.safe_load(f)

    if not config:
        raise ValueError("Configuration file is empty or invalid.")

    required_keys = set(['project_id', 'rotation_interval', 'grace_period'])
    missing_keys = required_keys - config.keys()
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    if not isinstance(config['rotation_interval'], int) or config['rotation_interval'] <= 0:
        raise ValueError("Configuration 'rotation_interval' must be a positive integer.")
    if not isinstance(config['grace_period'], int) or config['grace_period'] < 0:
        raise ValueError("Configuration 'grace_period' must be a non-negative integer.")
    if 'logging_level' in config:
        if not isinstance(config['logging_level'], str) or config['logging_level'].strip() not in logging._nameToLevel:
            raise ValueError("Configuration 'logging_level' must be one of: " + ", ".join(logging._nameToLevel.keys()))
    else:
        config['logging_level'] = 'INFO'

    return config

def load_service_accounts_config() -> ServiceAccountsConfig:
    """Loads the service accounts configuration from the YAML file."""
    with open(KEYS_FILE, 'r') as f:
        service_accounts_config = yaml.safe_load(f)

    if not service_accounts_config or 'service_accounts' not in service_accounts_config:
        raise ValueError("Service accounts configuration file is empty or invalid.")
    
    if not isinstance(service_accounts_config['service_accounts'], list):
        raise ValueError("Service accounts configuration must be a list of service accounts.")
    
    for account in service_accounts_config['service_accounts']:
        if 'account_id' not in account or 'display_name' not in account:
            raise ValueError("Each service account must have 'account_id' and 'display_name'.")
        if 'authorized_users' not in account or not isinstance(account['authorized_users'], list):
            raise ValueError("Each service account must have a list of 'authorized_users'.")
        
    return service_accounts_config

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="KeyService - GCP Service Account Key Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python keys.py --cron                 # Run key rotation for accounts that need it, ran only by cron job
  python keys.py --cron-dry-run         # Run a dry run of the key rotation cron job
  python keys.py --get-key my-sa        # Get the latest key for service account 'my-sa', ran by users
        """
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--cron',
        action='store_true',
        help='Run the cron job to rotate keys that require rotation'
    )
    group.add_argument(
        '--cron-dry-run',
        action='store_true',
        help='Run a dry run of the cron job to see what would be rotated'
    )
    group.add_argument(
        '--get-key',
        metavar='ACCOUNT_ID',
        type=str,
        help='Get the latest key for the specified service account ID'
    )
    
    return parser.parse_args()

class KeyService:
    """Service to manage GCP API keys rotation."""

    # Configuration
    project_id: str
    service_accounts: List[ServiceAccount]
    enable_logging: bool

    # Clients
    secret_manager_client: SecretManager
    service_account_manager: ServiceAccountManager
    logger: logging.Logger

    def __init__(self, config: ConfigDict, service_accounts_config: ServiceAccountsConfig, enable_logging: bool = True) -> None:
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
            service_accounts_config (ServiceAccountsConfig): Configuration for service accounts.
                - service_accounts: List of service accounts to manage and their configuration
            enable_logging (bool): Whether to enable logging. Defaults to True.
        Raises:
            ValueError: If any required configuration parameter is missing.
        """

        self.project_id = config['project_id']
        rotation_interval = config['rotation_interval']
        grace_period = config['grace_period']
        logging_level = config['logging_level']

        self.service_accounts = service_accounts_config['service_accounts']
        self.enable_logging = enable_logging

        self.logger = logging.getLogger("KeyService")
        if self.enable_logging:
            self.logger.setLevel(logging_level)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        else:
            # Create a null logger that doesn't actually log anything
            self.logger.setLevel(logging.CRITICAL + 1)  # Set to a level higher than CRITICAL to disable all logging

        self.secret_manager_client = SecretManager(self.project_id, self.logger, rotation_interval, grace_period)
        self.service_account_manager = ServiceAccountManager(self.project_id, self.logger)

        if self.enable_logging:
            self.logger.info(f"Initialized KeyService for project: {self.project_id}")

    def _start_all_service_accounts(self) -> None:
        """
        Reads the service accounts configuration and checks for service accounts.

        1. If a service account exists and is managed, it checks if the secret exists and updates access if needed.
        2. If the service account exists but the secret does not, it creates the secret and clears the service account
              keys as now keys will be managed by the Secret Manager.
        3. If neither the service account nor the secret exists, it creates and initializes both.
        4. If any other case is encountered, it logs an error and skips the account.
        """

        self.logger.debug("Creating service accounts if they do not exist")
        for account in self.service_accounts:
            account_id = account['account_id']
            authorized_users = [user['email'] for user in account.get('authorized_users', [])]

            try:
                secret_name = f"{account_id}-key"
                # If service account and secret exists and is managed, just check permissions
                if self.service_account_manager._service_account_exists(account_id) and self.secret_manager_client._secret_is_managed(secret_name):
                    self.logger.debug(f"Service account {account_id} and secret {secret_name} already exist and are managed")
                    if self.secret_manager_client.is_different_user_access(secret_name, authorized_users):
                        self.logger.debug(f"Updating access policy for secret {secret_name}")
                        self.secret_manager_client.update_secret_access(secret_name, authorized_users)

                # If the service account exists but the secret does not, create the secret and a key and ignore the existing keys
                elif self.service_account_manager._service_account_exists(account_id) and not self.secret_manager_client._secret_exists(secret_name):
                    self.logger.debug(f"Service account {account_id} exists but secret {secret_name} does not, creating secret and a new key")
                    self.secret_manager_client.create_secret(secret_name)
                    self.secret_manager_client.update_secret_access(secret_name, authorized_users)

                    new_key = self.service_account_manager.create_service_account_key(account_id)
                    new_key_id = new_key.name.split('/')[-1]
                    self.secret_manager_client.add_secret_version(secret_name, new_key_id, new_key.private_key_data)

                # If neither secret nor service account exists, create and initialize both
                elif not self.service_account_manager._service_account_exists(account_id) and not self.secret_manager_client._secret_exists(secret_name):
                    self.logger.debug(f"Service account {account_id} and secret {secret_name} do not exist, creating both")
                    display_name = account['display_name']
                    
                    self.service_account_manager.create_service_account(account_id, display_name)

                    secret_name = self.secret_manager_client.create_secret(secret_name)
                    self.secret_manager_client.update_secret_access(secret_name, authorized_users)

                    new_key = self.service_account_manager.create_service_account_key(account_id)
                    new_key_id = new_key.name.split('/')[-1]
                    self.secret_manager_client.add_secret_version(secret_name, new_key_id, new_key.private_key_data)

                else:
                    # Any other case is not supported
                    self.logger.error(f"Unexpected state for service account {account_id}")

            except Exception as e:
                self.logger.error(f"Error creating service account or secret for {account_id}: {e}")

    def cron(self, dry_run: bool = False) -> None:
        """
        Cron job to rotate service account keys and secrets.

        This method should be called periodically based on the rotation interval.
        It will:

        1. Check each service account to see if its key is due for rotation.
            1.1. If the key is due for rotation, it will rotate the key and update the secret in Secret Manager.
            1.2. If the key is not due for rotation, it will log that no action is needed.
        2. Check for keys that have expired the grace period and delete them from both the service account and Secret Manager.
        
        Args:
            dry_run (bool): If True, the method will only log the actions that would be taken.
        """

        if dry_run:
            self.logger.info("Starting cron job DRY RUN for service account key rotation")
        else:
            self.logger.info("Starting cron job for service account key rotation")
        
        if not dry_run:
            self._start_all_service_accounts()

        for account in self.service_accounts:
            account_id = account['account_id']
            secret_name = f"{account_id}-key"
            try:
                if self.secret_manager_client._is_key_rotation_due(secret_name):
                    if dry_run:
                        self.logger.info(f"[DRY RUN] Service account key for {account_id} is due for rotation, would rotate key.")
                    else:
                        self.logger.info(f"Service account key for {account_id} is due for rotation, rotating key")
                        new_key = self.service_account_manager.create_service_account_key(account_id)
                        new_key_id = new_key.name.split('/')[-1]
                        self.secret_manager_client.add_secret_version(secret_name, new_key_id, new_key.private_key_data)
                else:
                    self.logger.debug(f"Service account key for {account_id} is not due for rotation")
            except Exception as e:
                self.logger.error(f"Error during cron job for service account {account_id}: {e}")

        # Check for keys that have expired the grace period and delete them
        
        self.logger.info("Checking for keys that have expired the grace period")
        keys_to_delete = self.secret_manager_client.cron()
        for secret_id, key_ids in keys_to_delete:
            try:
                for key_id in key_ids:
                    if dry_run:
                        self.logger.info(f"[DRY RUN] Would delete expired key {key_id} for secret {secret_id}")
                    else:
                        self.logger.info(f"Deleting expired key {key_id} for secret {secret_id}")
                        self.service_account_manager.delete_service_account_key(secret_id, key_id)
            except Exception as e:
                self.logger.error(f"Error deleting expired keys for secret {secret_id}: {e}")
                continue

        if dry_run:
            self.logger.info("Cron job DRY RUN for service account key rotation completed")
        else:
            self.logger.info("Cron job for service account key rotation completed")

    def get_latest_service_account_key(self, account_id: str) -> str:
        """
        Retrieves the latest service account key for a given service account.
         
        Args:
            account_id (str): The ID of the service account to retrieve the key for.
        
        Returns:
            str: The latest service account key.
        """
        self.logger.info(f"Retrieving latest service account key for {account_id}")
        try:
            secret_name = f"{account_id}-key"
            key_bytes = self.secret_manager_client.get_latest_secret_version(secret_name)
            if not key_bytes:
                self.logger.warning(f"No key found for service account {account_id}.")
                raise ValueError(f"No key found for service account {account_id}.")

            self.logger.debug(f"Latest service account key for {account_id} retrieved successfully.")
            return key_bytes[1].decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error retrieving latest service account key for {account_id}: {e}")
            return ""

def main():
    """
    Main function to run the KeyService.
    
    Loads configuration, initializes the KeyService, and handles CLI arguments.
    """
    args = parse_arguments()
    
    key_service = None
    try:
        config = load_config()
        service_accounts_config = load_service_accounts_config()
        
        if args.cron or args.cron_dry_run:
            is_dry_run = args.cron_dry_run
            run_type = "dry run" if is_dry_run else "job"
            print(f"Running cron {run_type} for key rotation...")
            key_service = KeyService(config, service_accounts_config)
            key_service.cron(dry_run=is_dry_run)
            print(f"Cron {run_type} completed successfully.")
            
        elif args.get_key:
            account_id = args.get_key
            # If just a user getting the key, disable logging
            key_service = KeyService(config, service_accounts_config, enable_logging=False)
            print(f"Retrieving latest key for service account: {account_id}")
            
            # Validate that the account exists in configuration
            account_ids = [account['account_id'] for account in service_accounts_config['service_accounts']]
            if account_id not in account_ids:
                print(f"Error: Service account '{account_id}' not found in configuration.")
                print(f"Available accounts: {', '.join(account_ids)}")
                sys.exit(1)
            
            try:
                key = key_service.get_latest_service_account_key(account_id)
                if key:
                    print(f"Latest key for {account_id}:")
                    print(key)
                else:
                    print(f"No key found for service account: {account_id}")
                    sys.exit(1)
            except PermissionDenied as e:
                print(f"Permission denied when accessing the key for {account_id}: {e}")
                sys.exit(1)
            except Exception as e:
                print(f"Error retrieving key for {account_id}: {e}")
                sys.exit(1)
        else:
            print("You must specify either --cron to run the cron job or --get-key <ACCOUNT_ID> to retrieve a key.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()