
import traceback
import yaml
import logging
import argparse
import sys
from typing import List, TypedDict
# Importing custom modules
from gcp_logger import GCSLogHandler, GCPLogger
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
        config = yaml.safe_load(f)

    if not config:
        raise ValueError("Configuration file is empty or invalid.")
    
    required_keys = ['project_id', 'rotation_interval', 'max_versions_to_keep', 'bucket_name', 'log_file_prefix']
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
    
    if not isinstance(config['rotation_interval'], int) or config['rotation_interval'] <= 0:
        raise ValueError("Configuration 'rotation_interval' must be a positive integer.")
    if not isinstance(config['max_versions_to_keep'], int) or config['max_versions_to_keep'] <= 0:
        raise ValueError("Configuration 'max_versions_to_keep' must be a positive integer.")
    if not isinstance(config['bucket_name'], str) or not config['bucket_name'].strip():
        raise ValueError("Configuration 'bucket_name' must be a non-empty string.")
    if not isinstance(config['log_file_prefix'], str) or not config['log_file_prefix'].strip():
        raise ValueError("Configuration 'log_file_prefix' must be a non-empty string.")
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
  python main.py                        # Initialize service accounts and setup
  python main.py --cron                 # Run key rotation for accounts that need it
  python main.py --get-key my-sa        # Get the latest key for service account 'my-sa'
  python main.py --generate-key my-sa   # Manually rotate key for service account 'my-sa'
        """
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--cron',
        action='store_true',
        help='Run the cron job to rotate keys that require rotation'
    )
    group.add_argument(
        '--get-key',
        metavar='ACCOUNT_ID',
        type=str,
        help='Get the latest key for the specified service account ID'
    )
    group.add_argument(
        '--generate-key',
        metavar='ACCOUNT_ID',
        type=str,
        help='Manually rotate (generate new) key for the specified service account ID'
    )
    
    return parser.parse_args()

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
        logging_level = config['logging_level']

        self.service_accounts = service_accounts_config['service_accounts']

        self.logger = GCPLogger("KeyService", logging_level, bucket_name, log_file_prefix, self.project_id)
        self.secret_manager_client = SecretManager(self.project_id, self.logger, rotation_interval, max_versions_to_keep)
        self.service_account_manager = ServiceAccountManager(self.project_id, self.logger)

        self._start_all_service_accounts()
        self.logger.info(f"Initialized KeyService for project: {self.project_id}")

    def __del__(self) -> None:
        """Manually flush all logs to Google Cloud Storage."""
        try:
            for handler in self.logger.handlers:
                if isinstance(handler, GCSLogHandler):
                    handler.flush_to_gcs()
        except Exception:
            pass

    def cleanup(self) -> None:
        """Explicit cleanup method to flush logs and close resources."""
        try:
            self.logger.info("KeyService cleanup: Flushing logs to Google Cloud Storage")
            for handler in self.logger.handlers:
                if isinstance(handler, GCSLogHandler):
                    handler.flush_to_gcs()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def _start_all_service_accounts(self) -> None:
        """
        Reads the service accounts configuration and checks for service accounts.

        1. If a service account does not exist, it will be created.
        2. If a secret does not exist, it will be created.
        3. If a service account does not have a key, it will be created.
        """

        self.logger.debug("Creating service accounts if they do not exist")
        for account in self.service_accounts:
            account_id = account['account_id']
            try:
                # Check if the service account already exists
                if not self.service_account_manager._service_account_exists(account_id):
                    self.logger.debug(f"Service account {account_id} does not exist, creating it")
                    display_name = account['display_name']
                    self.logger.info(f"Creating service account: {account_id}")
                    self.service_account_manager.create_service_account(account_id, display_name)

                # Check if the secret for the service account exists
                secret_name = f"{account_id}-key"
                if not self.secret_manager_client._secret_exists(secret_name):
                    self.logger.debug(f"Secret {secret_name} does not exist, creating it")
                    self.logger.info(f"Creating secret for service account: {account_id}")
                    self.secret_manager_client.create_secret(secret_name)

                self._start_service_account_key(account_id)

                # Check if the service account key exists
            except Exception as e:
                self.logger.error(f"Error creating service account or secret for {account_id}: {e}")

    def _start_service_account_key(self, account_id: str) -> None:
        """
        Creates a service account key for a given service account during initialization.
        This method ensures that each service account has at least one key available.
        During initialization, we always create a key if none exists in Secret Manager.
        
        Args:
            account_id (str): The ID of the service account to create a key for.
        """
        self.logger.debug(f"Starting service account key for {account_id}")
        try:
            if not self.service_account_manager._service_account_exists(account_id):
                self.logger.error(f"Service account {account_id} does not exist, cannot create key")
                return
            
            # Check if a key exists in Secret Manager
            secret_name = f"{account_id}-key"
            try:
                existing_secret = self.secret_manager_client.get_secret_version(secret_name, "latest")
                if existing_secret:
                    self.logger.debug(f"Service account {account_id} already has a key in Secret Manager, skipping key creation.")
                    return
            except Exception:
                self.logger.debug(f"No existing key found in Secret Manager for {account_id}")
            
            # If no key exists in Secret Manager, create a new key
            self.logger.debug(f"Creating service account key for {account_id}")
            self.create_and_save_service_account_key(account_id)
            self.logger.debug(f"Service account key for {account_id} created and saved successfully.")

        except Exception as e:
            self.logger.error(f"Error creating service account key for {account_id}: {e}")

    def _cron_job(self) -> None:
        """
        Cron job to rotate service account keys and secrets.
        This method should be called periodically based on the rotation interval.
        It will rotate keys that have not been rotated in the last rotation_interval days.
        """

        self.logger.info("Starting cron job for service account key rotation")
        for account in self.service_accounts:
            account_id = account['account_id']
            try:
                secret_name = f"{account_id}-key"
                # Check if the service account is due for key rotation
                if self.secret_manager_client._is_key_rotation_due(secret_name):
                    self.logger.info(f"Rotating service account key for {account_id}")
                    self.rotate_service_account_key(account_id)
                else:
                    self.logger.debug(f"Service account key for {account_id} is not due for rotation")
            except Exception as e:
                self.logger.error(f"Error during cron job for service account {account_id}: {e}")

    def create_and_save_service_account_key(self, account_id: str) -> None:
        """
        Creates a service account key and saves it to the Secret Manager.
        
        Args:
            account_id (str): The ID of the service account to create a key for.
        """
        self.logger.info(f"Creating and saving service account key for {account_id}")
        try:
            # Verify service account exists and is enabled
            if not self.service_account_manager._service_account_exists(account_id):
                raise ValueError(f"Service account {account_id} does not exist")
            
            # Create the service account key
            key = self.service_account_manager.create_service_account_key(account_id)

            if key:
                secret_name = f"{account_id}-key"
                self.logger.info(f"Saving service account key for {account_id} to Secret Manager")
                self.secret_manager_client.add_secret_version(secret_name, key.private_key_data)
                self.logger.info(f"Successfully created and saved service account key for {account_id}")
            else:
                self.logger.warning(f"No key created for service account {account_id}.")
                raise ValueError(f"Failed to create key for service account {account_id}")
        except Exception as e:
            self.logger.error(f"Error creating and saving service account key for {account_id}: {e}")
            raise

    def delete_and_disable_oldest_service_account_key(self, account_id: str) -> None:
        """
        Disables the oldest secret version and deletes the oldest service account key for a given service account.
        Ensures that at least one key remains active.
        
        Args:
            account_id (str): The ID of the service account to delete the oldest key for.
        """
        self.logger.info(f"Deleting and disabling oldest service account key for {account_id}")
        try:
            secret_name = f"{account_id}-key"
            # First, delete old service account keys (keeping 1 newest)
            self.service_account_manager.delete_oldest_service_account_keys(account_id, max_keys=1)
            # Then disable old secret versions
            self.secret_manager_client.disable_secret_version(secret_name, "oldest")
        except Exception as e:
            self.logger.error(f"Error deleting and disabling oldest service account key for {account_id}: {e}")

    def rotate_service_account_key(self, account_id: str) -> None:
        """
        Rotates the service account key for a given service account.
        This process creates a new key first, then safely removes old keys.
        
        Args:
            account_id (str): The ID of the service account to rotate the key for.
        """
        self.logger.info(f"Rotating service account key for {account_id}")
        try:
            # Check if service account exists before attempting rotation
            if not self.service_account_manager._service_account_exists(account_id):
                raise ValueError(f"Service account {account_id} does not exist")
            
            # Create new key first to ensure we always have at least one working key
            self.logger.info(f"Creating new service account key for {account_id}")
            self.create_and_save_service_account_key(account_id)
            
            # Only delete old keys after the new key is successfully created and saved
            self.logger.info(f"Cleaning up old keys for {account_id}")
            self.delete_and_disable_oldest_service_account_key(account_id)
            
            self.logger.info(f"Successfully rotated service account key for {account_id}")
        except Exception as e:
            self.logger.error(f"Error rotating service account key for {account_id}: {e}")
            raise

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
            key_bytes = self.secret_manager_client.get_secret_version(secret_name, "latest")
            if not key_bytes:
                self.logger.warning(f"No key found for service account {account_id}.")
                raise ValueError(f"No key found for service account {account_id}.")

            self.logger.debug(f"Latest service account key for {account_id} retrieved successfully.")
            return key_bytes.decode('utf-8')
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
        
        key_service = KeyService(config, service_accounts_config)
        
        if args.cron:
            print("Running cron job for key rotation...")
            key_service._cron_job()
            print("Cron job completed successfully.")
            
        elif args.get_key:
            account_id = args.get_key
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
            except Exception as e:
                print(f"Error retrieving key for {account_id}: {e}")
                sys.exit(1)
                
        elif args.generate_key:
            account_id = args.generate_key
            print(f"Manually rotating key for service account: {account_id}")
            
            # Validate that the account exists in configuration
            account_ids = [account['account_id'] for account in service_accounts_config['service_accounts']]
            if account_id not in account_ids:
                print(f"Error: Service account '{account_id}' not found in configuration.")
                print(f"Available accounts: {', '.join(account_ids)}")
                sys.exit(1)
            
            try:
                key_service.rotate_service_account_key(account_id)
                print(f"Key rotation completed successfully for service account: {account_id}")
                
                print("\nRetrieving the new key...")
                key = key_service.get_latest_service_account_key(account_id)
                if key:
                    print(f"New key for {account_id}:")
                    print(key)
                else:
                    print(f"Warning: Could not retrieve the new key for {account_id}")
                    
            except Exception as e:
                print(f"Error rotating key for {account_id}: {e}")
                sys.exit(1)
                
        else:
            # Default behavior: initialize service accounts and setup
            print("KeyService initialized successfully.")
            print("Service accounts and secrets have been set up.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        if key_service is not None:
            try:
                key_service.cleanup()
            except:
                pass
        sys.exit(1)

if __name__ == "__main__":
    main()