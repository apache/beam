# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import sys
import yaml
import argparse
import os
from typing import List, Dict, TypedDict, Optional
from google.cloud import secretmanager
from google.cloud import iam_admin_v1
from google.cloud.iam_admin_v1 import types
from sending import SendingClient

SECRET_MANAGER_LABEL = "beam-infra-secret-manager"

class AuthorizedUser(TypedDict):
    email: str

class ServiceAccount(TypedDict):
    account_id: str
    display_name: str
    authorized_users: List[AuthorizedUser]

class ServiceAccountsConfig(TypedDict):
    service_accounts: List[ServiceAccount]

CONFIG_FILE = "config.yml"

class AccountKeysPolicyComplianceCheck:
    def __init__(self, project_id: str, service_account_keys_file: str, logger: logging.Logger, sending_client: Optional[SendingClient] = None):
        self.project_id = project_id
        self.service_account_keys_file = service_account_keys_file
        self.logger = logger
        self.sending_client = sending_client
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.service_account_client = iam_admin_v1.IAMClient()

    def _normalize_account_email(self, account_id: str) -> str:
        """
        Normalizes the account identifier to a full email format.
        
        Args:
            account_id (str): The unique identifier or email of the service account.
            
        Returns:
            str: The full service account email address.
        """
        if "@" in account_id:
            return account_id
        else:
            return f"{account_id}@{self.project_id}.iam.gserviceaccount.com"

    def _denormalize_account_email(self, email: str) -> str:
        """
        Denormalizes the full service account email address to its unique identifier.

        Args:
            email (str): The full service account email address.

        Returns:
            str: The unique identifier for the service account.
        """
        if email.endswith(f"@{self.project_id}.iam.gserviceaccount.com"):
            return email.split("@")[0]
        return email

    def _normalize_username(self, username: str) -> str:
        """
        Normalizes the username to a consistent format.

        Args:
            username (str): The username to normalize.

        Returns:
            str: The normalized username.
        """
        if not username.startswith("user:"):
            return f"user:{username.strip().lower()}"
        return username
    
    def _denormalize_username(self, username: str) -> str:
        """
        Denormalizes the username from the consistent format.

        Args:
            username (str): The normalized username.

        Returns:
            str: The denormalized username.
        """
        if username.startswith("user:"):
            return username.split(":", 1)[1].strip().lower()
        return username

    def _get_all_live_service_accounts(self) -> List[str]:
        """
        Retrieves all service accounts that are currently active (not disabled) in the project.

        Returns:
            List[str]: A list of email addresses for all live service accounts.
        """
        request = types.ListServiceAccountsRequest()
        request.name = f"projects/{self.project_id}"

        try:
            accounts = self.service_account_client.list_service_accounts(request=request)
            self.logger.debug(f"Retrieved {len(accounts.accounts)} service accounts for project {self.project_id}")

            if not accounts:
                self.logger.warning(f"No service accounts found in project {self.project_id}.")
                return []

            return [self._normalize_account_email(account.email) for account in accounts.accounts if not account.disabled]
        except Exception as e:
            self.logger.error(f"Failed to retrieve service accounts for project {self.project_id}: {e}")
            raise

    def _get_all_live_managed_secrets(self) -> List[str]:
        """
        Retrieves the list of secrets from the Secret Manager that where created by the beam-secret-service

        Returns:
            List[str]: A list of secret ids 
        """
        try:
            secrets = list(self.secret_client.list_secrets(request={"parent": f"projects/{self.project_id}"}))
            self.logger.debug(f"Retrieved {len(secrets)} secrets for project {self.project_id}")

            if not secrets:
                self.logger.warning(f"No secrets found in project {self.project_id}.")
                return []

            return [secret.name.split("/")[-1] for secret in secrets if "created_by" in secret.labels and secret.labels["created_by"] == SECRET_MANAGER_LABEL]
        except Exception as e:
            self.logger.error(f"Failed to retrieve secrets for project {self.project_id}: {e}")
            raise

    def _get_all_secret_authorized_users(self, secret_id: str) -> List[str]:
        """
        Retrieves a list of all users who have access to the secrets in the Secret Manager.

        Args:
            secret_id (str): The ID of the secret to check access for.
        Returns:
            List[str]: A list of email addresses for all users authorized to access the secrets.
        """
        accessor_role = "roles/secretmanager.secretAccessor"
        resource_name = self.secret_client.secret_path(self.project_id, secret_id)

        try:
            policy = self.secret_client.get_iam_policy(request={"resource": resource_name})
            self.logger.debug(f"Retrieved IAM policy for secret '{secret_id}': {policy}")

            if not policy.bindings:
                self.logger.warning(f"No IAM bindings found for secret '{secret_id}'.")
                return []
            
            authorized_users = []
            for binding in policy.bindings:
                if binding.role == accessor_role:
                    for user in binding.members:
                        authorized_users.append(self._normalize_username(user))
            
            return authorized_users
        except Exception as e:
            self.logger.error(f"Failed to get IAM policy for secret '{secret_id}': {e}")
            raise

    def _read_service_account_keys(self) -> ServiceAccountsConfig:
        """
        Reads the service account keys from a YAML file and returns a list of ServiceAccount objects.

        Returns:
            List[ServiceAccount]: A list of service account declarations.
        """
        try:
            with open(self.service_account_keys_file, "r") as file:
                keys = yaml.safe_load(file)

                if not keys or keys.get("service_accounts") is None:
                    return {"service_accounts": []}

                return keys
        except FileNotFoundError:
            self.logger.info(f"Service account keys file {self.service_account_keys_file} not found, starting with empty configuration")
            return {"service_accounts": []}
        except IOError as e:
            error_msg = f"Failed to read service account keys from {self.service_account_keys_file}: {e}"
            self.logger.error(error_msg)
            raise

    def _to_yaml_file(self, data: List[ServiceAccount], output_file: str, header_info: str = "") -> None:
        """
        Writes a list of dictionaries to a YAML file.
        Include the apache license header on the files

        Args:
            data: A list of dictionaries containing user permissions and details.
            output_file: The file path where the YAML output will be written.
            header_info: A string containing the header information to be included in the YAML file.
        """

        apache_license_header = """# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
    """

        # Prepare the header with the Apache license
        header = f"{apache_license_header}\n# {header_info}\n# Generated on {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

        try:
            with open(output_file, "w") as file:
                file.write(header)
                yaml_data = {"service_accounts": data}
                yaml.dump(yaml_data, file, sort_keys=False, default_flow_style=False, indent=2)
            self.logger.info(f"Successfully wrote Service Account Keys policy data to {output_file}")
        except IOError as e:
            self.logger.error(f"Failed to write to {output_file}: {e}")
            

    def check_compliance(self) -> List[str]:
        """
        Checks the compliance of service account keys with the defined policies.

        Returns:
            List[str]: A list of compliance issue messages.
        """

        service_account_data = self._read_service_account_keys()
        file_service_accounts = service_account_data.get("service_accounts")

        if not file_service_accounts:
            file_service_accounts = []
            self.logger.info(f"No service account keys found in the {self.service_account_keys_file}.")
        
        compliance_issues = []

        # Check that all service accounts that exist are declared
        for service_account in self._get_all_live_service_accounts():
            if self._denormalize_account_email(service_account) not in [account["account_id"] for account in file_service_accounts]:
                msg = f"Service account '{service_account}' is not declared in the service account keys file."
                compliance_issues.append(msg)
                self.logger.warning(msg)

        managed_secrets = self._get_all_live_managed_secrets()
        extracted_secrets = [f"{self._denormalize_account_email(account['account_id'])}-key" for account in file_service_accounts]

        # Check for managed secrets that are not declared
        for secret in managed_secrets:
            if secret not in extracted_secrets:
                msg = f"Managed secret '{secret}' is not declared in the service account keys file."
                compliance_issues.append(msg)
                self.logger.warning(msg)

        # Check for each managed secret if it has the correct permissions
        for account in file_service_accounts:
            secret_name = f"{self._denormalize_account_email(account['account_id'])}-key"
            if secret_name not in managed_secrets:
                # Skip accounts that don't have managed secrets
                continue
                
            authorized_users = [user["email"] for user in account["authorized_users"]]
            actual_users = [self._denormalize_username(user) for user in self._get_all_secret_authorized_users(secret_name)]
            
            # Sort both lists for proper comparison
            authorized_users.sort()
            actual_users.sort()
            
            if authorized_users != actual_users:
                msg = f"Managed secret '{account['account_id']}' does not have the correct permissions. Expected: {authorized_users}, Actual: {actual_users}"
                compliance_issues.append(msg)
                self.logger.warning(msg)

        return compliance_issues

    def create_announcement(self, recipient: str) -> None:
        """
        Creates an announcement about compliance issues using the SendingClient.

        Args:
            recipient (str): The email address of the announcement recipient.
        """
        if not self.sending_client:
            raise ValueError("SendingClient is required for creating announcements")
            
        diff = self.check_compliance()

        if not diff:
            self.logger.info("No compliance issues found, no announcement will be created.")
            return  

        title = f"Account Keys Compliance Issue Detected"
        body = f"Account keys for project {self.project_id} are not compliant with the defined policies on {self.service_account_keys_file}\n\n"
        for issue in diff:
            body += f"- {issue}\n"

        announcement = f"Dear team,\n\nThis is an automated notification about compliance issues detected in the Account Keys policy for project {self.project_id}.\n\n"
        announcement += f"We found {len(diff)} compliance issue(s) that need your attention.\n"
        announcement += f"\nPlease check the GitHub issue for detailed information and take appropriate action to resolve these compliance violations."

        self.sending_client.create_announcement(title, body, recipient, announcement)

    def print_announcement(self, recipient: str) -> None:
        """
        Prints announcement details instead of sending them (for testing purposes).
        Args:
            recipient (str): The email address of the announcement recipient.
        """
        if not self.sending_client:
            raise ValueError("SendingClient is required for printing announcements")
            
        diff = self.check_compliance()

        if not diff:
            self.logger.info("No compliance issues found, no announcement will be printed.")
            return

        title = f"Account Keys Compliance Issue Detected"
        body = f"Account keys for project {self.project_id} are not compliant with the defined policies on {self.service_account_keys_file}\n\n"
        for issue in diff:
            body += f"- {issue}\n"

        announcement = f"Dear team,\n\nThis is an automated notification about compliance issues detected in the Account Keys policy for project {self.project_id}.\n\n"
        announcement += f"We found {len(diff)} compliance issue(s) that need your attention.\n"
        announcement += f"\nPlease check the GitHub issue for detailed information and take appropriate action to resolve these compliance violations."

        self.sending_client.print_announcement(title, body, recipient, announcement)

    def generate_compliance(self) -> None:
        """
        Modifies the service account keys file to match the current state of service accounts and secrets.
        It will just add the non managed service accounts.
        """

        service_account_data = self._read_service_account_keys()
        file_service_accounts = service_account_data.get("service_accounts", [])
        
        # Ensure file_service_accounts is a list
        if file_service_accounts is None:
            file_service_accounts = []

        self.logger.info(f"Found {len(file_service_accounts)} existing service accounts in the keys file")
        
        # Check that all service accounts that exist are declared, if not, add them
        for service_account in self._get_all_live_service_accounts():
            if self._denormalize_account_email(service_account) not in [account["account_id"] for account in file_service_accounts]:
                self.logger.info(f"Service account '{service_account}' is not declared in the service account keys file, adding it")
                file_service_accounts.append({
                    "account_id": self._denormalize_account_email(service_account),
                    "display_name": service_account,
                    "authorized_users": []
                })

        managed_secrets = self._get_all_live_managed_secrets()
        extracted_secrets = [f"{self._denormalize_account_email(account['account_id'])}-key" for account in file_service_accounts]

        # Check for managed secrets that are not declared, if not, add them
        for secret in managed_secrets:
            if secret not in extracted_secrets:
                self.logger.info(f"Managed secret '{secret}' is not declared in the service account keys file, adding it")
                file_service_accounts.append({
                    "account_id": secret.strip("-key"),
                    "display_name": self._normalize_account_email(secret.strip("-key")),
                    "authorized_users": []
                })

        # Check for each managed secret if it has the correct permissions
        for account in file_service_accounts:
            secret_name = f"{self._denormalize_account_email(account['account_id'])}-key"
            if secret_name not in managed_secrets:
                continue

            authorized_users = sorted([user["email"] for user in account["authorized_users"]])

            if not authorized_users:
                self.logger.info(f"Managed secret '{account}' is new, skipping permission check")
                continue

            actual_users_normalized = sorted(self._get_all_secret_authorized_users(secret_name))
            actual_users = sorted([self._denormalize_username(user) for user in actual_users_normalized])

            if authorized_users != actual_users:
                self.logger.info(f"Managed secret '{account}' does not have the correct permissions, updating it")
                account["authorized_users"] = [{"email": user} for user in actual_users]

        # Remove duplicates based on account_id
        seen_accounts = set()
        deduplicated_accounts = []
        for account in file_service_accounts:
            if account["account_id"] not in seen_accounts:
                seen_accounts.add(account["account_id"])
                deduplicated_accounts.append(account)
            else:
                self.logger.info(f"Removing duplicate entry for account '{account['account_id']}'")

        self._to_yaml_file(deduplicated_accounts, self.service_account_keys_file, header_info="Service Account Keys")

def config_process() -> Dict[str, str]:
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError("Configuration file is empty or invalid.")
    
    config_res = dict()

    config_res["project_id"] = config.get("project_id", "apache-beam-testing")
    config_res["logging_level"] = config.get("logging", {}).get("level", "INFO")
    config_res["logging_format"] = config.get("logging", {}).get("format", "[%(asctime)s] %(levelname)s: %(message)s")
    config_res["service_account_keys_file"] = config.get("service_account_keys_file", "../keys/keys.yaml")
    config_res["action"] = config.get("action", "check")

    # SendingClient configuration
    config_res["github_token"] = os.getenv("GITHUB_TOKEN", "")
    config_res["github_repo"] = os.getenv("GITHUB_REPOSITORY", "apache/beam")
    config_res["smtp_server"] = os.getenv("SMTP_SERVER", "")
    config_res["smtp_port"] = os.getenv("SMTP_PORT", 587)
    config_res["email"] = os.getenv("EMAIL_ADDRESS", "")
    config_res["password"] = os.getenv("EMAIL_PASSWORD", "")
    config_res["recipient"] = os.getenv("EMAIL_RECIPIENT", "")

    return config_res

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Account Keys Compliance Checker")
    parser.add_argument("--action", choices=["check", "announce", "print", "generate"], 
                       help="Action to perform: check compliance, create announcement, print announcement, or generate new compliance")
    args = parser.parse_args()

    config = config_process()

    # Command line argument takes precedence over config file
    action = args.action if args.action else config.get("action", "check")

    logging.basicConfig(level=getattr(logging, config["logging_level"].upper(), logging.INFO),
                        format=config["logging_format"])
    logger = logging.getLogger("AccountKeysPolicyComplianceCheck")

    # Create SendingClient if needed for announcement actions
    sending_client = None
    if action in ["announce", "print"]:
        try:
            # Provide default values for testing, especially for print action
            github_token = config["github_token"] or "dummy-token"
            github_repo = config["github_repo"] or "dummy/repo"
            smtp_server = config["smtp_server"] or "dummy-server"
            smtp_port = int(config["smtp_port"]) if config["smtp_port"] else 587
            email = config["email"] or "dummy@example.com"
            password = config["password"] or "dummy-password"
            
            sending_client = SendingClient(
                logger=logger,
                github_token=github_token,
                github_repo=github_repo,
                smtp_server=smtp_server,
                smtp_port=smtp_port,
                email=email,
                password=password
            )
        except Exception as e:
            logger.error(f"Failed to initialize SendingClient: {e}")
            return 1

    logger.info(f"Starting Account Keys policy compliance check with action: {action}")
    account_keys_checker = AccountKeysPolicyComplianceCheck(config["project_id"], config["service_account_keys_file"], logger, sending_client)

    try:
        if action == "check":
            compliance_issues = account_keys_checker.check_compliance()
            if compliance_issues:
                logger.warning("Account Keys policy compliance issues found:")
                for issue in compliance_issues:
                    logger.warning(issue)
            else:
                logger.info("Account Keys policy is compliant.")
        elif action == "announce":
            logger.info("Creating announcement for compliance violations...")
            recipient = config["recipient"] or "admin@example.com"
            account_keys_checker.create_announcement(recipient)
        elif action == "print":
            logger.info("Printing announcement for compliance violations...")
            recipient = config["recipient"] or "admin@example.com"
            account_keys_checker.print_announcement(recipient)
        elif action == "generate":
            logger.info("Generating new compliance based on current Account Keys policy...")
            account_keys_checker.generate_compliance()
        else:
            logger.error(f"Unknown action: {action}")
            return 1
    except Exception as e:
        logger.error(f"Error executing action '{action}': {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
