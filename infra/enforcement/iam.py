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

import argparse
import datetime
import logging
import os
import sys
import yaml
from google.api_core import exceptions
from google.cloud import resourcemanager_v3
from typing import Optional, List, Dict, Tuple
from sending import SendingClient

CONFIG_FILE = "config.yml"

class IAMPolicyComplianceChecker:
    def __init__(self, project_id: str, users_file: str, logger: logging.Logger, sending_client: Optional[SendingClient] = None):
        self.project_id = project_id
        self.users_file = users_file
        self.client = resourcemanager_v3.ProjectsClient()
        self.logger = logger
        self.sending_client = sending_client

    def _parse_member(self, member: str) -> tuple[str, Optional[str], str]:
        """Parses an IAM member string to extract type, email, and a derived username.

        Args:
            member: The IAM member string
        Returns:
            A tuple containing:
                - username: The derived username from the member string.
                - email: The email address if available, otherwise None.
                - member_type: The type of the member (e.g., user, serviceAccount, group).
        """
        email = None
        username = member

        # Split the member string to determine type and identifier
        parts = member.split(':', 1)
        member_type = parts[0] if len(parts) > 1 else "unknown"
        identifier = parts[1] if len(parts) > 1 else member

        if member_type in ["user", "serviceAccount", "group"]:
            email = identifier
            if '@' in identifier:
                username = identifier.split('@')[0]
            else:
                username = identifier
        else:
            username = identifier
            member_type = "unknown"
            email = None

        return username, email, member_type

    def _export_project_iam(self) -> List[Dict]:
        """Exports the IAM policy for a given project to YAML format.

        Returns:
            A list of dictionaries containing the IAM policy details.
        """

        try:
            policy = self.client.get_iam_policy(resource=f"projects/{self.project_id}")
            self.logger.debug(f"Retrieved IAM policy for project {self.project_id}")
        except exceptions.NotFound as e:
            self.logger.error(f"Project {self.project_id} not found: {e}")
            raise
        except exceptions.PermissionDenied as e:
            self.logger.error(f"Permission denied for project {self.project_id}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An error occurred while retrieving IAM policy for project {self.project_id}: {e}")
            raise

        members_data = {}

        for binding in policy.bindings:
            role = binding.role

            for member_str in binding.members:
                if member_str not in members_data:
                    username, email_address, member_type = self._parse_member(member_str)
                    if member_type == "unknown":
                        self.logger.warning(f"Skipping member {member_str} with no email address")
                        continue  # Skip if no email address is found, probably a malformed member
                    members_data[member_str] = {
                        "username": username,
                        "email": email_address,
                        "permissions": []
                    }

                # Skip permissions that have a condition
                if "withcond" in role:
                    continue

                permission_entry = {}
                permission_entry["role"] = role

                members_data[member_str]["permissions"].append(permission_entry)

        output_list = []
        for data in members_data.values():
            data["permissions"] = sorted(data["permissions"], key=lambda p: p["role"])
            output_list.append({
                "username": data["username"],
                "email": data["email"],
                "permissions": data["permissions"]
            })

        output_list.sort(key=lambda x: x["username"])
        return output_list

    def _read_project_iam_file(self) -> List[Dict]:
        """Reads the IAM policy from a YAML file.

        Returns:
            A list of dictionaries containing the IAM policy details.
        """
        try:
            with open(self.users_file, "r") as file:
                iam_policy = yaml.safe_load(file)


                self.logger.debug(f"Retrieved IAM policy from file for project {self.project_id}")
                return iam_policy
        except FileNotFoundError:
            self.logger.error(f"IAM policy file not found for project {self.project_id}")
            return []
        except Exception as e:
            self.logger.error(f"An error occurred while reading IAM policy file for project {self.project_id}: {e}")
            return []

    def _to_yaml_file(self, data: List[Dict], output_file: str, header_info: str = "") -> None:
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
                yaml.dump(data, file, sort_keys=False, default_flow_style=False, indent=2)
            self.logger.info(f"Successfully wrote IAM policy data to {output_file}")
        except IOError as e:
            self.logger.error(f"Failed to write to {output_file}: {e}")
            raise
        
    def check_compliance(self) -> List[str]:
        """
        Checks the compliance of the IAM policy against the defined policies.

        Returns:
            A list of strings describing any compliance issues found.
        """
        current_users = {user['email']: user for user in self._export_project_iam()}
        existing_users = {user['email']: user for user in self._read_project_iam_file()}

        if not existing_users:
            error_msg = f"No IAM policy found in the {self.users_file}."
            self.logger.info(error_msg)
            raise RuntimeError(error_msg)

        differences = []        

        all_emails = set(current_users.keys()) | set(existing_users.keys())

        for email in sorted(list(all_emails)):
            current_user = current_users.get(email)
            existing_user = existing_users.get(email)

            if current_user and not existing_user:
                differences.append(f"User {email} not found in existing policy.")
            elif not current_user and existing_user:
                differences.append(f"User {email} found in policy file but not in GCP.")
            elif current_user and existing_user:
                if current_user["permissions"] != existing_user["permissions"]:
                    msg = f"\nPermissions for user {email} differ."
                    msg += f"\nIn GCP: {current_user['permissions']}"
                    msg += f"\nIn {self.users_file}: {existing_user['permissions']}"
                    self.logger.info(msg)
                    differences.append(msg)

        return differences

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

        title = f"IAM Policy Non-Compliance Detected"
        body = f"IAM policy for project {self.project_id} is not compliant with the defined policies on {self.users_file}\n\n"
        for issue in diff:
            body += f"- {issue}\n"

        announcement = f"Dear team,\n\nThis is an automated notification about compliance issues detected in the IAM policy for project {self.project_id}.\n\n"
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

        title = f"IAM Policy Non-Compliance Detected"
        body = f"IAM policy for project {self.project_id} is not compliant with the defined policies on {self.users_file}\n\n"
        for issue in diff:
            body += f"- {issue}\n"

        announcement = f"Dear team,\n\nThis is an automated notification about compliance issues detected in the IAM policy for project {self.project_id}.\n\n"
        announcement += f"We found {len(diff)} compliance issue(s) that need your attention.\n"
        announcement += f"\nPlease check the GitHub issue for detailed information and take appropriate action to resolve these compliance violations."

        self.sending_client.print_announcement(title, body, recipient, announcement)
    
    def generate_compliance(self) -> None:
        """
        Modifies the users file to match the current IAM policy.
        If no changes are needed, no file will be written.
        """
        
        try:
            diff = self.check_compliance()
        except RuntimeError:
            self.logger.info("No existing IAM policy found.")
            diff = ["No existing policy found"]

        if not diff or (len(diff) == 1 and "No existing policy found" not in diff[0]):
            self.logger.info("No compliance issues found, no changes will be made.")
            return

        current_policy = self._export_project_iam()
        header_info = f"IAM policy for project {self.project_id}"
        
        self._to_yaml_file(current_policy, self.users_file, header_info)
        self.logger.info(f"Generated new compliance file: {self.users_file}")

def config_process() -> Dict[str, str]:
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError("Configuration file is empty or invalid.")
    
    config_res = dict()

    config_res["project_id"] = config.get("project_id", "apache-beam-testing")
    config_res["logging_level"] = config.get("logging", {}).get("level", "INFO")
    config_res["logging_format"] = config.get("logging", {}).get("format", "[%(asctime)s] %(levelname)s: %(message)s")
    config_res["users_file"] = config.get("users_file", "../iam/users.yml")
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
    parser = argparse.ArgumentParser(description="IAM Policy Compliance Checker")
    parser.add_argument("--action", choices=["check", "announce", "print", "generate"], 
                       help="Action to perform: check compliance, create announcement, print announcement, or generate new compliance")
    args = parser.parse_args()

    config = config_process()

    # Command line argument takes precedence over config file
    action = args.action if args.action else config.get("action", "check")

    logging.basicConfig(level=getattr(logging, config["logging_level"].upper(), logging.INFO),
                        format=config["logging_format"])
    logger = logging.getLogger("IAMPolicyComplianceChecker")

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

    logger.info(f"Starting IAM policy compliance check with action: {action}")
    iam_checker = IAMPolicyComplianceChecker(config["project_id"], config["users_file"], logger, sending_client)

    try:
        if action == "check":
            compliance_issues = iam_checker.check_compliance()
            if compliance_issues:
                logger.warning("IAM policy compliance issues found:")
                for issue in compliance_issues:
                    logger.warning(issue)
            else:
                logger.info("IAM policy is compliant.")
        elif action == "announce":
            logger.info("Creating announcement for compliance violations...")
            recipient = config["recipient"] or "admin@example.com"
            iam_checker.create_announcement(recipient)
        elif action == "print":
            logger.info("Printing announcement for compliance violations...")
            recipient = config["recipient"] or "admin@example.com"
            iam_checker.print_announcement(recipient)
        elif action == "generate":
            logger.info("Generating new compliance based on current IAM policy...")
            iam_checker.generate_compliance()
        else:
            logger.error(f"Unknown action: {action}")
            return 1
    except Exception as e:
        logger.error(f"Error executing action '{action}': {e}")
        return 1

    return 0

if __name__ == "__main__":
    
    sys.exit(main())
