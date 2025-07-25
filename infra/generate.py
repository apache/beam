#
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
#
# This script is used to export the IAM policy of a Google Cloud project to a YAML format.
# It retrieves the IAM policy bindings, parses the members, and formats the output in a structured
# YAML format, excluding service accounts and groups. The output includes usernames, emails, and
# their associated permissions, with optional conditions for roles that have conditions attached.
# You need to have the Google Cloud SDK installed and authenticated to run this script.

import argparse
import datetime
import yaml
import logging
from typing import Optional, List, Dict
from google.cloud import resourcemanager_v3
from google.api_core import exceptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_member(member: str) -> tuple[str, Optional[str], str]:
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
        email = None

    return username, email, member_type

def export_project_iam(project_id: str) -> List[Dict]:
    """Exports the IAM policy for a given project to YAML format.

    Args:
        project_id: The ID of the Google Cloud project.
    Returns:
        A list of dictionaries containing the IAM policy details.
    """

    try:
        client = resourcemanager_v3.ProjectsClient()
        policy = client.get_iam_policy(resource=f"projects/{project_id}")
        logger.info(f"Successfully retrieved IAM policy for project {project_id}")
    except exceptions.NotFound as e:
        logger.error(f"Project {project_id} not found: {e}")
        raise
    except exceptions.PermissionDenied as e:
        logger.error(f"Permission denied for project {project_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while retrieving IAM policy for project {project_id}: {e}")
        raise

    members_data = {}

    for binding in policy.bindings:
        role = binding.role

        for member_str in binding.members:
            if member_str not in members_data:
                username, email_address, member_type = parse_member(member_str)
                if member_type == "serviceAccount":
                    continue # Skip service accounts
                if member_type == "group":
                    continue  # Skip groups
                if not email_address:
                    continue # Skip if no email address is found, probably a malformed member
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

def to_yaml_file(data: List[Dict], output_file: str, header_info: str = "") -> None:
    """
    Writes a list of dictionaries to a YAML file.
    Include the apache license header on the files

    Args:
        data: A list of dictionaries containing user permissions and details.
        output_file: The file path where the YAML output will be written.
        header_info: A string containing the header information to be included in the YAML file.
    """

    apache_license_header = """#
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
"""

    # Prepare the header with the Apache license
    header = f"{apache_license_header}\n# {header_info}\n# Generated on {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

    try:
        with open(output_file, "w") as file:
            file.write(header)
            yaml.dump(data, file, sort_keys=False, default_flow_style=False, indent=2)
        logger.info(f"Successfully wrote IAM policy data to {output_file}")
    except IOError as e:
        logger.error(f"Failed to write to {output_file}: {e}")
        raise

def main():
    """
    Main function to run the script.

    This function parses command-line arguments to either export IAM policies
    or generate permission differences for a specified GCP project.
    """
    parser = argparse.ArgumentParser(
        description="Export IAM policies or generate permission differences for a GCP project."
    )
    parser.add_argument(
        "project_id",
        help="The Google Cloud project ID."
    )
    parser.add_argument(
        "output_file",
        help="Defaults to 'users.yml' if not specified. The file where the IAM policy will be saved in YAML format.",
        nargs='?',
        default="users.yml"
    )

    args = parser.parse_args()
    project_id = args.project_id
    output_file = args.output_file

    # Export the IAM policy for the specified project
    iam_data = export_project_iam(project_id)

    # Write the exported data to the specified output file in YAML format
    to_yaml_file(iam_data, output_file, header_info=f"Exported IAM policy for project {project_id}")

if __name__ == "__main__":
    main()