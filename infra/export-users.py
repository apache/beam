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

import yaml
from google.cloud import resourcemanager_v3

def parse_member(member_str):
    """Parses an IAM member string to extract type, email, and a derived username.

    Args:
        member_str: The IAM member string
    Returns:
        A tuple containing the username, email address, and member type.
    """
    email = None
    username = member_str

    # Split the member string to determine type and identifier
    parts = member_str.split(':', 1)
    member_type = parts[0] if len(parts) > 1 else "unknown"
    identifier = parts[1] if len(parts) > 1 else member_str

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

def export_project_iam_to_yaml(project_id):
    """Exports the IAM policy for a given project to YAML format.

    Args:
        project_id: The ID of the Google Cloud project.
    Returns:
        A YAML formatted string containing the IAM policy details.
    """
    client = resourcemanager_v3.ProjectsClient()
    policy = client.get_iam_policy(resource=f"projects/{project_id}")

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
                members_data[member_str] = {
                    "username": username,
                    "email": email_address,
                    "permissions": []
                }

            permission_entry = {}

            permission_entry["role"] = role
            if "withcond" in role:
                permission_entry["request_description"] = "Add description"
                permission_entry["expiry_date"] = "YYYY-MM-DD"

            members_data[member_str]["permissions"].append(permission_entry)

    # Convert dictionary to the list format for YAML
    output_list = []
    for data in members_data.values():
        data["permissions"] = sorted(data["permissions"], key=lambda p: p["role"])
        output_list.append(data)

    # Sort the output list by username
    output_list.sort(key=lambda x: x["username"])

    final_output_list = []
    for item in output_list:
        final_output_list.append({
            "username": item["username"],
            "email": item["email"],
            "permissions": item["permissions"]
        })

    yaml_data = yaml.dump(final_output_list, sort_keys=False, default_flow_style=False, indent=2)
    return yaml_data

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python export-users.py <project_id> <output_file>")
        sys.exit(1)

    # Get the project ID to export from the arguments
    project_id_to_export = sys.argv[1]
    yaml_txt = export_project_iam_to_yaml(project_id_to_export)

    # Write the YAML output to the specified file
    with open(sys.argv[2], "w") as output_file:
        output_file.write(yaml_txt)