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
import os
import sys
import yaml
import roles.generate_roles as generate_roles
from generate import export_project_iam, to_yaml_file
from google.cloud.iam_admin_v1 import GetRoleRequest, IAMClient

def migrate_permissions(data: list) -> list:
    """
    Migrates permissions from the permissions to the new roles defined on beam_roles/ directory.

    The rules are:
    - If the user has owner role, leave it as is, remove any other role as it is redundant.
    - If the user has any admin or secret related role, it will be migrated to the beam_admin role.
    - If the user has an editor role or any user role but not an admin or secret related role, it will be migrated to the beam_infra_manager role.
    - If the user has a role that is not only viewer, it will be migrated to the beam_committer role.
    - The users with just viewer roles will be migrated to the beam_viewer role.

    The rules are in a hierarchical order, meaning that if a user has a high role, it will also have the lower roles.

    Args:
        data: A list of dictionaries containing user permissions and details.
    Returns:
        A list of dictionaries with migrated permissions.
    """

    migrated_data = []

    for item in data:
        username = item["username"]
        email = item["email"]
        permissions = item["permissions"]

        # Initialize the new roles
        new_roles = {
            "beam_owner": False,
            "beam_admin": False,
            "beam_infra_manager": False,
            "beam_committer": False,
            "beam_viewer": False
        }

        for permission in permissions:
            role = permission["role"]

            # If the role is 'roles/owner', it is considered an owner role.
            if role == "roles/owner":
                new_roles["beam_owner"] = True
            # If it ends with 'admin' or containes 'secretmanager' in the role, it is considered an admin role. Case insensitive.
            elif 'admin' in role.lower() or 'secretmanager' in role.lower():
                new_roles["beam_admin"] = True
                new_roles["beam_infra_manager"] = True
                new_roles["beam_committer"] = True
                new_roles["beam_viewer"] = True
            # If it is an editor role, it will be migrated to the beam_infra_manager.
            elif role == "roles/editor":
                new_roles["beam_infra_manager"] = True
                new_roles["beam_committer"] = True
                new_roles["beam_viewer"] = True
            elif role != "roles/viewer":
                # If it is a role that is not only viewer, it will be migrated to the beam_committer role.
                new_roles["beam_committer"] = True
                new_roles["beam_viewer"] = True
            # If it is a viewer role, it will be migrated to the beam_viewer role.
            else:
                new_roles["beam_viewer"] = True

        # Create the migrated entry
        migrated_entry = {
            "username": username,
            "email": email,
            "permissions": []
        }

        if new_roles["beam_owner"]:
            migrated_entry["permissions"].append({"role": "roles/owner"})
        else:
            if new_roles["beam_admin"]:
                migrated_entry["permissions"].append({"role": "projects/PROJECT-ID/roles/beam_admin"})
            if new_roles["beam_infra_manager"]:
                migrated_entry["permissions"].append({"role": "projects/PROJECT-ID/roles/beam_infra_manager"})
            if new_roles["beam_committer"]:
                migrated_entry["permissions"].append({"role": "projects/PROJECT-ID/roles/beam_committer"})
            if new_roles["beam_viewer"]:
                migrated_entry["permissions"].append({"role": "projects/PROJECT-ID/roles/beam_viewer"})

        migrated_data.append(migrated_entry)

    return migrated_data

def get_gcp_role_permissions(role_id: str) -> list:
    """
    Retrieves the permissions associated to a google cloud role.
    Args:
        project_id: The ID of the Google Cloud project.
        role_id: The name of the role to retrieve permissions for.
    Returns:
        A list of permissions associated with the specified role.
    """
    client = IAMClient()

    request = GetRoleRequest(name=role_id)
    role = client.get_role(request=request)

    return list(role.included_permissions)

def get_roles_from_file(file_path: str) -> list:
    """
    Reads a YAML file containing roles and returns a list of dictionaries with user data.

    Args:
        file_path: The path to the YAML file containing roles.
    Returns:
        A list of dictionaries with user data.
    """
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)

    roles = []
    for role in data:
        email = role.get("email")
        username = role.get("username")
        permissions = role.get("permissions", [])

        roles.append({
            "email": email,
            "username": username,
            "permissions": permissions
        })

    return roles

def permission_differences(project_id: str, user_email: str) -> list:
    """
    Generates a list of differences between the original and migrated permissions for a user.
    It gets the permission from the generated files, so it is expected that the files are already generated and up to date.

    Args:
        project_id: The ID of the Google Cloud project.
        user_email: The email of the user to compare permissions for.
    Returns:
        A list of dictionaries containing the differences in permissions for the specified user.
    """

    cache = {}
    user_differences = {}

    original = get_roles_from_file(f"{project_id}.original-roles.yaml")
    migrated = get_roles_from_file(f"{project_id}.migrated-roles.yaml")

    # Get the permissions on the beam_roles
    beam_roles = generate_roles.get_roles()
    for role_name, role_data in beam_roles.items():
        permissions = role_data["permissions"]
        cache[role_name] = permissions

    # Get the permissions for the original roles
    for user in original:
        username = user["username"]
        email = user["email"]

        # Skip if the user email does not match the specified user_email
        if user_email and email != user_email:
            continue

        original_roles = user["permissions"]

        original_permissions = []

        for role in original_roles:
            if '_withcond_' in role['role']:
                # Skip roles with conditions, as they are not supported in the new roles
                continue
            if 'organizations/' in role['role']:
                # Skip organization roles, as they are not supported in the new roles
                continue

            if role['role'] not in cache:
                permissions = get_gcp_role_permissions(role["role"])
                cache[role['role']] = sorted(permissions)
            original_permissions.extend(cache[role['role']])

        # Initialize the user differences entry
        user_differences[username] = {
            "email": email,
            "original_roles": original_roles,
            "original_permissions": sorted(original_permissions),
            "migrated_roles": [],
            "migrated_permissions": [],
            "differences": []
        }

    # Get the permissions for the migrated roles
    for user in migrated:
        username = user["username"]
        email = user["email"]

        # Skip if the user email does not match the specified user_email
        if user_email and email != user_email:
            continue

        migrated_roles = user["permissions"]

        migrated_permissions = []

        for role in migrated_roles:
            full_role_name = role["role"]
            # Owner is a special case, it should not be migrated to any other role.
            if "roles/owner" in full_role_name:
                migrated_permissions.extend(get_gcp_role_permissions(full_role_name))
            else:
                role_name = full_role_name.split('roles/')[1]
                migrated_permissions.extend(cache[role_name])

        user_differences[username]["migrated_roles"] = migrated_roles
        user_differences[username]["migrated_permissions"] = sorted(migrated_permissions)

    # Compare original and migrated permissions
    differences_list = []

    for username, user_data in user_differences.items():
        original_permissions = user_data["original_permissions"]
        migrated_permissions = user_data["migrated_permissions"]

        # Find differences in permissions
        original_set = set(original_permissions)
        migrated_set = set(migrated_permissions)

        added_permissions = migrated_set.difference(original_set)
        removed_permissions = original_set.difference(migrated_set)

        if added_permissions or removed_permissions:
            differences = {
                "username": username,
                "email": user_data["email"],
                "added_permissions": sorted(list(added_permissions)),
                "removed_permissions": sorted(list(removed_permissions))
            }
            differences_list.append(differences)

    return differences_list

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
        "--difference",
        dest="user_email",
        metavar="USER_EMAIL",
        help="Generate permission differences for the specified user email."
    )

    args = parser.parse_args()

    project_id = args.project_id
    user_email = args.user_email

    if user_email:
        # If the iam policy has not been generated yet, it will generate the original IAM policy first.
        if not os.path.exists(f"{project_id}.original-roles.yaml") or not os.path.exists(f"{project_id}.migrated-roles.yaml"):
            print(f"Original IAM policy for project {project_id} not found. Generating original and migrated roles first.")

            print(f"Exporting IAM policy for project {project_id}...")
            iam_data = export_project_iam(project_id)

            original_filename = f"{project_id}.original-roles.yaml"
            original_header = f"Exported original IAM policy for project {project_id}"
            to_yaml_file(iam_data, original_filename, header_info=original_header)

            print("Migrating permissions to new roles...")
            migrated_data = migrate_permissions(iam_data)
            migrated_filename = f"{project_id}.migrated-roles.yaml"
            migrated_header = f"Migrated IAM policy for project {project_id} to new beam_roles"
            to_yaml_file(migrated_data, migrated_filename, header_info=migrated_header)

            print(f"Generated {original_filename} and {migrated_filename}")

        print(f"Generating permission differences for {user_email} in project {project_id}...")
        differences = permission_differences(project_id, user_email)
        if differences:
            output_filename = f"{project_id}.permission-differences.yaml"
            header = f"Permission differences for user {user_email} in project {project_id}"
            to_yaml_file(differences, output_filename, header_info=header)
            print(f"Generated {output_filename}")
        else:
            print(f"No permission differences found for user {user_email} in project {project_id}.")
    else:
        print(f"Exporting IAM policy for project {project_id}...")
        iam_data = export_project_iam(project_id)

        original_filename = f"{project_id}.original-roles.yaml"
        original_header = f"Exported original IAM policy for project {project_id}"
        to_yaml_file(iam_data, original_filename, header_info=original_header)

        print("Migrating permissions to new roles...")
        migrated_data = migrate_permissions(iam_data)
        migrated_filename = f"{project_id}.migrated-roles.yaml"
        migrated_header = f"Migrated IAM policy for project {project_id} to new beam_roles"
        to_yaml_file(migrated_data, migrated_filename, header_info=migrated_header)

        print(f"Generated {original_filename} and {migrated_filename}")
        print(f"To generate permission differences, run: python {sys.argv[0]} {project_id} --difference <user_email>")


if __name__ == "__main__":
    main()