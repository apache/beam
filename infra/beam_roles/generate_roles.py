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

# This script generates roles based on what Apache Beam uses in GCP.
# The roles defined here are used to assign permissions to users.

# Roles are:
# - Beam Viewer -> GCP Viewer permissions based on the services used by Beam and excluding secretmanager permissions.
# - Beam Commiter -> GCP Viewer permissions based on the services used by Beam and excluding secretmanager permissions. Its like Beam Viewer but for people actively contributing code.
# - Beam Infra Manager -> GCP Editor permissions based on the services used by Beam Administrators without destructive permissions.
# - Beam Admin -> Permissions similar to GCP Editor based on the services used by Beam, but with destructive capabilities.
# - Beam Owner -> This is currently a placeholder; use the GCP Owner role directly if needed.


import yaml
from google.cloud import iam_admin_v1
from google.api_core import exceptions

# Services used by Beam.
# Note: Missing the ones from role: organizations/433637338589/roles/GceStorageAdmin
BEAM_VIEWER_SERVICES = [
    "artifactregistry",
    "biglake",
    "bigquery",
    "cloudasset",
    "cloudbuild",
    "cloudfunctions",
    "cloudsql",
    "compute",
    "container",
    "dataflow",
    "dataproc",
    "datastore",
    "dns",
    "firebase",
    "iam",
    "iap",
    "meshconfig",
    "monitoring",
    "pubsub",
    "redis",
    "resourcemanager",
    "secretmanager",
    "servicemanagement",
    "serviceusage",
    "spanner",
    "storage",
    "trafficdirector",
]

# Services that should not be included in the Beam roles.
SECRET_MANAGER_SERVICES = [
    "secretmanager",
]

# Suffixes that indicate destructive actions in GCP.
GCP_DESTRUCTIVE_SUFFIXES = [
    ".delete",
    ".remove",
    ".destroy",
    ".purge",
    ".cancel",
    ".stop",
    ".terminate",
]

# Permissions cache to avoid repeated API calls.
permissions_cache = {}

def get_permission_stage(permission_name: str, project_id: str) -> str:
    """
    Finds the support level of a specific IAM permission for a given project. This function caches the results to avoid repeated API calls.

    Args:
        permission_name: The name of the permission to check, e.g., 'storage.buckets.create'.
        project_id: The ID of the GCP project to check against.
    Returns:
        The support level of the permission as a string, or None if the permission is not found.
    """
    global permissions_cache

    try:
        if project_id in permissions_cache:
            return permissions_cache[project_id].get(permission_name, None)
        else:
            permissions_cache[project_id] = {}

        client = iam_admin_v1.IAMClient()
        resource = f"//cloudresourcemanager.googleapis.com/projects/{project_id}"

        request = iam_admin_v1.QueryTestablePermissionsRequest(
            full_resource_name=resource,
            page_size=1000
        )

        for permission in client.query_testable_permissions(request=request):
            permissions_cache[project_id][permission.name] = permission.custom_roles_support_level

        return permissions_cache.get(permission_name, None)

    except exceptions.PermissionDenied as e:
        print(f"Error: Permission denied. Ensure you have 'resourcemanager.projects.get' on project '{project_id}'.")
        print(f"Details: {e}")
        return None
    except exceptions.NotFound as e:
        print(f"Error: Project '{project_id}' not found.")
        print(f"Details: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching permissions: {e}")
        return None

def get_role_permissions(role_name: str, project_id: str = None) -> list[str]:
    """
    Gets the permissions included in a predefined or custom IAM role, filtered to only GA permissions.

    Args:
        role_name: The full name of the role.
                   For predefined roles, e.g., 'roles/secretmanager.viewer'.
                   For custom roles, e.g., 'projects/your-project-id/roles/your-custom-role'.

        project_id: Optional, used for permission metadata lookup.
    Returns:
        A list of GA permissions associated with the role.
    """
    try:
        client = iam_admin_v1.IAMClient()
        request = iam_admin_v1.GetRoleRequest(
            name=role_name,
        )
        role = client.get_role(request=request)
        all_perms = list(role.included_permissions)
        ga_perms = []
        for perm in all_perms:
            stage = get_permission_stage(perm, project_id)
            if stage == iam_admin_v1.Permission.CustomRolesSupportLevel.SUPPORTED:
                ga_perms.append(perm)
        return ga_perms
    except exceptions.NotFound:
        print(f"Error: The role '{role_name}' was not found.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def filter_permissions(permissions: list[str], allowed_strs: list[str] = None, denied_strs: list[str] = None) -> list[str]:
    """
    Filters permissions based on the provided services.

    Args:
        permissions: A list of permissions to filter.
        allowed_strs: A list of strings that permissions must contain to be included.
        denied_strs: A list of strings that permissions must not contain to be included.
    Returns:
        A list of permissions that match the specified services.
    """

    if allowed_strs is None:
        allowed_strs = []
    if denied_strs is None:
        denied_strs = GCP_DESTRUCTIVE_SUFFIXES

    filtered_permissions = []
    for perm in permissions:
        if any(allowed in perm for allowed in allowed_strs) and not any(denied in perm for denied in denied_strs):
            filtered_permissions.append(perm)
    return sorted(filtered_permissions)

def generate_role(role_name: str , perms: list[str]) -> dict:
    return {
        "role_id": f"{role_name}",
        "title": f"{role_name}",
        "stage": "GA",
        "description": f"This is the {role_name} role",
        "permissions": sorted(perms)
    }


def main():
    gcp_viewer_perms = get_role_permissions("roles/viewer", project_id="apache-beam-testing")
    gcp_editor_perms = get_role_permissions("roles/editor", project_id="apache-beam-testing")

    # Generate Beam Viewer role
    beam_viewer_perms = filter_permissions(gcp_viewer_perms, BEAM_VIEWER_SERVICES, GCP_DESTRUCTIVE_SUFFIXES + SECRET_MANAGER_SERVICES)
    beam_viewer_role = generate_role("beam_viewer", beam_viewer_perms)
    with open("beam_viewer.role.yaml", "w") as f:
        yaml.dump(beam_viewer_role, f, default_flow_style=False)

    # Generate Beam Committer role
    beam_committer_perms = filter_permissions(gcp_viewer_perms, BEAM_VIEWER_SERVICES, GCP_DESTRUCTIVE_SUFFIXES + SECRET_MANAGER_SERVICES)
    beam_committer_role = generate_role("beam_committer", beam_committer_perms)
    with open("beam_committer.role.yaml", "w") as f:
        yaml.dump(beam_committer_role, f, default_flow_style=False)

    # Generate Beam Infrastructure Manager role
    beam_infra_manager_perms = filter_permissions(gcp_editor_perms, BEAM_VIEWER_SERVICES, GCP_DESTRUCTIVE_SUFFIXES + SECRET_MANAGER_SERVICES)
    beam_infra_manager_role = generate_role("beam_infra_manager", beam_infra_manager_perms)
    with open("beam_infra_manager.role.yaml", "w") as f:
        yaml.dump(beam_infra_manager_role, f, default_flow_style=False)

    # Generate Beam Admin role
    beam_admin_perms = filter_permissions(gcp_editor_perms, BEAM_VIEWER_SERVICES, SECRET_MANAGER_SERVICES)
    beam_admin_role = generate_role("beam_admin", beam_admin_perms)
    with open("beam_admin.role.yaml", "w") as f:
        yaml.dump(beam_admin_role, f, default_flow_style=False)

    print("Roles generated successfully.")

if __name__ == "__main__":
    main()
