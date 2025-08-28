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

# This Terraform configuration file is used to manage custom IAM roles
# in a Google Cloud Platform (GCP) project. It reads role definitions
# from YAML files located in the same directory and creates custom roles
# in the specified GCP project.

locals {
  role_files = fileset(path.module, "*.role.yaml")
  roles_data = {
    for f in local.role_files :
    trimsuffix(f, ".role.yaml") => yamldecode(file("${path.module}/${f}"))
  }
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

resource "google_project_iam_custom_role" "custom_roles" {
  for_each = local.roles_data

  project     = var.project_id
  role_id     = each.value.role_id
  title       = each.value.title
  description = lookup(each.value, "description", null)
  permissions = each.value.permissions
  stage       = lookup(each.value, "stage", "GA")
}
