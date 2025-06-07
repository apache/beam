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

# This file ingests the users defined in the `users.yml` file and
# configures the corresponding users in the GCP project.

locals {
  users = yamldecode(file("${path.module}/users.yml"))

  user_permissions = flatten([
    for user in (local.users == null ? [] : local.users) : [
      for perm in (user.permissions == null ? [] : user.permissions) :
        {
          username              = user.username
          email                 = user.email
          role                  = perm.role
          request_description   = lookup(perm, "request_description", null)
          expiry_date           = lookup(perm, "expiry_date", null)
        } if perm != null && lookup(perm, "role", null) != null
    ]
  ])
}

resource "google_project_iam_member" "project_members" {
  for_each = {
    for up in local.user_permissions : "${up.email}-${up.role}" => up
  }
  project = var.project_id
  role    = each.value.role
  member  = "user:${each.value.email}"

  dynamic "condition" {
    # Condition is only created if expiry_date is set
    for_each = each.value.expiry_date != null && each.value.expiry_date != "" ? [true] : []
    content {
      title       = "${each.value.title}"
      description = "${each.value.description}"
      expression  = "request.time < timestamp('${each.value.expiry_date}T00:00:00Z')"
    }
  }
}
