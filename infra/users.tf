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
}

resource "google_project_iam_member" "project_members" {
  for_each = { for user in local.users : user.email => user }
  project  = var.project_id
  role     = each.value.role
  member   = "user:${each.value.email}"
}
