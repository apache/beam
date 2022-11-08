# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

output "cloudbuild_trigger_name" {
  value = google_cloudbuild_trigger.builder.name
}

output "next_step_custom_message_hack" {
  value = <<EOF

As a one-time setup,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project_id}
and click `RUN` for ${google_cloudbuild_trigger.builder.name}

(Note: You will have to manually start the trigger)

EOF
}