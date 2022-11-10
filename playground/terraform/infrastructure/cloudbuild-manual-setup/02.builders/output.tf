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

output "next_step_custom_message_hack" {
  value = <<EOF

Trigger #1:

There are two triggers that run Playground infrastructure deployment first, then to GKE.
To run FIRST trigger,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project}
and click `RUN` for ${google_cloudbuild_trigger.playground_infrastructure.name} to deploy Playground infrastructure

After trigger is succesfully triggered and Playground Infrastructure deployed:

Please navigate to https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#deploy-playground-infrastructure
and execute step #2:
"Add following DNS A records for the discovered static IP address".


(Note: To run the trigger you have to run it manually. See more in README file)

EOF
}

output "next_step_custom_message_hack_2" {
  value = <<EOF

Trigger #2:

Once Playground infrastructure deployed, you could now deploy Playground to GKE.
To run SECOND trigger,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project}
and click `RUN` for ${google_cloudbuild_trigger.playground_to_gke.name} to deploy Playground to GKE.

After trigger is succesfully triggered and Playground Infrastructure deployed:

Please navigate to https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#validate-deployed-playground
to validate deployment (see "Validate deployed Playground").

(Note: To run the trigger you have to run it manually. See more in README file)

EOF
}