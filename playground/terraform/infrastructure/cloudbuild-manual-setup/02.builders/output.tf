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

output "using_triggers" {
  value = <<EOF

To begin deploying Playground using triggers you must first execute steps described in "Prepare deployment configuration":
https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#prepare-deployment-configuration

-Trigger #1:

There are two triggers that run Playground infrastructure deployment first, then deployment to GKE.
To run FIRST trigger,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project}
and click `RUN` for ${google_cloudbuild_trigger.playground_infrastructure.name}.

After trigger is succesfully triggered and Playground Infrastructure deployed:

Please navigate to https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#deploy-playground-infrastructure
and execute step #2:
"Add following DNS A records for the discovered static IP address".

-Trigger #2:

Once Playground infrastructure deployed, you could now deploy Playground to GKE.
To run SECOND trigger,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project}
and click `RUN` for ${google_cloudbuild_trigger.playground_to_gke.name}.

Once Playground deployed to GKE:

Please navigate to https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#validate-deployed-playground to validate deployment.

EOF
}