#
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
#


data "terraform_remote_state" "remote_state_vpc" {
  backend = "gcs"
  config = {
    bucket  = "${var.beam_playground_terraform}"
  }
}


resource "google_redis_instance" "cache" {
  provider           = google-beta
  project            = "${var.project_id}"
#  region             = "${data.terraform_remote_state.remote_state_vpc.outputs.subnet_region}"
  region              = "us-central1"
  name               = "playground-backend-cache"
  tier               = "BASIC"
  memory_size_gb     = 16

#  authorized_network = "${data.terraform_remote_state.remote_state_vpc.outputs.vpc_id}"

  redis_version      = "${var.redis_version}"
  display_name       = "Playground Cache"

}
