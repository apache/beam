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

variable "project_id" {
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "playground_deploy_sa" {
  description = "The ID of the cloud build service account responsible for deploying the Playground"
  default = ""
}

variable "playground_update_sa" {
  description = "The ID of the cloud build service account responsible for updating the Playground"
  default = ""
}

variable "playground_ci_sa" {
  description = "The ID of the cloud build service account responsible for running Playground CI checks and scripts"
  default = ""
}

variable "playground_cd_sa" {
  description = "The ID of the cloud build service account responsible for running Playground CD checks and scripts"
  default = ""
}
