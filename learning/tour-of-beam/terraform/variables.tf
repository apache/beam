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

variable "project_id" {
  description = "The GCP Project ID where ToB Cloud Functions will be created"
}

variable "region" {
  description = "Location of Cloud Function"
}

variable "cloud_function_name" {
  description = "Name of cloud function"
}

variable "runtime_name" {
  description = "Name of runtime (e.g. nodejs16, python39, dotnet3, go116, java11, ruby30, php74) see https://cloud.google.com/functions/docs/concepts/execution-environment#runtimes for more runtimes"
}

variable "code_function_name" {
  description = "Name of function in code"
}

variable "service_account_name" {
  description = "Name of the service account to run Cloud Function"
}