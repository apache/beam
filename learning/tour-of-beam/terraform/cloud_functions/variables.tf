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

variable "service_account_id" {
  description = "The name of Service Account to run Cloud Function"
}

variable "project_id" {
  description = "The GCP Project ID of function"
}

variable "region" {
  description = "The GCP Region of function"
}

variable "source_archive_bucket" {
  description = "The GCS bucket containing the zip archive which contains the function"
}
variable "source_archive_object" {
  description = "The source archive object (file) in archive bucket"
}

variable "entry_point_names" {
  type = list
  default = ["getSdkList", "getContentTree", "getUnitContent", "getUserProgress", "postUnitComplete", "postUserCode"]
}