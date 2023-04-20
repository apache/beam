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

# GCS bucket for source code for cloud functions
resource "google_storage_bucket" "cloud_functions_bucket" {
  name          = local.cloudfunctions_bucket
  location      = var.region
  storage_class = "STANDARD"
}

# GCS bucket object to store source code
resource "google_storage_bucket_object" "zip" {
  # Use an MD5 here. If there's no changes to the source code, this won't change either.
  # We can avoid unnecessary redeployments by validating the code is unchanged, and forcing
  # a redeployment when it has
  name         = "${data.archive_file.source_code.output_md5}.zip"
  bucket       = google_storage_bucket.cloud_functions_bucket.name
  source       = data.archive_file.source_code.output_path
  content_type = "application/zip"
}
