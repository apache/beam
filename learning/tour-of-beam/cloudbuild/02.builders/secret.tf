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

resource "google_secret_manager_secret" "secret_firebase_deployment_token" {
  secret_id = var.firebase_token_secret_id

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "secret_firebase_deployment_token_data" {
  secret = google_secret_manager_secret.secret_firebase_deployment_token.id
  secret_data = var.data_for_firebase_token_secret
}
