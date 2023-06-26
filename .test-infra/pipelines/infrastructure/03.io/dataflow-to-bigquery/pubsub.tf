/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Provision a Pub/Sub topic to forward Eventarc Workflow event payloads
resource "google_pubsub_topic" "workflow_topic" {
  name = var.workflow_resource_name_base
}

// Provision a Pub/Sub subscription to the Eventarc Workflow event topic
resource "google_pubsub_subscription" "source" {
  name  = "${var.workflow_resource_name_base}-sub"
  topic = google_pubsub_topic.workflow_topic.name
}

// Allow Dataflow Worker Service Account to subscribe to Pub/Sub subscription
resource "google_pubsub_subscription_iam_member" "source" {
  for_each = toset([
    "roles/pubsub.viewer",
    "roles/pubsub.subscriber",
  ])
  member       = "serviceAccount:${data.google_service_account.dataflow_worker.email}"
  role         = each.key
  subscription = google_pubsub_subscription.source.id
}
