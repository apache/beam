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

// Creates a GCP Eventarc trigger matching Dataflow Job Status Changed events
// See the following resources for more details:
// https://cloud.google.com/eventarc
// https://cloud.google.com/eventarc/docs/reference/supported-events
resource "google_eventarc_trigger" "default" {
  depends_on      = [google_project_iam_member.default]
  location        = var.region
  name            = var.workflow_resource_name_base
  service_account = google_service_account.default.email

  matching_criteria {
    attribute = "type"
    // matches 'type' property of the following command's output:
    // gcloud eventarc providers describe dataflow.googleapis.com --location=us-central1
    value     = "google.cloud.dataflow.job.v1beta3.statusChanged"
  }

  destination {
    workflow = google_workflows_workflow.default.id
  }
}

// Provisions a Workflow to which Dataflow Job Status Eventarc events are sent
// Forwards event payload to PubSub. See the following for the expected schema:
// https://github.com/googleapis/google-cloudevents/tree/main/proto/google/events/cloud/dataflow
resource "google_workflows_workflow" "default" {
  depends_on      = [google_project_iam_member.default]
  name            = var.workflow_resource_name_base
  description     = "Consumes Dataflow Job Status Eventarc events and publishes to Pub/Sub"
  region          = var.region
  service_account = google_service_account.default.email
  source_contents = <<EOF
main:
  params: [event]
  steps:
    - init:
        assign:
          - project: '$${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'
          - topic: "${google_pubsub_topic.workflow_topic.name}"
          - base64Msg: '$${base64.encode(json.encode(event))}'
    - publish_message_to_topic:
        call: googleapis.pubsub.v1.projects.topics.publish
        args:
          topic: $${"projects/" + project + "/topics/" + topic}
          body:
            messages:
              - data: '$${base64Msg}'
EOF
}
