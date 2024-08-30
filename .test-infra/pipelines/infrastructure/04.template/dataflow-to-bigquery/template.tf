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

locals {
  beam_root = "${path.module}/../../../../.."
}

// Builds the Shadow jar via the gradle command.
resource "null_resource" "shadowjar" {
  triggers = {
    id = uuid()
  }
  provisioner "local-exec" {
    working_dir = local.beam_root
    command     = "./gradlew ${var.gradle_project}:shadowJar"
  }
}

// Builds the Dataflow Flex template by invoking the gcloud command.
resource "null_resource" "build_template" {
  triggers = {
    id = uuid()
  }
  depends_on = [null_resource.shadowjar]
  provisioner "local-exec" {
    command = <<EOF
      gcloud dataflow flex-template build \
        ${local.template_file_gcs_path} \
        --project=${var.project} \
        --image-gcr-path=${var.region}-docker.pkg.dev/${var.project}/${data.google_artifact_registry_repository.default.name}/${var.template_image_prefix} \
        --metadata-file=${local.beam_root}/.test-infra/pipelines/infrastructure/04.template/dataflow-to-bigquery/dataflow-template.json \
        --sdk-language=JAVA \
        --flex-template-base-image=JAVA11 \
        --jar=${local.beam_root}/.test-infra/pipelines/build/libs/beam-beam-test-infra-pipelines-latest.jar \
        --env=FLEX_TEMPLATE_JAVA_MAIN_CLASS=org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery \
        --network=${data.google_compute_network.default.name} \
        --subnetwork=regions/${var.region}/subnetworks/${data.google_compute_subnetwork.default.name} \
        --disable-public-ips \
        --service-account-email=${data.google_service_account.dataflow_worker.email} \
        --enable-streaming-engine
    EOF
  }
}
