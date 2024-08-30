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

provider "google" {
  project = var.project
}

resource "google_project_service" "required" {
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

resource "random_string" "postfix" {
  length  = 6
  upper   = false
  special = false
}

resource "google_vertex_ai_featurestore" "default" {
  depends_on = [google_project_service.required]
  name       = "${var.featurestore.name_prefix}_${random_string.postfix.result}"
  region     = var.region
  online_serving_config {
    fixed_node_count = var.featurestore.fixed_node_count
  }
}

resource "google_vertex_ai_featurestore_entitytype" "entities" {
  depends_on   = [google_project_service.required]
  for_each     = var.featurestore.entity_types
  name         = each.key
  featurestore = google_vertex_ai_featurestore.default.id
  description  = each.value.description
  monitoring_config {

    categorical_threshold_config {
      value = 0.3
    }

    numerical_threshold_config {
      value = 0.3
    }

    snapshot_analysis {
      disabled                 = false
      monitoring_interval_days = 1
      staleness_days           = 21
    }
  }
}

locals {
  features = flatten([
    for entitytype_name, entitytype in var.featurestore.entity_types : [
      for feature_name, feature_type in entitytype.features : {
        entitytype_name = entitytype_name
        feature_name    = feature_name
        feature_type    = feature_type
      }
    ]
  ])
  features_map = tomap({
    for feature in local.features :
    "${feature["entitytype_name"]}.${feature["feature_name"]}" => feature
  })
}

resource "google_vertex_ai_featurestore_entitytype_feature" "features" {
  depends_on = [google_project_service.required]
  for_each   = local.features_map
  name       = each.value["feature_name"]
  entitytype = google_vertex_ai_featurestore_entitytype.entities[each.value["entitytype_name"]].id
  value_type = each.value["feature_type"]
}
