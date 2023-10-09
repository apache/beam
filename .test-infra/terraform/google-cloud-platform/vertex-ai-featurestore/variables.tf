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

variable "project" {
  type        = string
  description = "The Google Cloud Platform (GCP) project within which resources are provisioned"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "featurestore" {
  type = object({
    // The prefix of the Featurestore name.
    name_prefix = string

    // The number of nodes to configure the Featurestore.
    fixed_node_count = number

    // The Featurestore's Entity Type configuration where the map key configures
    // the Entity Type name.
    entity_types = map(object({

      // The Entity Type's features configuration where the map key configures
      // the Feature name and the value configures its data type such as
      // BOOL, STRING, INT64, etc.
      // See https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.featurestores.entityTypes.features#ValueType
      // for allowed data types.
      description = string
      features    = map(string)
    }))
  })
  description = "The Vertex AI Featurestore configuration"
}
