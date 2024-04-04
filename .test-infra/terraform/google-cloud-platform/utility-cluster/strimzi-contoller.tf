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

resource "helm_release" "strimzi-helm-release" {
  name       = "strimzi"
  namespace  = "strimzi"
  create_namespace = true
  repository = "https://strimzi.io/charts/"
  chart      = "strimzi-kafka-operator"
  version = "0.40.0"
  
  atomic = "true"
  timeout = 500

  set {
    name  = "watchAnyNamespace"
    value = "true"
  }
  depends_on = [ module.gke.google_container_cluster ]
}

