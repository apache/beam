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

resource "google_app_engine_flexible_app_version" "backend_app_java" {
  version_id                = "v1"
  project                   = var.project_id
  service                   = "${var.service_name}-${var.environment}"
  runtime                   = "custom"
  delete_service_on_destroy = true


  liveness_check {
    path          = "/liveness"
    initial_delay = "40s"
  }

  readiness_check {
    path = "/readiness"
  }

  automatic_scaling {
    max_total_instances = var.max_instance
    min_total_instances = var.min_instance
    cool_down_period    = "120s"
    cpu_utilization {
      target_utilization = 0.7
    }
  }

  env_variables = {
    CACHE_TYPE        = var.cache_type
    CACHE_ADDRESS     = "${var.cache_address}:6379"
    NUM_PARALLEL_JOBS = 10
    LAUNCH_SITE       = "app_engine"
  }

  resources {
    memory_gb = var.memory
    cpu       = var.cpu
    volumes {
      name        = "inmemory"
      size_gb     = var.volume_size
      volume_type = "tmpfs"
    }
  }

  network {
    name = var.network_name
    subnetwork = var.subnetwork_name
  }

  deployment {
    container {
      image = "${var.docker_registry_address}/${var.docker_image_name}:${var.docker_image_tag}"
    }
  }
}

