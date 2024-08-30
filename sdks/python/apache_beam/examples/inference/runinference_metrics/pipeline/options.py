#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This file contains the pipeline options to configure
the Dataflow pipeline."""

from datetime import datetime
from typing import Any

import config as cfg
from apache_beam.options.pipeline_options import PipelineOptions


def get_pipeline_options(
    project: str,
    job_name: str,
    mode: str,
    device: str,
    num_workers: int = cfg.NUM_WORKERS,
    **kwargs: Any,
) -> PipelineOptions:
  """Function to retrieve the pipeline options.
    Args:
        project: GCP project to run on
        mode: Indicator to run local, cloud or template
        num_workers: Number of Workers for running the job parallely
    Returns:
        Dataflow pipeline options
    """
  job_name = f'{job_name}-{datetime.now().strftime("%Y%m%d%H%M%S")}'

  staging_bucket = f"gs://{cfg.PROJECT_ID}-ml-examples"

  # For a list of available options, check:
  # https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options
  dataflow_options = {
      "runner": "DirectRunner" if mode == "local" else "DataflowRunner",
      "job_name": job_name,
      "project": project,
      "region": cfg.REGION,
      "staging_location": f"{staging_bucket}/dflow-staging",
      "temp_location": f"{staging_bucket}/dflow-temp",
      "setup_file": "./setup.py",
  }
  flags = []
  if device == "GPU":
    flags = [
        "--experiment=worker_accelerator=type:nvidia-tesla-p4;count:1;"\
          "install-nvidia-driver",
    ]
    dataflow_options.update({
        "sdk_container_image": cfg.DOCKER_IMG,
        "machine_type": "n1-standard-4",
    })

  # Optional parameters
  if num_workers:
    dataflow_options.update({"num_workers": num_workers})
  return PipelineOptions(flags=flags, **dataflow_options)
