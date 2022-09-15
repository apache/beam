"""This file contains the pipeline options to configure the Dataflow pipeline."""
import os
from datetime import datetime
from typing import Any

from apache_beam.options.pipeline_options import PipelineOptions

import config as cfg


def get_pipeline_options(
    project: str,
    job_name: str,
    mode: str,
    num_workers: int = cfg.NUM_WORKERS,
    streaming: bool = True,
    **kwargs: Any,
) -> PipelineOptions:
    """Function to retrieve the pipeline options.
    Args:
        project: GCP project to run on
        mode: Indicator to run local, cloud or template
        num_workers: Number of Workers for running the job parallely
        max_num_workers: Maximum number of workers running the job parallely
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
        "region": "us-central1",
        "staging_location": f"{staging_bucket}/dflow-staging",
        "temp_location": f"{staging_bucket}/dflow-temp",
        # "save_main_session": False,
        "setup_file": "./setup.py",
        "streaming": streaming,
    }

    # Optional parameters
    if num_workers:
        dataflow_options.update({"num_workers": num_workers})

    return PipelineOptions(flags=[], **dataflow_options)
