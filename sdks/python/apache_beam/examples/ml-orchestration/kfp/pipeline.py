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

import argparse

import kfp
from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.compiler import Compiler


def parse_args():
  """Parse arguments."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--gcp-project-id",
      type=str,
      help="ID for the google cloud project to deploy the pipeline to.",
      required=True)
  parser.add_argument(
      "--region",
      type=str,
      help="Region in which to deploy the pipeline.",
      required=True)
  parser.add_argument(
      "--pipeline-root",
      type=str,
      help=
      "Path to artifact repository where Kubeflow Pipelines stores a pipeline’s artifacts.",
      required=True)
  parser.add_argument(
      "--component-artifact-root",
      type=str,
      help=
      "Path to artifact repository where Kubeflow Pipelines components can store artifacts.",
      required=True)
  parser.add_argument(
      "--dataflow-staging-root",
      type=str,
      help="Path to staging directory for dataflow.",
      required=True)
  parser.add_argument(
      "--beam-runner",
      type=str,
      help="Beam runner: DataflowRunner or DirectRunner.",
      default="DirectRunner")
  return parser.parse_args()


# arguments are parsed as a global variable so
# they can be used in the pipeline decorator below
ARGS = parse_args()
PIPELINE_ROOT = vars(ARGS)['pipeline_root']

# [START load_kfp_components]
# load the kfp components from their yaml files
DataIngestOp = comp.load_component('components/ingestion/component.yaml')
DataPreprocessingOp = comp.load_component(
    'components/preprocessing/component.yaml')
TrainModelOp = comp.load_component('components/train/component.yaml')
# [END load_kfp_components]


# [START define_kfp_pipeline]
@dsl.pipeline(
    pipeline_root=PIPELINE_ROOT,
    name="beam-preprocessing-kfp-example",
    description="Pipeline to show an apache beam preprocessing example in KFP")
def pipeline(
    gcp_project_id: str,
    region: str,
    component_artifact_root: str,
    dataflow_staging_root: str,
    beam_runner: str):
  """KFP pipeline definition.

  Args:
      gcp_project_id (str): ID for the google cloud project to deploy the pipeline to.
      region (str): Region in which to deploy the pipeline.
      component_artifact_root (str): Path to artifact repository where Kubeflow Pipelines
        components can store artifacts.
      dataflow_staging_root (str): Path to staging directory for the dataflow runner.
      beam_runner (str): Beam runner: DataflowRunner or DirectRunner.
  """

  ingest_data_task = DataIngestOp(base_artifact_path=component_artifact_root)

  data_preprocessing_task = DataPreprocessingOp(
      ingested_dataset_path=ingest_data_task.outputs["ingested_dataset_path"],
      base_artifact_path=component_artifact_root,
      gcp_project_id=gcp_project_id,
      region=region,
      dataflow_staging_root=dataflow_staging_root,
      beam_runner=beam_runner)

  train_model_task = TrainModelOp(
      preprocessed_dataset_path=data_preprocessing_task.
      outputs["preprocessed_dataset_path"],
      base_artifact_path=component_artifact_root)


# [END define_kfp_pipeline]

if __name__ == "__main__":
  # [START compile_kfp_pipeline]
  Compiler().compile(pipeline_func=pipeline, package_path="pipeline.json")
  # [END compile_kfp_pipeline]

  run_arguments = vars(ARGS)
  del run_arguments['pipeline_root']

  # [START execute_kfp_pipeline]
  client = kfp.Client()
  experiment = client.create_experiment("KFP orchestration example")
  run_result = client.run_pipeline(
      experiment_id=experiment.id,
      job_name="KFP orchestration job",
      pipeline_package_path="pipeline.json",
      params=run_arguments)
  # [END execute_kfp_pipeline]
