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

from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.compiler import Compiler

PIPELINE_ROOT = "<pipeline-root-path>"
BASE_ARTIFACT_PATH = "<base-path-to-store-pipeline-artifacts"

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
def pipeline(base_artifact_path: str = BASE_ARTIFACT_PATH):

  ingest_data_task = DataIngestOp(base_artifact_path=base_artifact_path)
  data_preprocessing_task = DataPreprocessingOp(
      ingested_dataset_path=ingest_data_task.outputs["ingested_dataset_path"],
      base_artifact_path=base_artifact_path)

  train_model_task = TrainModelOp(
      preprocessed_dataset_path=data_preprocessing_task.
      outputs["preprocessed_dataset_path"],
      base_artifact_path=base_artifact_path)


# [END define_kfp_pipeline]

if __name__ == "__main__":
  # [START compile_kfp_pipeline]
  Compiler().compile(pipeline_func=pipeline, package_path="pipeline.json")
  # [END compile_kfp_pipeline]
