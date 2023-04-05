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

"""Preprocessing example with TFX with the LocalDagRunner and
either the beam DirectRunner or DataflowRunner"""
import argparse
import os

from tfx import v1 as tfx


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
      "--pipeline-name",
      type=str,
      help="Name for the Beam pipeline.",
      required=True)
  parser.add_argument(
      "--pipeline-root",
      type=str,
      help=
      "Path to artifact repository where TFX stores a pipeline’s artifacts.",
      required=True)
  parser.add_argument(
      "--csv-file", type=str, help="Path to the csv input file.", required=True)
  parser.add_argument(
      "--csv-file", type=str, help="Path to the csv input file.", required=True)
  parser.add_argument(
      "--module-file",
      type=str,
      help="Path to module file containing the preprocessing_fn and run_fn.",
      default="coco_captions_utils.py")
  parser.add_argument(
      "--beam-runner",
      type=str,
      help="Beam runner: DataflowRunner or DirectRunner.",
      default="DirectRunner")
  parser.add_argument(
      "--metadata-file",
      type=str,
      help="Path to store a metadata file as a mock metadata database",
      default="metadata.db")
  return parser.parse_args()


# [START tfx_pipeline]
def create_pipeline(
    gcp_project_id,
    region,
    pipeline_name,
    pipeline_root,
    csv_file,
    module_file,
    beam_runner,
    metadata_file):
  """Create the TFX pipeline.

  Args:
      gcp_project_id (str): ID for the google cloud project to deploy the pipeline to.
      region (str): Region in which to deploy the pipeline.
      pipeline_name (str): Name for the Beam pipeline
      pipeline_root (str): Path to artifact repository where TFX
        stores a pipeline’s artifacts.
      csv_file (str): Path to the csv input file.
      module_file (str): Path to module file containing the preprocessing_fn and run_fn.
      beam_runner (str): Beam runner: DataflowRunner or DirectRunner.
      metadata_file (str): Path to store a metadata file as a mock metadata database.
  """
  example_gen = tfx.components.CsvExampleGen(input_base=csv_file)

  # Computes statistics over data for visualization and example validation.
  statistics_gen = tfx.components.StatisticsGen(
      examples=example_gen.outputs['examples'])

  schema_gen = tfx.components.SchemaGen(
      statistics=statistics_gen.outputs['statistics'], infer_feature_shape=True)

  transform = tfx.components.Transform(
      examples=example_gen.outputs['examples'],
      schema=schema_gen.outputs['schema'],
      module_file=module_file)

  trainer = tfx.components.Trainer(
      module_file=module_file,
      examples=transform.outputs['transformed_examples'],
      transform_graph=transform.outputs['transform_graph'])

  components = [example_gen, statistics_gen, schema_gen, transform, trainer]

  beam_pipeline_args_by_runner = {
      'DirectRunner': [],
      'DataflowRunner': [
          '--runner=DataflowRunner',
          '--project=' + gcp_project_id,
          '--temp_location=' + os.path.join(pipeline_root, 'tmp'),
          '--region=' + region,
      ]
  }

  return tfx.dsl.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      components=components,
      enable_cache=True,
      metadata_connection_config=tfx.orchestration.metadata.
      sqlite_metadata_connection_config(metadata_file),
      beam_pipeline_args=beam_pipeline_args_by_runner[beam_runner])


# [END tfx_pipeline]

if __name__ == "__main__":

  # [START tfx_execute_pipeline]
  args = parse_args()
  tfx.orchestration.LocalDagRunner().run(create_pipeline(**vars(args)))
  # [END tfx_execute_pipeline]
