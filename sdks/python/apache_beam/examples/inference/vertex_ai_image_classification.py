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

import argparse
import apache_beam as beam
import logging
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
from apache_beam.runners.runner import PipelineResult
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.examples.inference.testdata.constants import PROCESSED_JSON
""" A sample pipeline uses the RunInference API to classify images of flowers.
    
This pipeline reads an already-processes representation of an image of
sunflowers and sends it to a deployed Vertex AI model endpoint, then 
returns the predictions from the classifier model. The model and image
are from the Hello Image Data Vertex AI tutorial (see
ttps://cloud.google.com/vertex-ai/docs/tutorials/image-recognition-automl 
for more information.)
"""


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--endpoint',
      dest='endpoint',
      required=True,
      help='Vertex AI Endpoint resource ID to query.')
  parser.add_argument(
      '--project', dest='project', required=True, help='GCP Project')
  parser.add_argument(
      '--location',
      dest='location',
      required=True,
      help='GCP location for the Endpoint')
  parser.add_argument(
      '--experiment',
      dest='experiment',
      required=False,
      help='GCP experiment to pass to init')
  return parser.parse_known_args(argv)


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  model_handler = VertexAIModelHandlerJSON(
      endpoint_id=known_args.endpoint,
      project=known_args.project,
      location=known_args.location,
      experiment=known_args.experiment)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  read_text_json = pipeline | "ReadExample" >> beam.Create([PROCESSED_JSON])
  predictions = read_text_json | "RunInference" >> RunInference(
      model_handler=model_handler)
  write_output = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()