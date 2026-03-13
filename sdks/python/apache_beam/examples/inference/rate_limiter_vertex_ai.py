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

"""A simple example demonstrating usage of the EnvoyRateLimiter with Vertex AI.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io.components.rate_limiter import EnvoyRateLimiter
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--project',
      dest='project',
      help='The Google Cloud project ID for Vertex AI.')
  parser.add_argument(
      '--location',
      dest='location',
      help='The Google Cloud location (e.g. us-central1) for Vertex AI.')
  parser.add_argument(
      '--endpoint_id',
      dest='endpoint_id',
      help='The ID of the Vertex AI endpoint.')
  parser.add_argument(
      '--rls_address',
      dest='rls_address',
      help='The address of the Envoy Rate Limit Service (e.g. localhost:8081).')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # Initialize the EnvoyRateLimiter
  rate_limiter = EnvoyRateLimiter(
      service_address=known_args.rls_address,
      domain="mongo_cps",
      descriptors=[{
          "database": "users"
      }],
      namespace='example_pipeline')

  # Initialize the VertexAIModelHandler with the rate limiter
  model_handler = VertexAIModelHandlerJSON(
      endpoint_id=known_args.endpoint_id,
      project=known_args.project,
      location=known_args.location,
      rate_limiter=rate_limiter)

  # Input features for the model
  features = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0],
              [10.0, 11.0, 12.0], [13.0, 14.0, 15.0]]

  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | 'CreateInputs' >> beam.Create(features)
        | 'RunInference' >> RunInference(model_handler)
        | 'PrintPredictions' >> beam.Map(logging.info))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
