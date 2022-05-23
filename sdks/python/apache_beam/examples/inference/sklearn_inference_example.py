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
from apache_beam.ml.inference.api import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelLoader
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class PostProcessor(beam.DoFn):
  """Post process PredictionResult to output true_label and
  prediction using numpy."""
  def process(self, element):
    true_label, prediction_result = element
    prediction = prediction_result.inference
    yield true_label, prediction


def process_input(data):
  split_input = data.split(',')
  label, pixel = split_input[0], split_input[1:]
  return label, pixel


def setup_pipeline(options: PipelineOptions, args=None):
  """Sets up Sklearn RunInference pipeline"""
  model_loader = SklearnModelLoader(
      model_file_type=ModelFileType.PICKLE, model_uri=args.model_path)

  with beam.Pipeline(options=options) as p:
    data = (
        p
        | "Read from csv file" >> beam.io.ReadFromText(
            args.input, skip_header_lines=1)
        | "Process inputs" >> beam.Map(process_input))

    predictions = (
        data | "RunInference" >> RunInference(model_loader)
        | "PostProcessor" >> beam.ParDo(PostProcessor()))

    predictions | "Write output to GCS" >> beam.io.WriteToText( # pylint: disable=expression-not-assigned
      args.output,
      shard_name_template='',
      append_trailing_newlines=True)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', dest='input', help='path to input file')
  parser.add_argument(
      '--output', dest='output', help='Predictions are saved to the output.')
  parser.add_argument(
      '--model_path', dest='model_path', help='Path to load the model.')
  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Entry point. Defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args=pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  setup_pipeline(pipeline_options, args=known_args)


if __name__ == '__main__':
  run()
