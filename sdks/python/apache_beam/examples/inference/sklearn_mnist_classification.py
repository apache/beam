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

"""A pipeline that uses RunInference API to perform image classification."""

import argparse
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple

import apache_beam as beam
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.api import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelLoader
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def process_input(row: str) -> Tuple[int, List[int]]:
  data = row.split(',')
  label, pixels = int(data[0]), data[1:]
  pixels = [int(pixel) for pixel in pixels]
  return label, pixels


class PostProcessor(beam.DoFn):
  def process(self, element: Tuple[int, PredictionResult]) -> Iterable[Dict]:
    label, prediction_result = element
    prediction = prediction_result.inference
    yield {label: prediction}


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_file',
      dest='input',
      help='CSV file with row containing label and pixel values.')
  parser.add_argument(
      '--output', dest='output', help='Path to save output predictions.')
  parser.add_argument(
      '--model_path',
      dest='model_path',
      help='Path to load the Sklearn model for Inference.')
  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Entry point. Defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  model_loader = SklearnModelLoader(
      model_file_type=ModelFileType.PICKLE, model_uri=known_args.model_path)

  with beam.Pipeline(options=pipeline_options) as p:
    label_pixel_tuple = (
        p
        | "ReadFromInput" >> beam.io.ReadFromText(
            known_args.input, skip_header_lines=1)
        | "Process inputs" >> beam.Map(process_input))

    predictions = (
        label_pixel_tuple
        | "RunInference" >> RunInference(model_loader).with_output_types(
            Tuple[int, PredictionResult])
        | "PostProcessor" >> beam.ParDo(PostProcessor()))

    if known_args.output:
      predictions | "WriteOutputToGCS" >> beam.io.WriteToText( # pylint: disable=expression-not-assigned
        known_args.output,
        shard_name_template='',
        append_trailing_newlines=True)


if __name__ == '__main__':
  run()
