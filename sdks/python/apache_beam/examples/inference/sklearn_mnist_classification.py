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

"""A pipeline that uses RunInference API to classify MNIST data.

This pipeline takes a text file in which data is comma separated ints. The first
column would be the true label and the rest would be the pixel values. The data
is processed and then a model trained on the MNIST data would be used to perform
the inference. The pipeline writes the prediction to an output file in which
users can then compare against the true label.
"""

import argparse
from typing import Iterable
from typing import List
from typing import Tuple

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def process_input(row: str) -> Tuple[int, List[int]]:
  data = row.split(',')
  label, pixels = int(data[0]), data[1:]
  pixels = [int(pixel) for pixel in pixels]
  return label, pixels


class PostProcessor(beam.DoFn):
  """Process the PredictionResult to get the predicted label.
  Returns a comma separated string with true label and predicted label.
  """
  def process(self, element: Tuple[int, PredictionResult]) -> Iterable[str]:
    label, prediction_result = element
    prediction = prediction_result.inference
    yield '{},{}'.format(label, prediction)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_file',
      dest='input',
      required=True,
      help='text file with comma separated int values.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--model_path',
      dest='model_path',
      required=True,
      help='Path to load the Sklearn model for Inference.')
  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Entry point. Defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # In this example we pass keyed inputs to RunInference transform.
  # Therefore, we use KeyedModelHandler wrapper over SklearnModelHandlerNumpy.
  model_loader = KeyedModelHandler(
      SklearnModelHandlerNumpy(
          model_file_type=ModelFileType.PICKLE,
          model_uri=known_args.model_path))

  with beam.Pipeline(options=pipeline_options) as p:
    label_pixel_tuple = (
        p
        | "ReadFromInput" >> beam.io.ReadFromText(
            known_args.input, skip_header_lines=1)
        | "PreProcessInputs" >> beam.Map(process_input))

    predictions = (
        label_pixel_tuple
        | "RunInference" >> RunInference(model_loader)
        | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

    _ = predictions | "WriteOutput" >> beam.io.WriteToText(
        known_args.output,
        shard_name_template='',
        append_trailing_newlines=True)


if __name__ == '__main__':
  run()
