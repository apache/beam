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
import logging
import os
from collections.abc import Iterable

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


def process_input(row: str) -> tuple[int, list[int]]:
  data = row.split(',')
  label, pixels = int(data[0]), data[1:]
  pixels = [int(pixel) for pixel in pixels]
  return label, pixels


class PostProcessor(beam.DoFn):
  """Process the PredictionResult to get the predicted label.
  Returns a comma separated string with true label and predicted label.
  """
  def process(self, element: tuple[int, PredictionResult]) -> Iterable[str]:
    label, prediction_result = element
    prediction = prediction_result.inference
    yield '{},{}'.format(label, prediction)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
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
  parser.add_argument(
      '--large_model',
      action='store_true',
      dest='large_model',
      default=False,
      help='Set to true if your model is large enough to run into memory '
      'pressure if you load multiple copies.')
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
  requirements_dir = os.path.dirname(os.path.realpath(__file__))
  # Pin to the version that we trained the model on.
  # Sklearn doesn't guarantee compatability between versions.
  pipeline_options.view_as(
      SetupOptions
  ).requirements_file = f'{requirements_dir}/sklearn_examples_requirements.txt'

  # In this example we pass keyed inputs to RunInference transform.
  # Therefore, we use KeyedModelHandler wrapper over SklearnModelHandlerNumpy.
  model_loader = KeyedModelHandler(
      SklearnModelHandlerNumpy(
          model_file_type=ModelFileType.PICKLE,
          model_uri=known_args.model_path,
          large_model=known_args.large_model))

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  label_pixel_tuple = (
      pipeline
      | "ReadFromInput" >> beam.io.ReadFromText(known_args.input)
      | "PreProcessInputs" >> beam.Map(process_input))

  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(model_loader)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
