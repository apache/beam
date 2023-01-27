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

# pylint: skip-file

import argparse
from typing import Iterable
from typing import List
from typing import Tuple

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window
from apache_beam.ml.inference.utils import WatchFilePattern


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


def run(argv=None, save_main_session=True, test_pipeline=None):
  """Entry point for running the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  interval = known_args.interval
  file_pattern = known_args.file_pattern

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  si_pcoll = pipeline | WatchFilePattern(
      file_pattern=file_pattern,
      interval=interval,
      default_value=known_args.model_path)

  model_handler = KeyedModelHandler(
      SklearnModelHandlerNumpy(model_uri=known_args.model_path))

  label_pixel_tuple = (
      pipeline
      | "ReadFromPubSub" >> beam.io.ReadFromPubSub(known_args.topic)
      | "ApplyMainInputWindow" >> beam.WindowInto(
          beam.transforms.window.FixedWindows(interval))
      | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
      | "PreProcessInputs" >> beam.Map(process_input))

  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(
          model_handler=model_handler, model_path_pcoll=si_pcoll)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  predictions | beam.Map(print)

  # _ = predictions | "WriteOutput" >> beam.io.WriteToText(
  #     known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run().wait_until_finish()
  return result


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
      '--topic',
      default='projects/apache-beam-testing/topics/anandinguva-model-updates',
      help='Path to Pub/Sub topic.')
  parser.add_argument(
      '--file_pattern',
      default='gs://apache-beam-ml/tmp/side_input_test/*.pickle',
      help='Glob pattern to watch for an update.')
  parser.add_argument(
      '--interval',
      default=360,
      type=int,
      help='interval used to look for updates on a given file_pattern.')
  return parser.parse_known_args(argv)


if __name__ == '__main__':
  run()
