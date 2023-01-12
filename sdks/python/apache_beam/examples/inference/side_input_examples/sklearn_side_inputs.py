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

"""
This file is WIP.

TODOs:
1. Change the file name.
2. Add unbounded source for Main input PCollection.
"""

import argparse
from typing import Iterable
from typing import List
from typing import Tuple

import apache_beam as beam
from apache_beam.io.fileio import MatchContinuously
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window
from apache_beam.transforms import trigger


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


# we need a side input that emits model path.
def get_model_path_side_input(pipeline, file_pattern):
  model_path = (
      pipeline
      | 'MatchContinuously' >> MatchContinuously(
          file_pattern=file_pattern, interval=30)
      | 'ApplyGlobalWindow' >> beam.transforms.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.Repeatedly(trigger.AfterCount(1)),
          accumulation_mode=trigger.AccumulationMode.DISCARDING))
  return beam.pvalue.AsIter(model_path)


def run(argv=None, save_main_session=True, test_pipeline=None):

  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)
  # side input pcollection that looks for any files matching file_pattern,
  si_pcoll = get_model_path_side_input(
      pipeline, file_pattern='gs://apache-beam-ml/tmp/side_input_test/*')

  model_handler = KeyedModelHandler(
      SklearnModelHandlerNumpy(model_uri=known_args.model_path))

  label_pixel_tuple = (
      pipeline
      # Implement unbounded source.
      | "ReadFromText" >> beam.io.ReadFromText(known_args.input)
      | "ApplyMainInputWindow" >> beam.WindowInto(
          beam.transforms.window.FixedWindows(1))
      | "PreProcessInputs" >> beam.Map(process_input))

  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(
          model_handler=model_handler, update_model_pcoll=si_pcoll)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  # predictions | beam.Map(print)
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
  return parser.parse_known_args(argv)


if __name__ == '__main__':
  run()
