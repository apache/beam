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

""" A sample pipeline using the RunInference API to classify text using an LLM.
This pipeline creates a set of prompts and sends it to a Gemini service then
returns the predictions from the classifier model. This example uses the
gemini-2.0-flash-001 model.
"""

import argparse
import logging
from collections.abc import Iterable
from io import BytesIO

import apache_beam as beam
from apache_beam.io.fileio import FileSink
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io.fileio import default_file_naming
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
from apache_beam.ml.inference.gemini_inference import generate_image_from_strings_and_images
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from PIL import Image


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      type=str,
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--api_key',
      dest='api_key',
      type=str,
      required=False,
      help='Gemini Developer API key.')
  parser.add_argument(
      '--cloud_project',
      dest='project',
      type=str,
      required=False,
      help='GCP Project')
  parser.add_argument(
      '--cloud_region',
      dest='location',
      type=str,
      required=False,
      help='GCP location for the Endpoint')
  return parser.parse_known_args(argv)


class PostProcessor(beam.DoFn):
  def process(self, element: PredictionResult) -> Iterable[Image.Image]:
    try:
      response = element.inference
      for part in response.parts:
        if part.text is not None:
          print(part.text)
        elif part.inline_data is not None:
          image = Image.open(BytesIO(part.inline_data.data))
          yield image
    except Exception as e:
      print(f"Can't decode inference for element: {element.example}, got {e}")
      raise e


class ImageSink(FileSink):
  def open(self, fh) -> None:
    self._fh = fh

  def write(self, record):
    record.save(self._fh, format='PNG')

  def flush(self):
    self._fh.flush()


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
  model_handler = GeminiModelHandler(
      model_name='gemini-2.5-flash-image',
      request_fn=generate_image_from_strings_and_images,
      api_key=known_args.api_key,
      project=known_args.project,
      location=known_args.location)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  prompts = [
      "Create a picture of a pineapple in the sand at a beach.",
  ]

  read_prompts = pipeline | "Get prompt" >> beam.Create(prompts)
  predictions = read_prompts | "RunInference" >> RunInference(model_handler)
  processed = predictions | "PostProcess" >> beam.ParDo(PostProcessor())
  _ = processed | "WriteOutput" >> WriteToFiles(
      path=known_args.output,
      file_naming=default_file_naming("gemini-image", ".png"),
      sink=ImageSink())

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
