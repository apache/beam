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
import os
import uuid
from collections.abc import Iterable
from google.genai.types import GenerateContentResponse
from io import BytesIO
from PIL import Image
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import PredictionResult, RunInference
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
from apache_beam.ml.inference.gemini_inference import generate_image_from_strings_and_images
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.runner import PipelineResult


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
      response: GenerateContentResponse = element.inference
      for part in response.candidates[0].content.parts:
        if part.text is not None:
          print(part.text)
        elif part.inline_data is not None:
          image = Image.open(BytesIO(part.inline_data.data))
          yield image
    except Exception as e:
      print(f"Can't decode inference for element: {element.example}, got {e}")
      raise e


class _WriteImageFn(beam.DoFn):
  """
    A DoFn that writes a single PIL.Image.Image object to a file.

    This DoFn is intended to be used internally by the WriteImages transform.
  """
  def __init__(self, output_dir: str, filename_prefix: str):
    """
        Initializes the _WriteImageFn.

        Args:
            output_dir: The directory where the image files will be written.
            filename_prefix: A prefix to use for the output filenames. A UUID
                and the .png extension will be appended to this prefix.
    """
    self._output_dir = output_dir
    self._filename_prefix = filename_prefix

  def setup(self):
    """
        Ensures the output directory exists.
    """
    FileSystems().mkdirs(self._output_dir)

  def process(self, image: Image.Image):
    """
        Saves the given PIL Image to a PNG file.

        Args:
            image: A PIL.Image.Image object.
    """
    # Generate a unique filename to prevent collisions.
    unique_id = uuid.uuid4()
    filename = f"{self._filename_prefix}-{unique_id}.png"
    output_path = os.path.join(self._output_dir, filename)

    try:
      logging.debug("Writing image to %s", output_path)
      with FileSystems().create(output_path) as image_file:
        image.save(image_file, "PNG")
    except Exception as e:
      logging.error("Failed to write image to %s: %s", output_path, e)
      raise


class WriteImages(beam.PTransform):
  """
    An Apache Beam PTransform for writing a PCollection of PIL Image objects
    to individual PNG files in a specified directory.
  """
  def __init__(self, output_dir: str, filename_prefix: Optional[str] = "image"):
    self._output_dir = output_dir
    self._filename_prefix = filename_prefix

  def expand(self, pcoll: beam.PCollection) -> beam.pvalue.PDone:
    return (
        pcoll
        | "WriteImageToFile" >> beam.ParDo(
            _WriteImageFn(self._output_dir, self._filename_prefix)))


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
  _ = processed | "WriteOutput" >> WriteImages(
      output_dir=known_args.output, filename_prefix="gemini_image")

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
