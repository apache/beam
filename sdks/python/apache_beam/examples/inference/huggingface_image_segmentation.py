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

"""A pipeline that uses RunInference API to perform image segmentation."""

import argparse
import logging

import apache_beam as beam
from apache_beam.examples.inference import pytorch_image_segmentation
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from torchvision.models import resnet50


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to the text file containing image names.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path where to save output predictions.'
      ' text file.')
  parser.add_argument(
      '--repo_id',
      dest='repo_id',
      required=True,
      help="Name of the Hugging Face Models repository.")
  parser.add_argument(
      '--model',
      dest='model',
      required=True,
      help="Name of the weights file to load from Hugging Face repository.")
  parser.add_argument(
      '--images_dir',
      help='Path to the directory where images are stored.'
      'Not required if image names in the input file have absolute path.')
  return parser.parse_known_args(argv)


def run(
    argv=None,
    model_class=None,
    save_main_session=True,
    test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    model_class: Reference to the class definition of the model.
                If None, resnet will be used as default .
    model_params: Parameters passed to the constructor of the model_class.
                  These will be used to instantiate the model object in the
                  RunInference API.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  if not model_class:
    model_class = resnet50

  kwargs = {'model_class': model_class}
  model_handler = HuggingFaceModelHandlerTensor(
      repo_id=known_args.repo_id, filename=known_args.model, kwargs=kwargs)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  filename_value_pair = (
      pipeline
      | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
      | 'FilterEmptyLines' >> beam.ParDo(
          pytorch_image_segmentation.filter_empty_lines)
      | 'ReadImageData' >> beam.Map(
          lambda image_name: pytorch_image_segmentation.read_image(
              image_file_name=image_name, path_to_dir=known_args.images_dir))
      | 'PreprocessImages' >> beam.MapTuple(
          lambda file_name,
          data: (file_name, pytorch_image_segmentation.preprocess_image(data))))
  predictions = (
      filename_value_pair
      | 'PyTorchRunInference' >> RunInference(KeyedModelHandler(model_handler))
      |
      'ProcessOutput' >> beam.ParDo(pytorch_image_segmentation.PostProcessor()))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
