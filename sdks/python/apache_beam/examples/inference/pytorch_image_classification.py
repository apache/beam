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
import io
import logging
import os
from collections.abc import Iterator
from typing import Optional

import apache_beam as beam
import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from PIL import Image
from torchvision import models
from torchvision import transforms


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(data: Image.Image) -> torch.Tensor:
  image_size = (224, 224)
  # Pre-trained PyTorch models expect input images normalized with the
  # below values (see: https://pytorch.org/vision/stable/models.html)
  normalize = transforms.Normalize(
      mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
  transform = transforms.Compose([
      transforms.Resize(image_size),
      transforms.ToTensor(),
      normalize,
  ])
  return transform(data)


def filter_empty_lines(text: str) -> Iterator[str]:
  if len(text.strip()) > 0:
    yield text


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
      '--model_state_dict_path',
      dest='model_state_dict_path',
      required=True,
      help="Path to the model's state_dict.")
  parser.add_argument(
      '--images_dir',
      default=None,
      help='Path to the directory where images are stored.'
      'Not required if image names in the input file have absolute path.')
  return parser.parse_known_args(argv)


def run(
    argv=None,
    model_class=None,
    model_params=None,
    save_main_session=True,
    device='CPU',
    test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    model_class: Reference to the class definition of the model.
    model_params: Parameters passed to the constructor of the model_class.
                  These will be used to instantiate the model object in the
                  RunInference API.
    save_main_session: Used for internal testing.
    device: Device to be used on the Runner. Choices are (CPU, GPU).
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  if not model_class:
    # default model class will be mobilenet with pretrained weights.
    model_class = models.mobilenet_v2
    model_params = {'num_classes': 1000}

  def preprocess(image_name: str) -> tuple[str, torch.Tensor]:
    image_name, image = read_image(
      image_file_name=image_name,
      path_to_dir=known_args.images_dir)
    return (image_name, preprocess_image(image))

  def postprocess(element: tuple[str, PredictionResult]) -> str:
    filename, prediction_result = element
    prediction = torch.argmax(prediction_result.inference, dim=0)
    return filename + ',' + str(prediction.item())

  # In this example we pass keyed inputs to RunInference transform.
  # Therefore, we use KeyedModelHandler wrapper over PytorchModelHandler.
  model_handler = KeyedModelHandler(
      PytorchModelHandlerTensor(
          state_dict_path=known_args.model_state_dict_path,
          model_class=model_class,
          model_params=model_params,
          device=device,
          min_batch_size=10,
          max_batch_size=100)).with_preprocess_fn(
              preprocess).with_postprocess_fn(postprocess)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  filename_value_pair = (
      pipeline
      | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
      | 'FilterEmptyLines' >> beam.ParDo(filter_empty_lines))
  predictions = (
      filename_value_pair
      | 'PyTorchRunInference' >> RunInference(model_handler))

  predictions | "WriteOutputToGCS" >> beam.io.WriteToText( # pylint: disable=expression-not-assigned
    known_args.output,
    shard_name_template='',
    append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
