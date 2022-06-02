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

"""Pipeline that uses RunInference API to perform classification task on imagenet dataset"""  # pylint: disable=line-too-long

import argparse
import io
import os
from functools import partial
from typing import Any
from typing import Iterable
from typing import Tuple
from typing import Union

import apache_beam as beam
import torch
import torchvision
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.api import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelLoader
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from PIL import Image
from torchvision import transforms


def read_image(image_file_name: str,
               path_to_dir: str = None) -> Tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(data: Image) -> torch.Tensor:
  image_size = (224, 224)
  # to use pretrained models in torch with imagenet weights,
  # normalize the images using the below values.
  # ref: https://pytorch.org/vision/stable/models.html#
  normalize = transforms.Normalize(
      mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
  transform = transforms.Compose([
      transforms.Resize(image_size),
      transforms.ToTensor(),
      normalize,
  ])
  return transform(data)


class PostProcessor(beam.DoFn):
  def process(
      self, element: Union[PredictionResult, Tuple[Any, PredictionResult]]
  ) -> Iterable[str]:
    filename, prediction_result = element
    prediction = torch.argmax(prediction_result.inference, dim=0)
    yield filename + ',' + str(prediction.item())


def run_pipeline(options: PipelineOptions, args=None):
  """Sets up PyTorch RunInference pipeline"""
  # class definition of the model.
  model_class = torchvision.models.mobilenet_v2
  # params for model class constructor. These values will be used in
  # RunInference API to instantiate the model object.
  model_params = {'pretrained': False}
  model_loader = PytorchModelLoader(
      state_dict_path=args.model_state_dict_path,
      model_class=model_class,
      model_params=model_params)
  with beam.Pipeline(options=options) as p:
    filename_value_pair = (
        p
        | 'Read from csv file' >> beam.io.ReadFromText(
            args.input, skip_header_lines=1)
        | 'Parse and read files from the input_file' >> beam.Map(
            partial(read_image, path_to_dir=args.images_dir))
        | 'Preprocess images' >> beam.MapTuple(
            lambda file_name, data: (file_name, preprocess_image(data))))
    predictions = (
        filename_value_pair
        | 'PyTorch RunInference' >> RunInference(model_loader)
        | 'Process output' >> beam.ParDo(PostProcessor()))

    if args.output:
      predictions | "Write output to GCS" >> beam.io.WriteToText( # pylint: disable=expression-not-assigned
        args.output,
        shard_name_template='',
        append_trailing_newlines=True)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to the CSV file containing image names')
  parser.add_argument(
      '--output',
      dest='output',
      help='Predictions are saved to the output'
      ' text file.')
  parser.add_argument(
      '--model_state_dict_path',
      dest='model_state_dict_path',
      required=True,
      help='Path to load the model.')
  parser.add_argument(
      '--images_dir',
      default=None,
      help='Path to the directory where images are stored.'
      'This is not required if the --input has absolute path to the images.')
  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Entry point. Defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  run_pipeline(pipeline_options, args=known_args)


if __name__ == '__main__':
  run()
