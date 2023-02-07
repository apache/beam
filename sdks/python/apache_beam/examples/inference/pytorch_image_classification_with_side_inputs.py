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
import io
import logging
import os
from typing import Iterable
from typing import Tuple
from typing import Optional

import torchvision.models

import apache_beam as beam
import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window
from apache_beam.ml.inference.utils import WatchFilePattern
from PIL import Image
from torchvision import transforms


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> Tuple[str, Image.Image]:
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
      PytorchModelHandlerTensor(
          state_dict_path=known_args.model_path,
          model_class=torchvision.models.resnet152,
          model_params={'num_classes': 1000}))

  label_pixel_tuple = (
      pipeline
      | "ReadFromPubSub" >> beam.io.ReadFromPubSub(known_args.topic)
      | "ApplyMainInputWindow" >> beam.WindowInto(
          beam.transforms.window.FixedWindows(interval))
      | "ReadImageData" >>
      beam.Map(lambda image_name: read_image(image_file_name=image_name))
      | "PreprocessImages" >> beam.MapTuple(
          lambda file_name, data: (file_name, preprocess_image(data))))

  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(
          model_handler=model_handler, model_metadata_pcoll=si_pcoll)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  predictions | beam.Map(logging.info)

  # _ = predictions | "WriteOutput" >> beam.io.WriteToText(
  #     known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run().wait_until_finish()
  return result


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
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
      default='gs://apache-beam-ml/tmp/side_input_test/*.pth',
      help='Glob pattern to watch for an update.')
  parser.add_argument(
      '--interval',
      default=360,
      type=int,
      help='interval used to look for updates on a given file_pattern.')
  return parser.parse_known_args(argv)


if __name__ == '__main__':
  run()
