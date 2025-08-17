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
A pipeline that uses RunInference PTransform to perform image classification
and uses WatchFilePattern as side input to the RunInference PTransform.
WatchFilePattern is used to watch for a file updates matching the file_pattern
based on timestamps and emits latest model metadata, which is used in
RunInference API for the dynamic model updates without the need for stopping
the beam pipeline.

This pipeline follows the pattern from
https://beam.apache.org/documentation/patterns/side-inputs/

To use the PubSub reading from a topic in the pipeline as source, you can
publish a path to the model(resnet152 used in the pipeline from
torchvision.models.resnet152) to the PubSub topic. Then pass that
topic via command line arg --topic.  The published path(str) should be
UTF-8 encoded.

To run the example on DataflowRunner,

python apache_beam/examples/inference/pytorch_image_classification_with_side_inputs.py # pylint: disable=line-too-long
  --project=<your-project>
  --re=<your-region>
  --temp_location=<your-tmp-location>
  --staging_location=<your-staging-location>
  --runner=DataflowRunner
  --streaming
  --interval=10
  --num_workers=5
  --requirements_file=apache_beam/ml/inference/torch_tests_requirements.txt
  --topic=<pubsub_topic>
  --file_pattern=<glob_pattern>

file_pattern is path(can contain glob characters), which will be passed to
WatchContinuously transform for model updates. WatchContinuously watches the
file_pattern and emits a latest file path, sorted by timestamp. Files that
are read before and updated with same name will be ignored as an update.

The pipeline expects there is at least one file present to match the
file_pattern before pipeline startup. Presumably, this would be the
`initial_model_path`. If there is no file matching before pipeline
startup time, the pipeline would fail.
"""

import argparse
import io
import logging
import os
from collections.abc import Iterable
from collections.abc import Iterator
from typing import Optional

import apache_beam as beam
import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.utils import WatchFilePattern
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


class PostProcessor(beam.DoFn):
  """
  Return filename, prediction and the model id used to perform the
  prediction
  """
  def process(self, element: tuple[str, PredictionResult]) -> Iterable[str]:
    filename, prediction_result = element
    prediction = torch.argmax(prediction_result.inference, dim=0)
    yield filename, prediction, prediction_result.model_id


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--topic',
      dest='topic',
      help='PubSub topic emitting absolute path to the images.'
      'Path must be accessible by the pipeline.')
  parser.add_argument(
      '--model_path',
      '--initial_model_path',
      dest='model_path',
      default='gs://apache-beam-samples/run_inference/resnet152.pth',
      help="Path to the initial model's state_dict. "
      "This will be used until the first model update occurs.")
  parser.add_argument(
      '--file_pattern', help='Glob pattern to watch for an update.')
  parser.add_argument(
      '--interval',
      default=10,
      type=int,
      help='Interval used to check for file updates.')

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
                  RunInference PTransform.
    save_main_session: Used for internal testing.
    device: Device to be used on the Runner. Choices are (CPU, GPU).
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  if not model_class:
    model_class = models.resnet152
    model_params = {'num_classes': 1000}

  # In this example we pass keyed inputs to RunInference transform.
  # Therefore, we use KeyedModelHandler wrapper over PytorchModelHandler.
  model_handler = KeyedModelHandler(
      PytorchModelHandlerTensor(
          state_dict_path=known_args.model_path,
          model_class=model_class,
          model_params=model_params,
          device=device,
          min_batch_size=10,
          max_batch_size=100))

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  side_input = pipeline | WatchFilePattern(
      interval=known_args.interval, file_pattern=known_args.file_pattern)

  filename_value_pair = (
      pipeline
      | 'ReadImageNamesFromPubSub' >> beam.io.ReadFromPubSub(known_args.topic)
      | 'DecodeBytes' >> beam.Map(lambda x: x.decode('utf-8'))
      | 'ReadImageData' >>
      beam.Map(lambda image_name: read_image(image_file_name=image_name))
      | 'PreprocessImages' >> beam.MapTuple(
          lambda file_name, data: (file_name, preprocess_image(data))))
  predictions = (
      filename_value_pair
      | 'PyTorchRunInference' >> RunInference(
          model_handler, model_metadata_pcoll=side_input)
      | 'ProcessOutput' >> beam.ParDo(PostProcessor()))

  _ = predictions | beam.Map(logging.info)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
