# coding=utf-8
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

# pytype: skip-file


def images(test=None):
  # [START images]
import apache_beam as beam
import torch
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from torchvision.models.mobilenetv2 import MobileNetV2

model_class = MobileNetV2
model_params = {'num_classes': 1000}

model_handler = KeyedModelHandler(
   PytorchModelHandlerTensor(
     state_dict_path=TODO,
     model_class=model_class,
     model_params=model_params))

with beam.Pipeline() as pipeline:
    images = (
        p
        | 'CreateImages' >> beam.Create([
            (
              'img1',
              torch.Tensor([
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]], 
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]],
                [[-1.5, -1, -.5],[-.5, 0, .5],[.5, 1, 1.5]]
              ])
            ),
        ])
    )
    predictions = (
        images
        | 'PyTorchRunInference' >> RunInference(model_handler)
        | 'ProcessOutput' >> beam.Map(print))
    # [END images]
    if test:
      test(plants)


def digits(test=None):
  # pylint: disable=line-too-long
  # [START digits]
  import apache_beam as beam

  class AnalyzeElement(beam.DoFn):
    def process(
        self,
        elem,
        timestamp=beam.DoFn.TimestampParam,
        window=beam.DoFn.WindowParam):
      yield '\n'.join([
          '# timestamp',
          'type(timestamp) -> ' + repr(type(timestamp)),
          'timestamp.micros -> ' + repr(timestamp.micros),
          'timestamp.to_rfc3339() -> ' + repr(timestamp.to_rfc3339()),
          'timestamp.to_utc_datetime() -> ' + repr(timestamp.to_utc_datetime()),
          '',
          '# window',
          'type(window) -> ' + repr(type(window)),
          'window.start -> {} ({})'.format(
              window.start, window.start.to_utc_datetime()),
          'window.end -> {} ({})'.format(
              window.end, window.end.to_utc_datetime()),
          'window.max_timestamp() -> {} ({})'.format(
              window.max_timestamp(), window.max_timestamp().to_utc_datetime()),
      ])

  with beam.Pipeline() as pipeline:
    dofn_params = (
        pipeline
        | 'Create a single test element' >> beam.Create([':)'])
        | 'Add timestamp (Spring equinox 2020)' >>
        beam.Map(lambda elem: beam.window.TimestampedValue(elem, 1584675660))
        |
        'Fixed 30sec windows' >> beam.WindowInto(beam.window.FixedWindows(30))
        | 'Analyze element' >> beam.ParDo(AnalyzeElement())
        | beam.Map(print))
    # [END digits]
    # pylint: enable=line-too-long
    if test:
      test(digits)