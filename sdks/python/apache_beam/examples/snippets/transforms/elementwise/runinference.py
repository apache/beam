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
# pylint: disable=reimported

import torch


class LinearRegression(torch.nn.Module):
  def __init__(self, input_dim=1, output_dim=1):
    super().__init__()
    self.linear = torch.nn.Linear(input_dim, output_dim)

  def forward(self, x):
    out = self.linear(x)
    return out


def torch_unkeyed_model_handler(test=None):
  # [START torch_unkeyed_model_handler]
  import apache_beam as beam
  import numpy
  import torch
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor

  model_state_dict_path = 'gs://apache-beam-samples/run_inference/five_times_table_torch.pt'  # pylint: disable=line-too-long
  model_class = LinearRegression
  model_params = {'input_dim': 1, 'output_dim': 1}
  model_handler = PytorchModelHandlerTensor(
      model_class=model_class,
      model_params=model_params,
      state_dict_path=model_state_dict_path)

  unkeyed_data = numpy.array([10, 40, 60, 90],
                             dtype=numpy.float32).reshape(-1, 1)

  with beam.Pipeline() as p:
    predictions = (
        p
        | 'InputData' >> beam.Create(unkeyed_data)
        | 'ConvertNumpyToTensor' >> beam.Map(torch.Tensor)
        | 'PytorchRunInference' >> RunInference(model_handler=model_handler)
        | beam.Map(print))
    # [END torch_unkeyed_model_handler]
    if test:
      test(predictions)


def torch_keyed_model_handler(test=None):
  # [START torch_keyed_model_handler]
  import apache_beam as beam
  import torch
  from apache_beam.ml.inference.base import KeyedModelHandler
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor

  model_state_dict_path = 'gs://apache-beam-samples/run_inference/five_times_table_torch.pt'  # pylint: disable=line-too-long
  model_class = LinearRegression
  model_params = {'input_dim': 1, 'output_dim': 1}
  keyed_model_handler = KeyedModelHandler(
      PytorchModelHandlerTensor(
          model_class=model_class,
          model_params=model_params,
          state_dict_path=model_state_dict_path))

  keyed_data = [("first_question", 105.00), ("second_question", 108.00),
                ("third_question", 1000.00), ("fourth_question", 1013.00)]

  with beam.Pipeline() as p:
    predictions = (
        p
        | 'KeyedInputData' >> beam.Create(keyed_data)
        | "ConvertIntToTensor" >>
        beam.Map(lambda x: (x[0], torch.Tensor([x[1]])))
        | 'PytorchRunInference' >>
        RunInference(model_handler=keyed_model_handler)
        | beam.Map(print))
    # [END torch_keyed_model_handler]
    if test:
      test(predictions)
