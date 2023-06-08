#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: used-model
#   description: Using models for ml
#   multifile: true
#   files:
#     - name: five_times_table_torch.pt
#     - name: ten_times_table_torch.pt
#   context_line: 45
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

from typing import Tuple

import apache_beam as beam
import torch
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions

save_model_dir_multiply_five = 'five_times_table_torch.pt'
save_model_dir_multiply_ten = 'ten_times_table_torch.pt'


class LinearRegression(torch.nn.Module):
    def __init__(self, input_dim=1, output_dim=1):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

    def forward(self, x):
        out = self.linear(x)
        return out


class PredictionWithKeyProcessor(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, element: Tuple[str, PredictionResult]):
        key = element[0]
        input_value = element[1].example
        output_value = element[1].inference
        yield (f"key: {key}, input: {input_value.item()} output: {output_value.item()}")


torch_ten_times_model_handler = PytorchModelHandlerTensor(state_dict_path=save_model_dir_multiply_ten,
                                                          model_class=LinearRegression,
                                                          model_params={'input_dim': 1,
                                                                        'output_dim': 1}
                                                          )
keyed_torch_ten_times_model_handler = KeyedModelHandler(torch_ten_times_model_handler)

torch_five_times_model_handler = PytorchModelHandlerTensor(
    state_dict_path=save_model_dir_multiply_five,
    model_class=LinearRegression,
    model_params={'input_dim': 1,
                  'output_dim': 1}
)

keyed_torch_five_times_model_handler = KeyedModelHandler(torch_five_times_model_handler)

pipeline_options = PipelineOptions().from_dictionary(
    {'temp_location': f'gs://btestq/xasv.csv'})
pipeline = beam.Pipeline(options=pipeline_options)


class TensorDoFn(beam.DoFn):
    def process(self, element):
        yield (element, torch.Tensor([int(element) * 10]))


def process_interim_inference(element):
    key, prediction_result = element
    input_value = prediction_result.example
    inference = prediction_result.inference
    formatted_input_value = 'original input is `{} {}`'.format(key, input_value)
    return formatted_input_value, inference


with pipeline as p:
    multiply_five = (
            p | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
        table='apache-beam-testing:clouddataflow_samples.weather_stations',
        method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
            | beam.combiners.Sample.FixedSizeGlobally(5)
            | beam.FlatMap(lambda line: line)
            | beam.Map(lambda element: element['mean_wind_speed'])
            | beam.ParDo(TensorDoFn())
            | "RunInferenceTorchFiveTuple" >> RunInference(keyed_torch_five_times_model_handler)
    )

    inference_result = (
            multiply_five
            | "ExtractResult" >> beam.Map(process_interim_inference)
            | "RunInferenceTorchTenTuple" >> RunInference(keyed_torch_ten_times_model_handler)
            | beam.ParDo(PredictionWithKeyProcessor())
    )
    inference_result | beam.Map(print)
