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

# beam-playground:
#   name: RunInferencePyTorch
#   description: Demonstrates the use of the RunInference transform for PyTorch.
#   multifile: false
#   context_line: 54
#   categories:
#     - Machine Learning
#   complexity: MEDIUM
#   tags:
#     - runinference
#     - pytorch

import argparse
import csv
import json
import os
import torch
from typing import Tuple

import apache_beam as beam
import numpy
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions

import logging
import warnings
warnings.filterwarnings('ignore')

save_model_dir_multiply_five = 'five_times_table_torch.pt'
save_model_dir_multiply_ten = 'ten_times_table_torch.pt'


# Create a linear regression model in PyTorch
class LinearRegression(torch.nn.Module):
    def __init__(self, input_dim=1, output_dim=1):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

    def forward(self, x):
        out = self.linear(x)
        return out


class PredictionProcessor(beam.DoFn):
    """
    A processor to format the output of the RunInference transform.
    """

    def process(
            self,
            element: PredictionResult):
        input_value = element.example
        output_value = element.inference
        yield (f"input is {input_value.item()} output is {output_value.item()}")

# Attach a key


class PredictionWithKeyProcessor(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)

    def process(
            self,
            element: Tuple[str, PredictionResult]):
        key = element[0]
        input_value = element[1].example
        output_value = element[1].inference
        yield (f"key: {key}, input: {input_value.item()} output: {output_value.item()}")


def main():
    # Prepare train and test data for an example model
    x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
    y = (x * 5).reshape(-1, 1)
    value_to_predict = numpy.array(
        [20, 40, 60, 90], dtype=numpy.float32).reshape(-1, 1)

    # Train the linear regression mode on 5 times data
    five_times_model = LinearRegression()
    optimizer = torch.optim.Adam(five_times_model.parameters())
    loss_fn = torch.nn.L1Loss()

    epochs = 10000
    tensor_x = torch.from_numpy(x)
    tensor_y = torch.from_numpy(y)
    for epoch in range(epochs):
        y_pred = five_times_model(tensor_x)
        loss = loss_fn(y_pred, tensor_y)
        five_times_model.zero_grad()
        loss.backward()
        optimizer.step()
    torch.save(five_times_model.state_dict(), save_model_dir_multiply_five)

    # Prepare train and test data for a 10 times model
    x = numpy.arange(0, 100, dtype=numpy.float32).reshape(-1, 1)
    y = (x * 10).reshape(-1, 1)

    ten_times_model = LinearRegression()
    optimizer = torch.optim.Adam(ten_times_model.parameters())
    loss_fn = torch.nn.L1Loss()

    epochs = 10000
    tensor_x = torch.from_numpy(x)
    tensor_y = torch.from_numpy(y)
    for epoch in range(epochs):
        y_pred = ten_times_model(tensor_x)
        loss = loss_fn(y_pred, tensor_y)
        ten_times_model.zero_grad()
        loss.backward()
        optimizer.step()
    torch.save(ten_times_model.state_dict(), save_model_dir_multiply_ten)

    print("\nUse RunInference within the pipeline:")
    torch_five_times_model_handler = PytorchModelHandlerTensor(
        state_dict_path=save_model_dir_multiply_five,
        model_class=LinearRegression,
        model_params={'input_dim': 1,
                      'output_dim': 1}
    )
    pipeline = beam.Pipeline()
    with pipeline as p:
        (
            p
            | "ReadInputData" >> beam.Create(value_to_predict)
            | "ConvertNumpyToTensor" >> beam.Map(torch.Tensor)
            | "RunInferenceTorch" >> RunInference(torch_five_times_model_handler)
            | beam.Map(print)
        )

    print("\nPostprocess RunInference results:")

    pipeline = beam.Pipeline()
    with pipeline as p:
        (
            p
            | "ReadInputData" >> beam.Create(value_to_predict)
            | "ConvertNumpyToTensor" >> beam.Map(torch.Tensor)
            | "RunInferenceTorch" >> RunInference(torch_five_times_model_handler)
            | "PostProcessPredictions" >> beam.ParDo(PredictionProcessor())
            | beam.Map(print)
        )

    # Use a CSV file as the source
    csv_values = [("first_question", 105.00),
                  ("second_question", 108.00),
                  ("third_question", 1000.00),
                  ("fourth_question", 1013.00)]
    input_csv_file = "./maths_problem.csv"

    with open(input_csv_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['key', 'value'])
        for row in csv_values:
            writer.writerow(row)

    pipeline_options = PipelineOptions()
    pipeline = beam.Pipeline(options=pipeline_options)

    keyed_torch_five_times_model_handler = KeyedModelHandler(
        torch_five_times_model_handler)

    with pipeline as p:
        df = p | beam.dataframe.io.read_csv(input_csv_file)
        pc = to_pcollection(df)
        (pc
         | "ConvertNumpyToTensor" >> beam.Map(lambda x: (x[0], torch.Tensor([x[1]])))
         | "RunInferenceTorch" >> RunInference(keyed_torch_five_times_model_handler)
         | "PostProcessPredictions" >> beam.ParDo(PredictionWithKeyProcessor())
         | beam.Map(print)
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
