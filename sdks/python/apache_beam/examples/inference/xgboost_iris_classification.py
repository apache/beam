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

import argparse
import logging
from collections.abc import Callable
from collections.abc import Iterable
from typing import Union

import numpy
import pandas
import scipy
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split

import apache_beam as beam
import datatable
import xgboost
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerDatatable
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerNumpy
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerPandas
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerSciPy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


class PostProcessor(beam.DoFn):
  """Process the PredictionResult to get the predicted label.
  Returns a comma separated string with true label and predicted label.
  """
  def process(self, element: tuple[int, PredictionResult]) -> Iterable[str]:
    label, prediction_result = element
    prediction = prediction_result.inference
    yield '{},{}'.format(label, prediction)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_type',
      dest='input_type',
      required=True,
      choices=['numpy', 'pandas', 'scipy', 'datatable'],
      help='Datatype of the input data.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--model_state',
      dest='model_state',
      required=True,
      help='Path to the state of the XGBoost model loaded for Inference.')
  parser.add_argument(
      '--large_model',
      action='store_true',
      dest='large_model',
      default=False,
      help='Set to true if your model is large enough to run into memory '
      'pressure if you load multiple copies.')
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--split', action='store_true', dest='split')
  group.add_argument('--no_split', action='store_false', dest='split')
  return parser.parse_known_args(argv)


def load_sklearn_iris_test_data(
    data_type: Callable,
    split: bool = True,
    seed: int = 999) -> list[Union[numpy.array, pandas.DataFrame]]:
  """
    Loads test data from the sklearn Iris dataset in a given format,
    either in a single or multiple batches.
    Args:
      data_type: Datatype of the iris test dataset.
      split: Split the dataset in different batches or return single batch.
      seed: Random state for splitting the train and test set.
  """
  dataset = load_iris()
  _, x_test, _, _ = train_test_split(
      dataset['data'], dataset['target'], test_size=.2, random_state=seed)

  if split:
    return [(index, data_type(sample.reshape(1, -1)))
            for index, sample in enumerate(x_test)]
  return [(0, data_type(x_test))]


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

  data_types = {
      'numpy': (numpy.array, XGBoostModelHandlerNumpy),
      'pandas': (pandas.DataFrame, XGBoostModelHandlerPandas),
      'scipy': (scipy.sparse.csr_matrix, XGBoostModelHandlerSciPy),
      'datatable': (datatable.Frame, XGBoostModelHandlerDatatable),
  }

  input_data_type, model_handler = data_types[known_args.input_type]

  xgboost_model_handler = KeyedModelHandler(
      model_handler(
          model_class=xgboost.XGBClassifier,
          model_state=known_args.model_state,
          large_model=known_args.large_model))

  input_data = load_sklearn_iris_test_data(
      data_type=input_data_type, split=known_args.split)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  predictions = (
      pipeline
      | "ReadInputData" >> beam.Create(input_data)
      | "RunInference" >> RunInference(xgboost_model_handler)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
