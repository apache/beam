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

"""A pipeline that uses RunInference API on a regression about housing prices.

This example uses the japanese housing data from kaggle.
https://www.kaggle.com/datasets/nishiodens/japan-real-estate-transaction-prices

Since the data has missing fields, this example illustrates how to split
data and assign it to the models that are trained on different subsets of
features. The predictions are then recombined.

In order to set this example up, you will need two things.
1. Build models (or use ours) and reference those via the model directory.
2. Download the data from kaggle and host it.
"""

import argparse
from typing import Iterable

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import pandas

MODELS = [{
    'name': 'all_features',
    'required_features': [
        'Area',
        'Year',
        'MinTimeToNearestStation',
        'MaxTimeToNearestStation',
        'TotalFloorArea',
        'Frontage',
        'Breadth',
        'BuildingYear'
    ]
},
          {
              'name': 'floor_area',
              'required_features': ['Area', 'Year', 'TotalFloorArea']
          },
          {
              'name': 'stations',
              'required_features': [
                  'Area',
                  'Year',
                  'MinTimeToNearestStation',
                  'MaxTimeToNearestStation'
              ]
          }, {
              'name': 'no_features', 'required_features': ['Area', 'Year']
          }]


def sort_by_features(dataframe, max_size):
  """ Partitions the dataframe by what data it has available."""
  for i, model in enumerate(MODELS):
    required_features = dataframe[model['required_features']]
    if not required_features.isnull().any().any():
      return i
  return -1


class LoadDataframe(beam.DoFn):
  def process(self, file_name: str) -> Iterable[pandas.DataFrame]:
    """ Loads data files as a pandas dataframe."""
    file = FileSystems.open(file_name, 'rb')
    dataframe = pandas.read_csv(file)
    for i in range(dataframe.shape[0]):
      yield dataframe.iloc[[i]]


def report_predictions(prediction_result):
  true_result = prediction_result.example['TradePrice'].values[0]
  inference = prediction_result.inference
  return 'True Price %.1f, Predicted Price %f' % (true_result, inference)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='A metadata file with all models, with references to models and '
      'information about all the files and data.')
  parser.add_argument(
      '--model_path',
      dest='model_path',
      required=True,
      help='A path from where all models can be read.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path to save output predictions.')
  return parser.parse_known_args(argv)


def inference_transform(model_name, model_path):
  # These sklearn models are a pipeline that use pandas.
  model_filename = model_path + model_name + '.pickle'
  model_loader = SklearnModelHandlerPandas(
      model_file_type=ModelFileType.PICKLE, model_uri=model_filename)
  transform_name = 'RunInference ' + model_name
  return transform_name >> RunInference(model_loader)


def run(argv=None, save_main_session=True):
  """Entry point. Defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:
    # This example uses a single file, but it is possible to use many files.
    file_names = p | 'FileNames' >> beam.Create([known_args.input])
    loaded_data = file_names | beam.ParDo(LoadDataframe())

    # Some examples don't have all features. Pipelines
    # that expect those fields will fail. There are many ways to deal with
    # missing data. This example illustrates how to assign predictions to
    # different models depending upon what data is available.
    [all, floor_area, stations, no_features] = (
        loaded_data
        | 'Partition' >> beam.Partition(sort_by_features, len(MODELS)))

    model_path = known_args.model_path
    prediction_1 = all | inference_transform('all_features', model_path)
    prediction_2 = floor_area | inference_transform('floor_area', model_path)
    prediction_3 = stations | inference_transform('stations', model_path)
    prediction_4 = no_features | inference_transform('no_features', model_path)

    all_predictions = (prediction_1, prediction_2, prediction_3, prediction_4)
    flattened_predictions = all_predictions | 'Flatten' >> beam.Flatten()
    predictions = (
        flattened_predictions
        | 'AllPredictions' >> beam.Map(report_predictions))
    _ = predictions | "WriteOutput" >> beam.io.WriteToText(
        known_args.output, append_trailing_newlines=True)


if __name__ == '__main__':
  run()
