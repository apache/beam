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

"""A streaming pipeline that uses RunInference API and windowing that classifies
the quality of milk as good, bad or medium based on pH, temperature,
taste, odor, fat, turbidity and color. Each minute new measurements come in
and a sliding window aggregates the number of good, bad and medium
samples.

This example uses the milk quality prediction dataset from kaggle.
https://www.kaggle.com/datasets/cpluzshrijayan/milkquality


In order to set this example up, you will need two things.
1. Download the data in csv format from kaggle and host it.
2. Split the dataset in a training set and test set (preprocess_data function).
3. Train the classifier.
"""

import argparse
import logging
import time
from typing import NamedTuple

import pandas
from sklearn.model_selection import train_test_split

import apache_beam as beam
import xgboost
from apache_beam import window
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerPandas
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.testing.test_stream import TestStream


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--dataset',
      dest='dataset',
      required=True,
      help='Path to the csv containing Kaggle Milk Quality dataset.')
  parser.add_argument(
      '--pipeline_input_data',
      dest='pipeline_input_data',
      required=True,
      help='Path to store the csv containing input data for the pipeline.'
      'This will be generated as part of preprocessing the data.')
  parser.add_argument(
      '--training_set',
      dest='training_set',
      required=True,
      help='Path to store the csv containing the training set.'
      'This will be generated as part of preprocessing the data.')
  parser.add_argument(
      '--labels',
      dest='labels',
      required=True,
      help='Path to store the csv containing the labels used in training.'
      'This will be generated as part of preprocessing the data.')
  parser.add_argument(
      '--model_state',
      dest='model_state',
      required=True,
      help='Path to the state of the XGBoost model loaded for Inference.')
  return parser.parse_known_args(argv)


def preprocess_data(
    dataset_path: str,
    training_set_path: str,
    labels_path: str,
    test_set_path: str):
  """
    Helper function to split the dataset into a training set
    and its labels and a test set. The training set and
    its labels are used to train a lightweight model.
    The test set is used to create a test streaming pipeline.
    Args:
        dataset_path: path to csv file containing the Kaggle
         milk quality dataset
        training_set_path: path to output the training samples
        labels_path:  path to output the labels for the training set
        test_set_path: path to output the test samples
    """
  df = pandas.read_csv(dataset_path)
  df['Grade'].replace(['low', 'medium', 'high'], [0, 1, 2], inplace=True)
  x = df.drop(columns=['Grade'])
  y = df['Grade']
  x_train, x_test, y_train, _ = \
      train_test_split(x, y, test_size=0.60, random_state=99)
  x_train.to_csv(training_set_path, index=False)
  y_train.to_csv(labels_path, index=False)
  x_test.to_csv(test_set_path, index=False)


def train_model(
    samples_path: str, labels_path: str, model_state_output_path: str):
  """Function to train the XGBoost model.
    Args:
      samples_path: path to csv file containing the training data
      labels_path: path to csv file containing the labels for the training data
      model_state_output_path: Path to store the trained model
  """
  samples = pandas.read_csv(samples_path)
  labels = pandas.read_csv(labels_path)
  xgb = xgboost.XGBClassifier(max_depth=3)
  xgb.fit(samples, labels)
  xgb.save_model(model_state_output_path)
  return xgb


class MilkQualityAggregation(NamedTuple):
  bad_quality_measurements: int
  medium_quality_measurements: int
  high_quality_measurements: int


class AggregateMilkQualityResults(beam.CombineFn):
  """Simple aggregation to keep track of the number
   of samples with good, bad and medium quality milk."""
  def create_accumulator(self):
    return MilkQualityAggregation(0, 0, 0)

  def add_input(
      self, accumulator: MilkQualityAggregation, element: PredictionResult):
    quality = element.inference[0]
    if quality == 0:
      return MilkQualityAggregation(
          accumulator.bad_quality_measurements + 1,
          accumulator.medium_quality_measurements,
          accumulator.high_quality_measurements)
    elif quality == 1:
      return MilkQualityAggregation(
          accumulator.bad_quality_measurements,
          accumulator.medium_quality_measurements + 1,
          accumulator.high_quality_measurements)
    else:
      return MilkQualityAggregation(
          accumulator.bad_quality_measurements,
          accumulator.medium_quality_measurements,
          accumulator.high_quality_measurements + 1)

  def merge_accumulators(self, accumulators: MilkQualityAggregation):
    return MilkQualityAggregation(
        sum(
            aggregation.bad_quality_measurements
            for aggregation in accumulators),
        sum(
            aggregation.medium_quality_measurements
            for aggregation in accumulators),
        sum(
            aggregation.high_quality_measurements
            for aggregation in accumulators),
    )

  def extract_output(self, accumulator: MilkQualityAggregation):
    return accumulator


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

  milk_quality_data = pandas.read_csv(known_args.pipeline_input_data)

  start = time.mktime(time.strptime('2023/06/29 10:00:00', '%Y/%m/%d %H:%M:%S'))

  # Create a test stream
  test_stream = TestStream()

  # Watermark is set to 10:00:00
  test_stream.advance_watermark_to(start)

  # Split the dataframe in individual samples
  samples = [
      milk_quality_data.iloc[i:i + 1] for i in range(len(milk_quality_data))
  ]

  for watermark_offset, sample in enumerate(samples, 1):
    test_stream.advance_watermark_to(start + watermark_offset)
    test_stream.add_elements([sample])

  test_stream.advance_watermark_to_infinity()

  model_handler = XGBoostModelHandlerPandas(
      model_class=xgboost.XGBClassifier, model_state=known_args.model_state)

  with beam.Pipeline() as p:
    _ = (
        p | test_stream
        | 'window' >> beam.WindowInto(window.SlidingWindows(30, 5))
        | "RunInference" >> RunInference(model_handler)
        | 'Count number of elements in window' >> beam.CombineGlobally(
            AggregateMilkQualityResults()).without_defaults()
        | 'Print' >> beam.Map(print))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)

  known_args, _ = parse_known_args(None)

  preprocess_data(
      known_args.dataset,
      training_set_path=known_args.training_set,
      labels_path=known_args.labels,
      test_set_path=known_args.pipeline_input_data)
  train_model(
      samples_path=known_args.training_set,
      labels_path=known_args.labels,
      model_state_output_path=known_args.model_state)
  run()
