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

"""A pipeline to demonstrate per-entity training.

This pipeline reads data from a CSV file, that contains information
about 15 different attributes like salary >=50k, education level,
native country, age, occupation and others. The pipeline does some filtering
by selecting certain education level, discarding missing values and empty rows.
The pipeline then groups the rows based on education level and
trains Decision Trees for each group and finally saves them.
"""

import argparse
import logging
import pickle

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CreateKey(beam.DoFn):
  def process(self, element, *args, **kwargs):
    # 3rd column of the dataset is Education
    idx = 3
    key = element.pop(idx)
    yield (key, element)


def custom_filter(element):
  """Discard data point if contains ?,
  doesn't have all features, or
  doesn't have Bachelors, Masters or a Doctorate Degree"""
  return len(element) == 15 and '?' not in element \
      and ' Bachelors' in element or ' Masters' in element \
      or ' Doctorate' in element


class PrepareDataforTraining(beam.DoFn):
  """Preprocess data in a format suitable for training."""
  def process(self, element, *args, **kwargs):
    key, values = element
    #Convert to dataframe
    df = pd.DataFrame(values)
    last_ix = len(df.columns) - 1
    X, y = df.drop(last_ix, axis=1), df[last_ix]
    # select categorical and numerical features
    cat_ix = X.select_dtypes(include=['object', 'bool']).columns
    num_ix = X.select_dtypes(include=['int64', 'float64']).columns
    # label encode the target variable to have the classes 0 and 1
    y = LabelEncoder().fit_transform(y)
    yield (X, y, cat_ix, num_ix, key)


class TrainModel(beam.DoFn):
  """Takes preprocessed data as input,
  transforms categorical columns using OneHotEncoder,
  normalizes numerical columns and then
  fits a decision tree classifier.
  """
  def process(self, element, *args, **kwargs):
    X, y, cat_ix, num_ix, key = element
    steps = [('c', OneHotEncoder(handle_unknown='ignore'), cat_ix),
             ('n', MinMaxScaler(), num_ix)]
    # one hot encode categorical, normalize numerical
    ct = ColumnTransformer(steps)
    # wrap the model in a pipeline
    pipeline = Pipeline(steps=[('t', ct), ('m', DecisionTreeClassifier())])
    pipeline.fit(X, y)
    yield (key, pipeline)


class ModelSink(fileio.FileSink):
  def open(self, fh):
    self._fh = fh

  def write(self, record):
    _, trained_model = record
    pickled_model = pickle.dumps(trained_model)
    self._fh.write(pickled_model)

  def flush(self):
    self._fh.flush()


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      help='Path to the text file containing sentences.')
  parser.add_argument(
      '--output-dir',
      dest='output',
      required=True,
      help='Path of directory for saving trained models.')
  return parser.parse_known_args(argv)


def run(
    argv=None,
    save_main_session=True,
):
  """
  Args:
    argv: Command line arguments defined for this example.
    save_main_session: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline | "Read Data" >> beam.io.ReadFromText(known_args.input)
        | "Split data to make List" >> beam.Map(lambda x: x.split(','))
        | "Filter rows" >> beam.Filter(custom_filter)
        | "Create Key" >> beam.ParDo(CreateKey())
        | "Group by education" >> beam.GroupByKey()
        | "Prepare Data" >> beam.ParDo(PrepareDataforTraining())
        | "Train Model" >> beam.ParDo(TrainModel())
        |
        "Save" >> fileio.WriteToFiles(path=known_args.output, sink=ModelSink()))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
