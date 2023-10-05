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

"""A pipeline that uses OnlineClustering transform to group houses
with a similar value together.

This example uses the California Housing Prices dataset from kaggle.
https://www.kaggle.com/datasets/camnugent/california-housing-prices

In the first step of the pipeline, the clustering model is trained
using the OnlineKMeans transform, then the AssignClusterLabels
transform assigns a cluster to each record in the dataset. This
transform makes use of the RunInference API under the hood.

In order to run this example:
1. Download the data from kaggle as csv
2. Run `python california_housing_clustering.py --input <path/to/housing.csv> --checkpoints_path <path/to/checkpoints>`  # pylint: disable=line-too-long
"""

import argparse

import numpy as np

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import read_csv
from apache_beam.examples.online_clustering import AssignClusterLabelsInMemoryModel
from apache_beam.examples.online_clustering import OnlineClustering
from apache_beam.examples.online_clustering import OnlineKMeans
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='A csv file containing the data that needs to be clustered.')
  parser.add_argument(
      '--checkpoints_path',
      dest='checkpoints_path',
      required=True,
      help='A path to a directory where model checkpoints can be stored.')
  return parser.parse_known_args(argv)


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

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  data = pipeline | read_csv(known_args.input)

  features = ['longitude', 'latitude', 'median_income']

  housing_features = to_pcollection(data[features])

  # 1. Calculate clustering centers and save model to persistent storage
  model = (
      housing_features
      | beam.Map(lambda record: list(record))
      | "Train clustering model" >> OnlineClustering(
          OnlineKMeans,
          n_clusters=6,
          batch_size=256,
          cluster_args={},
          checkpoints_path=known_args.checkpoints_path))

  # 2. Calculate labels for all records in the dataset
  # using the trained clustering model using in memory model
  _ = (
      housing_features
      | beam.Map(lambda sample: np.array(sample))
      | "RunInference" >> AssignClusterLabelsInMemoryModel(
          model=pvalue.AsSingleton(model),
          model_id="kmeans",
          n_clusters=6,
          batch_size=512)
      | beam.Map(print))

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  run()
