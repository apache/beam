import argparse

import numpy as np

import apache_beam as beam
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import read_csv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.examples.online_clustering import OnlineClustering, AssignClusterLabels
from apache_beam.examples.online_clustering import OnlineKMeans


"""A pipeline that uses OnlineClustering transform to group houses 
with a similar value together.

This example uses the California Housing Prices dataset from kaggle.
https://www.kaggle.com/datasets/camnugent/california-housing-prices

In the first step of the pipeline, the clustering model is trained
using the OnlineKMeans transform, then the AssignClusterLabels
transform assigns a cluster to each record in the dataset. This
transform makes use of the RunInference API under the hood.

In order to set this example up, you will need one thing.
1. Download the data from kaggle as csv

california_housing_clustering.py --input /tmp/housing.csv
"""


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='A csv file containing the data that needs to be clustered.')
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
  _ = (
      housing_features
      | beam.Map(lambda record: list(record))
      | "Train clustering model" >> OnlineClustering(
          OnlineKMeans,
          n_clusters=6,
          batch_size=256,
          cluster_args={},
          checkpoints_path='/tmp/checkpoints'))

  # 2. Calculate labels for all records in the dataset
  # using the trained clustering model
  _ = (
          housing_features
          | beam.Map(lambda sample: np.array(sample))
          | "RunInference" >>
          AssignClusterLabels(checkpoints_path='/tmp/checkpoints')
          | beam.Map(print))

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  run()
