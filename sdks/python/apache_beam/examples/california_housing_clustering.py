import argparse

import numpy as np

import apache_beam as beam
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import read_csv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms.online_clustering import OnlineClustering
from apache_beam.transforms.online_clustering import OnlineKMeansClustering


def parse_known_args(argv):
    """Parses args for the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='A csv file containing the data that needs to be clustered.'
    )
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

    data = (
        pipeline
        | read_csv(known_args.input)
    )

    features = ['longitude', 'latitude', 'median_income']

    housing_features = to_pcollection(data[features])

    _ = (
        housing_features
        | beam.Map(lambda record: list(record))
        | OnlineClustering(OnlineKMeansClustering, n_clusters=6, batch_size=256)
    )

    result = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
    run()
