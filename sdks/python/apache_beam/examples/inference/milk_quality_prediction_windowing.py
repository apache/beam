import argparse
import logging
import time
from typing import NamedTuple

import pandas
import pandas as pd
import xgboost

from sklearn.model_selection import train_test_split

import apache_beam as beam
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerPandas
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.testing.test_stream import TestStream
from apache_beam import window


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_data',
      dest='input_data',
      required=True,
      help='Path to the csv containing the dataset.')
  parser.add_argument(
      '--model_state',
      dest='model_state',
      required=True,
      help='Path to the state of the XGBoost model loaded for Inference.')
  return parser.parse_known_args(argv)


def train_model(
    samples: pandas.DataFrame,
    labels: pandas.DataFrame,
    model_state_output_path: str):
  """Function to train the XGBoost model.
    Args:
      samples: Dataframe contiaing the training data
      labels: Dataframe containing the labels for the training data
      model_state_output_path: Path to store the trained model
  """
  xgb = xgboost.XGBClassifier(max_depth=3)
  xgb.fit(samples, labels)
  xgb.save_model(model_state_output_path)
  return xgb


class MilkQualityAggregation(NamedTuple):
  bad_quality_measurements: int
  medium_quality_measurements: int
  high_quality_measurements: int


class AggregateMilkQualityResults(beam.CombineFn):
  """Simple aggregation to keep track of the number of samples with good, bad and medium quality milk."""
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

  df = pd.read_csv(known_args.input_data)
  df['Grade'].replace(['low', 'medium', 'high'], [0, 1, 2], inplace=True)
  x = df.drop(columns=['Grade'])
  y = df['Grade']
  x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.60, random_state=99)

  start = time.mktime(time.strptime('2023/06/29 10:00:00', '%Y/%m/%d %H:%M:%S'))

  # Create a test stream
  test_stream = TestStream()

  # Watermark is set to 10:00:00
  test_stream.advance_watermark_to(start)

  samples = [x_test.iloc[i:i + 1] for i in range(len(x_test))]

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
  run()
