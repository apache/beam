# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compute stats, infer schema, and validate stats for chicago taxi example."""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import numpy as np
import tensorflow as tf
from tensorflow.python.lib.io import file_io
import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import statistics_pb2

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader

from google.protobuf import text_format  # type: ignore  # typeshed out of date
from trainer import taxi


def infer_schema(stats_path, schema_path):
  """Infers a schema from stats in stats_path.

  Args:
    stats_path: Location of the stats used to infer the schema.
    schema_path: Location where the inferred schema is materialized.
  """
  print('Infering schema from statistics.')
  schema = tfdv.infer_schema(
      tfdv.load_statistics(stats_path), infer_feature_shape=False)
  print(text_format.MessageToString(schema))

  print('Writing schema to output path.')
  file_io.write_string_to_file(schema_path, text_format.MessageToString(schema))


def validate_stats(stats_path, schema_path, anomalies_path):
  """Validates the statistics against the schema and materializes anomalies.

  Args:
    stats_path: Location of the stats used to infer the schema.
    schema_path: Location of the schema to be used for validation.
    anomalies_path: Location where the detected anomalies are materialized.
  """
  print('Validating schema against the computed statistics.')
  schema = taxi.read_schema(schema_path)

  stats = tfdv.load_statistics(stats_path)
  anomalies = tfdv.validate_statistics(stats, schema)
  print('Detected following anomalies:')
  print(text_format.MessageToString(anomalies))

  print('Writing anomalies to anomalies path.')
  file_io.write_string_to_file(
      anomalies_path, text_format.MessageToString(anomalies))


def compute_stats(
    input_handle,
    stats_path,
    max_rows=None,
    for_eval=False,
    pipeline_args=None,
    publish_to_bq=None,
    metrics_dataset=None,
    metrics_table=None,
    project=None):
  """Computes statistics on the input data.

  Args:
    input_handle: BigQuery table name to process specified as DATASET.TABLE or
      path to csv file with input data.
    stats_path: Directory in which stats are materialized.
    max_rows: Number of rows to query from BigQuery
    for_eval: Query for eval set rows from BigQuery
    pipeline_args: additional DataflowRunner or DirectRunner args passed to the
      beam pipeline.
  """
  namespace = metrics_table
  pipeline = beam.Pipeline(argv=pipeline_args)
  metrics_monitor = None
  if publish_to_bq:
    metrics_monitor = MetricsReader(
        publish_to_bq=publish_to_bq,
        project_name=project,
        bq_table=metrics_table,
        bq_dataset=metrics_dataset,
        namespace=namespace,
        filters=MetricsFilter().with_namespace(namespace),
    )

  query = taxi.make_sql(
      table_name=input_handle, max_rows=max_rows, for_eval=for_eval)
  raw_data = (
      pipeline
      | 'ReadBigQuery' >> ReadFromBigQuery(
          query=query, project=project, use_standard_sql=True)
      | 'Measure time: Start' >> beam.ParDo(MeasureTime(namespace))
      | 'ConvertToTFDVInput' >> beam.Map(
          lambda x:
          {key: np.asarray([x[key]])
           for key in x if x[key] is not None}))

  _ = (
      raw_data
      | 'GenerateStatistics' >> tfdv.GenerateStatistics()
      | 'Measure time: End' >> beam.ParDo(MeasureTime(namespace))
      | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
          stats_path,
          shard_name_template='',
          coder=beam.coders.ProtoCoder(
              statistics_pb2.DatasetFeatureStatisticsList)))
  result = pipeline.run()
  result.wait_until_finish()
  if metrics_monitor:
    metrics_monitor.publish_metrics(result)


def main():
  tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      help=(
          'Input BigQuery table to process specified as: '
          'DATASET.TABLE or path to csv file with input data.'))

  parser.add_argument(
      '--stats_path',
      help='Location for the computed stats to be materialized.')

  parser.add_argument(
      '--for_eval',
      help='Query for eval set rows from BigQuery',
      action='store_true')

  parser.add_argument(
      '--max_rows',
      help='Number of rows to query from BigQuery',
      default=None,
      type=int)

  parser.add_argument(
      '--schema_path',
      help='Location for the computed schema is located.',
      default=None,
      type=str)

  parser.add_argument(
      '--infer_schema',
      help='If specified, also infers a schema based on the computed stats.',
      action='store_true')

  parser.add_argument(
      '--validate_stats',
      help='If specified, also validates the stats against the schema.',
      action='store_true')

  parser.add_argument(
      '--anomalies_path',
      help='Location for detected anomalies are materialized.',
      default=None,
      type=str)

  parser.add_argument(
      '--publish_to_big_query',
      help='Whether to publish to BQ',
      default=None,
      type=bool)

  parser.add_argument(
      '--metrics_dataset', help='BQ dataset', default=None, type=str)

  parser.add_argument(
      '--metrics_table', help='BQ table', default=None, type=str)

  parser.add_argument(
      '--metric_reporting_project',
      help='BQ table project',
      default=None,
      type=str)

  known_args, pipeline_args = parser.parse_known_args()
  compute_stats(
      input_handle=known_args.input,
      stats_path=known_args.stats_path,
      max_rows=known_args.max_rows,
      for_eval=known_args.for_eval,
      pipeline_args=pipeline_args,
      publish_to_bq=known_args.publish_to_big_query,
      metrics_dataset=known_args.metrics_dataset,
      metrics_table=known_args.metrics_table,
      project=known_args.metric_reporting_project)
  print('Stats computation done.')

  if known_args.infer_schema:
    infer_schema(
        stats_path=known_args.stats_path, schema_path=known_args.schema_path)

  if known_args.validate_stats:
    validate_stats(
        stats_path=known_args.stats_path,
        schema_path=known_args.schema_path,
        anomalies_path=known_args.anomalies_path)


if __name__ == '__main__':
  main()
