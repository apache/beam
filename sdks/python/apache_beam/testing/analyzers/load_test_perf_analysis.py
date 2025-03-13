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

from apache_beam.testing.analyzers import constants
from apache_beam.testing.analyzers import perf_analysis
from apache_beam.testing.analyzers import perf_analysis_utils
from apache_beam.testing.analyzers.perf_analysis_utils import MetricContainer
from apache_beam.testing.analyzers.perf_analysis_utils import TestConfigContainer

try:
  from google.cloud import bigquery
except ImportError:
  bigquery = None  # type: ignore


class LoadTestMetricsFetcher(perf_analysis_utils.MetricsFetcher):
  """
    Metrics fetcher used to get metric data from a BigQuery table. The metrics
    are fetched and returned as a dataclass containing lists of timestamps and
    metric_values.
    """
  def fetch_metric_data(
      self, *, test_config: TestConfigContainer) -> MetricContainer:
    if test_config.test_name:
      test_name, pipeline_name = test_config.test_name.split(',')
    else:
      raise Exception("test_name not provided in config.")

    query = f"""
      SELECT timestamp, metric.value
      FROM {test_config.project}.{test_config.metrics_dataset}.{test_config.metrics_table}
      CROSS JOIN UNNEST(metrics) AS metric
      WHERE test_name = "{test_name}" AND pipeline_name = "{pipeline_name}" AND metric.name = "{test_config.metric_name}"
      ORDER BY timestamp DESC
      LIMIT {constants._NUM_DATA_POINTS_TO_RUN_CHANGE_POINT_ANALYSIS}
    """
    logging.debug("Running query: %s" % query)
    if bigquery is None:
      raise ImportError('Bigquery dependencies are not installed.')
    client = bigquery.Client()
    query_job = client.query(query=query)
    metric_data = query_job.result().to_dataframe()
    if metric_data.empty:
      logging.error(
          "No results returned from BigQuery. Please check the query.")
    return MetricContainer(
        values=metric_data['value'].tolist(),
        timestamps=metric_data['timestamp'].tolist(),
    )


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  load_test_metrics_fetcher = LoadTestMetricsFetcher()

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--config_file_path',
      required=True,
      type=str,
      help='Path to the config file that contains data to run the Change Point '
      'Analysis.The default file will used will be '
      'apache_beam/testing/analyzers/tests.config.yml. '
      'If you would like to use the Change Point Analysis for finding '
      'performance regression in the tests, '
      'please provide an .yml file in the same structure as the above '
      'mentioned file. ')
  parser.add_argument(
      '--save_alert_metadata',
      action='store_true',
      default=False,
      help='Save perf alert/ GH Issue metadata to BigQuery table.')
  known_args, unknown_args = parser.parse_known_args()

  if unknown_args:
    logging.warning('Discarding unknown arguments : %s ' % unknown_args)

  perf_analysis.run(
      big_query_metrics_fetcher=load_test_metrics_fetcher,
      config_file_path=known_args.config_file_path,
      # Set this to true while running in production.
      save_alert_metadata=known_args.save_alert_metadata)
