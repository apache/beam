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

# This script is used to run Change Point Analysis using a config file.
# config file holds the parameters required to fetch data, and to run the
# change point analysis. Change Point Analysis is used to find Performance
# regressions for benchmark/load/performance test.

import argparse
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Dict

import pandas as pd

from apache_beam.testing.analyzers import constants
from apache_beam.testing.analyzers.perf_analysis_utils import BigQueryMetricsFetcher
from apache_beam.testing.analyzers.perf_analysis_utils import ChangePointConfig
from apache_beam.testing.analyzers.perf_analysis_utils import GitHubIssueMetaData
from apache_beam.testing.analyzers.perf_analysis_utils import MetricsFetcher
from apache_beam.testing.analyzers.perf_analysis_utils import TestConfigContainer
from apache_beam.testing.analyzers.perf_analysis_utils import create_performance_alert
from apache_beam.testing.analyzers.perf_analysis_utils import find_latest_change_point_index
from apache_beam.testing.analyzers.perf_analysis_utils import get_existing_issues_data
from apache_beam.testing.analyzers.perf_analysis_utils import is_change_point_in_valid_window
from apache_beam.testing.analyzers.perf_analysis_utils import is_sibling_change_point
from apache_beam.testing.analyzers.perf_analysis_utils import publish_issue_metadata_to_big_query
from apache_beam.testing.analyzers.perf_analysis_utils import read_test_config


def get_test_config_container(
    params: Dict[str, Any],
    test_id: str,
    metric_name: str,
) -> TestConfigContainer:
  """
  Args:
    params: Dict containing parameters to run change point analysis.
  Returns:
    TestConfigContainer object containing test config parameters.
  """
  return TestConfigContainer(
      project=params['project'],
      metrics_dataset=params['metrics_dataset'],
      metrics_table=params['metrics_table'],
      metric_name=metric_name,
      test_id=test_id,
      test_description=params['test_description'],
      test_name=params.get('test_name', None),
      labels=params.get('labels', None),
  )


def get_change_point_config(params: Dict[str, Any], ) -> ChangePointConfig:
  """
  Args:
    params: Dict containing parameters to run change point analysis.
  Returns:
    ChangePointConfig object containing change point analysis parameters.
  """
  return ChangePointConfig(
      min_runs_between_change_points=params.get(
          'min_runs_between_change_points',
          constants._DEFAULT_MIN_RUNS_BETWEEN_CHANGE_POINTS),
      num_runs_in_change_point_window=params.get(
          'num_runs_in_change_point_window',
          constants._DEFAULT_NUM_RUMS_IN_CHANGE_POINT_WINDOW))


def run_change_point_analysis(
    test_config_container: TestConfigContainer,
    big_query_metrics_fetcher: MetricsFetcher,
    change_point_config: ChangePointConfig = ChangePointConfig(),
    save_alert_metadata: bool = False,
):
  """
  Args:
   test_config_container: TestConfigContainer containing test metadata for
    fetching data and running change point analysis.
   big_query_metrics_fetcher: BigQuery metrics fetcher used to fetch data for
    change point analysis.
    change_point_config: ChangePointConfig containing parameters to run
      change point analysis.
    save_alert_metadata: bool indicating if issue metadata
      should be published to BigQuery table.
  Returns:
     bool indicating if a change point is observed and alerted on GitHub.
  """
  logging.info(
      "Running change point analysis for test ID :%s on metric: % s" %
      (test_config_container.test_id, test_config_container.metric_name))

  # test_name will be used to query a single test from
  # multiple tests in a single BQ table. Right now, the default
  # assumption is that all the test have an individual BQ table
  # but this might not be case for other tests(such as IO tests where
  # a single BQ tables stores all the data)
  test_name = test_config_container.test_name

  min_runs_between_change_points = (
      change_point_config.min_runs_between_change_points)

  num_runs_in_change_point_window = (
      change_point_config.num_runs_in_change_point_window)

  metric_container = big_query_metrics_fetcher.fetch_metric_data(
      test_config=test_config_container)
  metric_container.sort_by_timestamp()

  metric_values = metric_container.values
  timestamps = metric_container.timestamps

  change_point_index = find_latest_change_point_index(
      metric_values=metric_values)
  if not change_point_index:
    logging.info(
        "Change point is not detected for the test ID %s" %
        test_config_container.test_id)
    return False
  # since timestamps are ordered in ascending order and
  # num_runs_in_change_point_window refers to the latest runs,
  # latest_change_point_run can help determine if the change point
  # index is recent wrt num_runs_in_change_point_window
  latest_change_point_run = len(timestamps) - 1 - change_point_index
  if not is_change_point_in_valid_window(num_runs_in_change_point_window,
                                         latest_change_point_run):
    logging.info(
        'Performance regression/improvement found for the test ID: %s. '
        'on metric %s. Since the change point run %s '
        'lies outside the num_runs_in_change_point_window distance: %s, '
        'alert is not raised.' % (
            test_config_container.test_id,
            test_config_container.metric_name,
            latest_change_point_run + 1,
            num_runs_in_change_point_window))
    return False

  is_valid_change_point = True
  last_reported_issue_number = None

  # create a unique table name for each test and metric combination.
  # for beam load tests, metric_name and metric table are enough to
  # create a unique table name. For templates/IO tests, add `test_name`.
  issue_metadata_table_name = (
      f'{test_config_container.metrics_table}_{test_config_container.metric_name}'  # pylint: disable=line-too-long
  )
  if test_config_container.test_name:
    issue_metadata_table_name = (
        f'{issue_metadata_table_name}_{test_config_container.test_name}')

  existing_issue_data = get_existing_issues_data(
      table_name=issue_metadata_table_name)

  if existing_issue_data is not None:
    existing_issue_timestamps = existing_issue_data[
        constants._CHANGE_POINT_TIMESTAMP_LABEL].tolist()
    last_reported_issue_number = existing_issue_data[
        constants._ISSUE_NUMBER].tolist()[0]

    if not isinstance(last_reported_issue_number, int):
      # convert numpy.int64 to int
      last_reported_issue_number = last_reported_issue_number.item()

    is_valid_change_point = is_sibling_change_point(
        previous_change_point_timestamps=existing_issue_timestamps,
        change_point_index=change_point_index,
        timestamps=timestamps,
        min_runs_between_change_points=min_runs_between_change_points,
        test_id=test_config_container.test_id)

  # for testing purposes, we don't want to create an issue even if there is
  # a valid change point. This is useful when we want to test the change point
  # analysis logic without creating an issue.
  if is_valid_change_point and save_alert_metadata:
    issue_number, issue_url = create_performance_alert(
    test_config_container=test_config_container,
    metric_container=metric_container,
    change_point_index=change_point_index,
    existing_issue_number=last_reported_issue_number,
    )

    issue_metadata = GitHubIssueMetaData(
        issue_timestamp=pd.Timestamp(
            datetime.now().replace(tzinfo=timezone.utc)),
        # BQ doesn't allow '.' in table name
        test_id=test_config_container.test_id.replace('.', '_'),
        test_name=test_name or uuid.uuid4().hex,
        metric_name=test_config_container.metric_name,
        change_point=metric_values[change_point_index],
        issue_number=issue_number,
        issue_url=issue_url,
        change_point_timestamp=timestamps[change_point_index],
    )
    publish_issue_metadata_to_big_query(
        issue_metadata=issue_metadata,
        table_name=issue_metadata_table_name,
        project=test_config_container.project,
    )
  return is_valid_change_point


def run(
    *,
    config_file_path: str,
    big_query_metrics_fetcher: MetricsFetcher = BigQueryMetricsFetcher(),
    save_alert_metadata: bool = False,
) -> None:
  """
  run is the entry point to run change point analysis on test metric
  data, which is read from config file, and if there is a performance
  regression/improvement observed for a test, an alert
  will filed with GitHub Issues.

  If config_file_path is None, then the run method will use default
  config file to read the required perf test parameters.

  Please take a look at the README for more information on the parameters
  defined in the config file.

  """
  tests_config: Dict[str, Dict[str, Any]] = read_test_config(config_file_path)

  for test_id, params in tests_config.items():
    # single test config can have multiple metrics so we need to
    # iterate over all the metrics and run change point analysis
    # for each metric.
    metric_names = params['metric_name']
    if isinstance(metric_names, str):
      metric_names = [metric_names]

    for metric_name in metric_names:
      test_config_container = get_test_config_container(
          params=params, test_id=test_id, metric_name=metric_name)
      change_point_config = get_change_point_config(params)
      run_change_point_analysis(
          test_config_container=test_config_container,
          big_query_metrics_fetcher=big_query_metrics_fetcher,
          change_point_config=change_point_config,
          save_alert_metadata=save_alert_metadata)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

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
      help='Save perf alert/ GH Issue metadata to BigQuery table.')
  known_args, unknown_args = parser.parse_known_args()

  if unknown_args:
    logging.warning('Discarding unknown arguments : %s ' % unknown_args)

  run(
      config_file_path=known_args.config_file_path,
      # Set this to true while running in production.
      save_alert_metadata=known_args.save_alert_metadata  # pylint: disable=line-too-long
  )
