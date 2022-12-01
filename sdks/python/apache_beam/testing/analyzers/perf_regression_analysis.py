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
from dataclasses import asdict
from dataclasses import dataclass
import logging
import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd
import yaml
from google.api_core import exceptions

from apache_beam.testing.analyzers.github_issues_utils import get_issue_description
from apache_beam.testing.analyzers.github_issues_utils import report_change_point_on_issues
from apache_beam.testing.load_tests import load_test_metrics_utils
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsPublisher
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsFetcher
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive

_BQ_PROJECT_NAME = 'apache-beam-testing'
_BQ_DATASET = 'beam_perf_storage'

_UNIQUE_ID = 'test_id'
_ISSUE_CREATION_TIMESTAMP_LABEL = 'issue_timestamp'
_CHANGE_POINT_TIMESTAMP_LABEL = 'change_point_timestamp'
_CHANGE_POINT_LABEL = 'change_point'
_TEST_NAME = 'test_name'
_METRIC_NAME = 'metric_name'
_ISSUE_NUMBER = 'issue_number'
_ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions.
_NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10
_NUM_DATA_POINTS_TO_RUN_CHANGE_POINT_ANALYSIS = 100
# Variables used for finding duplicate change points.
_DEFAULT_MIN_RUNS_BETWEEN_CHANGE_POINTS = 5
_DEFAULT_NUM_RUMS_IN_CHANGE_POINT_WINDOW = 20

_PERF_ALERT_LABEL = 'perf-alert'

_PERF_TEST_KEYS = {
    'test_name', 'metrics_dataset', 'metrics_table', 'project', 'metric_name'
}

SCHEMA = [{
    'name': _UNIQUE_ID, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': _ISSUE_CREATION_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': _CHANGE_POINT_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': _CHANGE_POINT_LABEL,
              'field_type': 'FLOAT64',
              'mode': 'REQUIRED'
          }, {
              'name': _METRIC_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': _TEST_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': _ISSUE_NUMBER, 'field_type': 'INT64', 'mode': 'REQUIRED'
          }, {
              'name': _ISSUE_URL, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }]

TITLE_TEMPLATE = """
  Performance Regression or Improvement: {}:{}
"""
# TODO: Add Median value before and Median value after.
_METRIC_DESCRIPTION = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
ISSUE_LABELS = ['perf-alert']


@dataclass
class GitHubIssueMetaData:
  """
  This class holds metadata that needs to be published to the
  BigQuery when a GitHub issue is created on a performance
  alert.
  """
  issue_timestamp: pd.Timestamp
  change_point_timestamp: pd.Timestamp
  test_name: str
  metric_name: str
  issue_number: int
  issue_url: str
  test_id: str
  change_point: float


def is_change_point_in_valid_window(
    num_runs_in_change_point_window: int, change_point_index: int) -> bool:
  # If the change point is more than N runs behind the most recent run,
  # Ignore the change point and don't raise an alert for it.
  return num_runs_in_change_point_window >= change_point_index


def find_existing_issue(
    test_name: str,
) -> Tuple[Optional[int], Optional[pd.Timestamp]]:
  """
  Finds the most recent GitHub issue created for the test_name.
  If no table found with name=test_name, return (None, None)
  else return issue_number and the timestamp of change point
  reported in the existing issue.
  """
  query_template = f"""
  SELECT * FROM {_BQ_PROJECT_NAME}.{_BQ_DATASET}.{test_name}
  ORDER BY {_ISSUE_CREATION_TIMESTAMP_LABEL} DESC
  LIMIT 1
  """
  try:
    df = BigQueryMetricsFetcher().get_metrics(query_template=query_template)
  except exceptions.NotFound:
    # If no table found, that means this is first performance regression
    # on the current test+metric.
    return None, None
  issue_number = df[_ISSUE_NUMBER].tolist()[0]
  existing_change_point_timestamp = df[_CHANGE_POINT_TIMESTAMP_LABEL].tolist(
  )[0]
  return issue_number, existing_change_point_timestamp


def is_perf_alert(
    previous_change_point_timestamp: pd.Timestamp,
    change_point_index: int,
    timestamps: List[pd.Timestamp],
    min_runs_between_change_points: int) -> bool:

  change_point_timestamp = timestamps[change_point_index]
  sibling_change_point_min_timestamp = timestamps[min(
      change_point_index + min_runs_between_change_points, len(timestamps) - 1)]
  sibling_change_point_max_timestamp = timestamps[max(
      0, change_point_index - min_runs_between_change_points)]

  if (previous_change_point_timestamp == change_point_timestamp) or (
      sibling_change_point_min_timestamp <= previous_change_point_timestamp <=
      sibling_change_point_max_timestamp):
    return False
  return True


def read_test_config(config_file_path: str) -> Dict:
  """
  Reads the config file in which the data required to
  run the change point analysis is specified.
  """
  with open(config_file_path, 'r') as stream:
    config = yaml.safe_load(stream)
  return config


def validate_config(keys):
  return _PERF_TEST_KEYS.issubset(keys)


def fetch_metric_data(
    params: Dict[str,
                 Any]) -> Tuple[List[Union[int, float]], List[pd.Timestamp]]:
  # replace . with _ in test_name. This test name would be used later
  # as a BQ table name and the BQ table doesn't accept . in the name.
  try:
    metric_data: pd.DataFrame = BigQueryMetricsFetcher().get_metrics(
        project_name=params['project'],
        dataset=params['metrics_dataset'],
        table=params['metrics_table'],
        metric_name=params['metric_name'],
        limit=_NUM_DATA_POINTS_TO_RUN_CHANGE_POINT_ANALYSIS)
  except BaseException as e:
    raise e
  return (
      metric_data[load_test_metrics_utils.VALUE_LABEL],
      metric_data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL])


def find_latest_change_point_index(metric_values: List[Union[float, int]]):
  change_points_idx = e_divisive(metric_values)
  if not change_points_idx:
    return None
  # Consider the latest change point.
  change_points_idx.sort(reverse=True)
  change_point_index = change_points_idx[0]
  return change_point_index


def publish_issue_metadata_to_big_query(issue_metadata, test_name):
  bq_metrics_publisher = BigQueryMetricsPublisher(
      project_name=_BQ_PROJECT_NAME,
      dataset=_BQ_DATASET,
      table=test_name,
      bq_schema=SCHEMA)
  bq_metrics_publisher.publish([asdict(issue_metadata)])
  logging.info(
      'GitHub metadata is published to Big Query Dataset %s'
      ', table %s' % (_BQ_DATASET, test_name))


def run(config_file_path: str = None) -> None:
  """
  run is the entry point to run change point analysis on test metric
  data, which is read from config file, and if there is a performance
  regression observed for a test, an alert will filed with GitHub Issues.

  If config_file_path is None, then the run method will use default
  config file to read the required perf test parameters.

  Please take a look at the README for more information on the parameters
  defined in the config file.

  """
  if config_file_path is None:
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')

  tests_config: Dict[str, Dict[str, Any]] = read_test_config(config_file_path)

  for test_id, params in tests_config.items():

    if not validate_config(params.keys()):
      raise Exception(
          f"Please make sure all these keys {_PERF_TEST_KEYS} "
          f"are specified for the {test_id}")

    metric_name = params['metric_name']
    test_name = params['test_name'].replace('.', '_') + f'_{metric_name}'

    labels = [_PERF_ALERT_LABEL]
    if 'labels' in params:
      labels += params['labels']

    min_runs_between_change_points = _DEFAULT_MIN_RUNS_BETWEEN_CHANGE_POINTS
    if 'min_runs_between_change_points' in params:
      min_runs_between_change_points = params['min_runs_between_change_points']

    num_runs_in_change_point_window = _DEFAULT_NUM_RUMS_IN_CHANGE_POINT_WINDOW
    if 'num_runs_in_change_point_window' in params:
      num_runs_in_change_point_window = params[
          'num_runs_in_change_point_window']

    metric_values, timestamps = fetch_metric_data(params)

    change_point_index = find_latest_change_point_index(
        metric_values=metric_values)
    if not change_point_index:
      continue

    if not is_change_point_in_valid_window(num_runs_in_change_point_window,
                                           change_point_index):
      logging.info(
          'Performance regression/improvement found for the test: %s. '
          'Since the change point index %s '
          'lies outside the num_runs_in_change_point_window distance: %s, '
          'alert is not raised.' %
          (test_name, change_point_index, num_runs_in_change_point_window))
      continue

    existing_issue_number, existing_issue_timestamp = find_existing_issue(
      test_name=test_name
    )
    # since there was no issue found for the current change point,
    # considering it as new change point and raising an alert.
    if not existing_issue_timestamp:
      create_alert = True
    else:
      create_alert = is_perf_alert(
          previous_change_point_timestamp=existing_issue_timestamp,
          change_point_index=change_point_index,
          timestamps=timestamps,
          min_runs_between_change_points=min_runs_between_change_points)

    # TODO: Remove this before merging.
    logging.info(
        "Create performance alert for the "
        "test %s: %s" % (test_name, create_alert))

    if create_alert:
      description = get_issue_description(
          metric_name=metric_name,
          timestamps=timestamps,
          metric_values=metric_values,
          change_point_index=change_point_index,
          max_results_to_display=_NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION)

      issue_number, issue_url = report_change_point_on_issues(
        title=TITLE_TEMPLATE.format(test_name, metric_name),
        description=description,
        labels=labels,
        issue_number=existing_issue_number
      )

      logging.info(
          'Performance regression is alerted on issue #%s. Link to '
          'the issue: %s' % (issue_number, issue_url))

      issue_metadata = GitHubIssueMetaData(
          issue_timestamp=pd.Timestamp(
              datetime.now().replace(tzinfo=timezone.utc)),
          test_name=test_name,
          metric_name=metric_name,
          test_id=uuid.uuid4().hex,
          change_point=metric_values[change_point_index],
          issue_number=issue_number,
          issue_url=issue_url,
          change_point_timestamp=timestamps[change_point_index])

      publish_issue_metadata_to_big_query(
          issue_metadata=issue_metadata, test_name=test_name)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--config_file_path',
      default=None,
      type=str,
      help='Path to the config file that contains data to run the Change Point '
      'Analysis.The default file will used will be '
      'apache_beam/testing/analyzers/tests.config.yml. '
      'If you would like to use the Change Point Analysis for finding '
      'performance regression in the tests, '
      'please provide an .yml file in the same structure as the above '
      'mentioned file. ')
  known_args, unknown_args = parser.parse_known_args()

  if unknown_args:
    logging.warning('Discarding unknown arguments : %s ' % unknown_args)

  run(known_args.config_file_path)
