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
import os
import time
import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import google.api_core.exceptions
import numpy as np
import pandas as pd
import yaml

from apache_beam.testing.analyzers.github_issues_utils import create_or_comment_issue
from apache_beam.testing.analyzers.github_issues_utils import get_issue_description
from apache_beam.testing.load_tests import load_test_metrics_utils
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsPublisher
from apache_beam.testing.load_tests.load_test_metrics_utils import FetchMetrics
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive

_BQ_PROJECT_NAME = 'apache-beam-testing'
_BQ_DATASET = 'beam_perf_storage'

UNIQUE_ID = 'test_id'
ISSUE_CREATION_TIMESTAMP_LABEL = 'issue_timestamp'
CHANGEPOINT_TIMESTAMP_LABEL = 'change_point_timestamp'
CHANGE_POINT_LABEL = 'change_point'
TEST_NAME = 'test_name'
METRIC_NAME = 'metric_name'
ISSUE_NUMBER = 'issue_number'
ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions.
NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10

SCHEMA = [{
    'name': UNIQUE_ID, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': ISSUE_CREATION_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGEPOINT_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGE_POINT_LABEL,
              'field_type': 'FLOAT64',
              'mode': 'REQUIRED'
          }, {
              'name': METRIC_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': TEST_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': ISSUE_NUMBER, 'field_type': 'INT64', 'mode': 'REQUIRED'
          }, {
              'name': ISSUE_URL, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }]

TITLE_TEMPLATE = """
  Performance Regression: {}:{}
"""
# TODO: Add mean value before and mean value after.
_METRIC_DESCRIPTION = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
ISSUE_LABELS = ['perf-alerts']


class GitHubIssueMetaData:
  """
  This class holds metadata that needs to be published to the
  BigQuery when a GitHub issue is created on a performance
  alert.
  """
  def __init__(
      self,
      issue_creation_timestamp,
      change_point_timestamp,
      test_name,
      metric_name,
      issue_number,
      issue_url,
      test_id,
      change_point):
    self.issue_creation_timestamp = issue_creation_timestamp
    self.change_point_timestamp = change_point_timestamp
    self.test_name = test_name
    self.metric_name = metric_name
    self.issue_number = issue_number
    self.issue_url = issue_url
    self.test_id = test_id
    self.change_point = change_point

  def as_dict(self) -> Dict:
    return {
        ISSUE_CREATION_TIMESTAMP_LABEL: self.issue_creation_timestamp,
        CHANGEPOINT_TIMESTAMP_LABEL: self.change_point_timestamp,
        TEST_NAME: self.test_name,
        METRIC_NAME: self.metric_name,
        ISSUE_NUMBER: self.issue_number,
        UNIQUE_ID: self.test_id,
        CHANGE_POINT_LABEL: self.change_point,
        ISSUE_URL: self.issue_url
    }


class ChangePointAnalysis:
  def __init__(
      self,
      data: Union[List[float], List[List[float]], np.ndarray],
      metric_name: str,
  ):
    self.data = data
    self.metric_name = metric_name

  def edivisive_means(self,
                      pvalue: float = 0.05,
                      permutations: int = 100) -> List[int]:
    """
    Args:
     pvalue: p value for the permutation test.
     permutations: Number of permutations for the permutation test.

    Performs edivisive means on the data and returns the indices of the
    Change points.

    Returns:
     The indices of change points.
    """
    return e_divisive(self.data, pvalue, permutations)


def is_change_point_in_valid_window(
    change_point_to_recent_run_window: int, change_point_index: int) -> bool:
  # If the change point is more than N runs behind the most recent run,
  # Ignore the change point and don't raise an alert for it.
  if change_point_to_recent_run_window >= change_point_index:
    return True
  return False


def has_sibling_change_point(
    change_point_index: int,
    change_point_sibling_distance: int,
    metric_values: List,
    metric_name: str,
    test_name: str,
    change_point_timestamp: float,
) -> Optional[Tuple[bool, Optional[int]]]:
  """
  Finds the sibling change point index. If not,
  returns the original change point index.

  Sibling change point is a neighbor of latest
  change point, within the distance of change_point_sibling_distance.
  For sibling change point, a GitHub issue is already created.
  """

  # Search backward from the current change point
  sibling_indexes_to_search = []
  for i in range(change_point_index - 1, -1, -1):
    if change_point_index - i <= change_point_sibling_distance:
      sibling_indexes_to_search.append(i)
  # Search forward from the current change point
  for i in range(change_point_index + 1, len(metric_values)):
    if i - change_point_index <= change_point_sibling_distance:
      sibling_indexes_to_search.append(i)
  # Look for change points within change_point_sibling_distance.
  # Return the first change point found.
  query_template = """
  SELECT * FROM {project}.{dataset}.{table}
  WHERE {metric_name_id} = '{metric_name}'
  ORDER BY {timestamp} DESC
  LIMIT 10
  """.format(
      project=_BQ_PROJECT_NAME,
      dataset=_BQ_DATASET,
      metric_name_id=METRIC_NAME,
      metric_name=metric_name,
      timestamp=ISSUE_CREATION_TIMESTAMP_LABEL,
      table=test_name)
  try:
    df = FetchMetrics.fetch_from_bq(query_template=query_template)
  except google.api_core.exceptions.NotFound:
    # If no table found, that means this is first performance regression
    # on the current test:metric.
    return True, None
  previous_change_point = df[CHANGE_POINT_LABEL].tolist()[0]
  previous_change_point_timestamp = df[CHANGEPOINT_TIMESTAMP_LABEL].tolist()[0]
  issue_number = df[ISSUE_NUMBER].tolist()[0]

  # Check if the current change point is equal to the
  # latest reported change point on GitHub Issues using the
  # value and the timestamp. If it's the same,
  # don't create or comment on issues.
  if ((previous_change_point == metric_values[change_point_index]) and
      (previous_change_point_timestamp == change_point_timestamp)):
    return False, None

  alert = True
  for sibling_index in sibling_indexes_to_search:
    if metric_values[sibling_index] == previous_change_point:
      alert = False
      return alert, None
  return alert, issue_number


def read_test_config(config_file_path: str) -> Dict:
  """
  Reads the config file in which the data required to
  run the change point analysis is specified.
  """
  with open(config_file_path, 'r') as stream:
    config = yaml.safe_load(stream)
  return config


def run(args) -> None:
  """
  run is the entry point to run change point analysis on test metric
  data, which is read from config file, and if there is a performance
  regression observed for a test, an alert will filed with GitHub Issues.

  The config file is provide as command line argument. If no config file was
  provided on cmd line, the default config file will be used.

  For each test is config yaml file, if the source is the big_query,
  the expected data required to run the change point analysis are
  test_name, metrics_dataset, metrics_table, project, metric_name.

  """
  config_file_path = args.config_file_path
  if config_file_path is None:
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')

  tests_config: Dict[Dict[str, Any]] = read_test_config(config_file_path)

  # change_point_sibling_distance, change_point_to_recent_run_window can be
  # defined in the config file for each test whihc are used
  # to avoid filing GitHub issues for duplicate change points. Please take
  # a look at the README for more information on the parameters defined in the
  # config file.
  for _, params in tests_config.items():
    metric_name = params['metric_name']
    # replace . with _ in test_name. This test name would be used later
    # as a BQ table name and the BQ table doesn't accept . in the name.
    test_name = params['test_name'].replace('.', '_')
    if params['source'] == 'big_query':
      metric_data: pd.DataFrame = FetchMetrics.fetch_from_bq(
          project_name=params['project'],
          dataset=params['metrics_dataset'],
          table=params['metrics_table'],
          metric_name=metric_name)
    else:
      # (TODO): Implement fetching metric_data from InfluxDB.
      params = None
    assert params is not None

    labels = params['labels']
    change_point_sibling_distance = params['change_point_sibling_distance']
    change_point_to_recent_run_window = params[
        'change_point_to_recent_run_window']

    metric_values = metric_data[load_test_metrics_utils.VALUE_LABEL]
    timestamps = metric_data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL]

    # run change point analysis on the metric_values using edivisive means
    cp_analyzer = ChangePointAnalysis(
        metric_name=metric_name, data=metric_values)

    change_points_idx = cp_analyzer.edivisive_means()
    # No change point found. Continue on to the next test.
    if not change_points_idx:
      continue

    # always consider the latest change points
    change_points_idx.sort(reverse=True)
    change_point_index = change_points_idx[0]
    change_point_timestamp = timestamps[change_point_index]

    # check if the change point lies in the valid window.
    # window - Number of runs between the
    # change_point_to_recent_run_window run and the most recent run.
    if not is_change_point_in_valid_window(change_point_to_recent_run_window,
                                           change_point_index):
      # change point lies outside the window from the recent run.
      # Ignore this change point.
      logging.info(
          'Performance regression found for the test: %s. '
          'but not creating an alert since the Change Point '
          'lies outside the '
          'change_point_to_recent_run_window distance' % test_name)
      continue

    # check for sibling change point. Sibling change point is a change
    # point that lies in the distance of change_point_sibling_distance
    # in both directions from the current change point index.
    # Here, distance can be interpreted as number of runs between two change
    # points. The idea here is that sibling change point will also point to
    # the same performance regression.

    create_alert, last_created_issue_number = (
      has_sibling_change_point(
        change_point_index=change_point_index,
        change_point_sibling_distance=change_point_sibling_distance,
        metric_values=metric_values,
        metric_name=metric_name,
        test_name=test_name,
        change_point_timestamp=change_point_timestamp
      )
    )

    logging.info(
        "Create performance alert for the "
        "test %s: %s" % (test_name, create_alert))

    if create_alert:
      # get the issue description for the creating GH issue or
      # to comment on open GH issue.
      issue_description = get_issue_description(
          metric_name=metric_name,
          timestamps=timestamps,
          metric_values=metric_values,
          change_point_index=change_point_index,
          max_results_to_display=NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION)

      issue_number, issue_url = create_or_comment_issue(
        title=TITLE_TEMPLATE.format(test_name, metric_name),
        description=issue_description,
        labels=labels,
        issue_number=last_created_issue_number
      )

      logging.info(
          'Performance regression is alerted on issue #%s. Link to '
          'the issue: %s' % (issue_number, issue_url))

      issue_meta_data_dict = GitHubIssueMetaData(
          issue_creation_timestamp=time.time(),
          test_name=test_name,
          metric_name=metric_name,
          test_id=uuid.uuid4().hex,
          change_point=metric_values[change_point_index],
          issue_number=issue_number,
          issue_url=issue_url,
          change_point_timestamp=timestamps[change_point_index]).as_dict()

      # publish GH issue metadata to GH issue. This metadata would be
      # used for finding the sibling change point.
      bq_metrics_publisher = BigQueryMetricsPublisher(
          project_name=_BQ_PROJECT_NAME,
          dataset=_BQ_DATASET,
          table=test_name,
          bq_schema=SCHEMA)

      bq_metrics_publisher.publish([issue_meta_data_dict])
      logging.info(
          'GitHub metadata is published to Big Query Dataset %s'
          ', table %s' % (_BQ_DATASET, test_name))


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

  run(known_args)
