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
# regressions for Benchmark/load/performance test.

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

import numpy as np
import pandas as pd
import yaml
from google.api_core import exceptions

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
CHANGE_POINT_TIMESTAMP_LABEL = 'change_point_timestamp'
CHANGE_POINT_LABEL = 'change_point'
TEST_NAME = 'test_name'
METRIC_NAME = 'metric_name'
ISSUE_NUMBER = 'issue_number'
ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions.
_NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10

SCHEMA = [{
    'name': UNIQUE_ID, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': ISSUE_CREATION_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGE_POINT_TIMESTAMP_LABEL,
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
  Performance Regression or Improvement: {}:{}
"""
# TODO: Add mean value before and mean value after.
_METRIC_DESCRIPTION = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
ISSUE_LABELS = ['perf-alerts']


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
    num_runs_in_change_point_window: int, change_point_index: int) -> bool:
  # If the change point is more than N runs behind the most recent run,
  # Ignore the change point and don't raise an alert for it.
  return num_runs_in_change_point_window >= change_point_index


def find_existing_issue(
    metric_name: str,
    test_name: str,
    change_point_timestamp: pd.Timestamp,
    sibling_change_point_min_timestamp: pd.Timestamp,
    sibling_change_point_max_timestamp: pd.Timestamp,
) -> Optional[Tuple[bool, Optional[int]]]:
  """
  Finds the most recent GitHub issue created for change points for this
  test+metric in sibling change point min and max timestamps window.
  Returns a boolean and an issue ID whether the issue needs to be updated.
  """
  query_template = f"""
  SELECT * FROM {_BQ_PROJECT_NAME}.{_BQ_DATASET}.{test_name}
  WHERE {METRIC_NAME} = '{metric_name}'
  ORDER BY {ISSUE_CREATION_TIMESTAMP_LABEL} DESC
  LIMIT 1
  """
  try:
    df = FetchMetrics.fetch_from_bq(query_template=query_template)
  except exceptions.NotFound:
    # If no table found, that means this is first performance regression
    # on the current test+metric.
    return True, None
  issue_number = df[ISSUE_NUMBER].tolist()[0]

  if (sibling_change_point_min_timestamp <= change_point_timestamp <=
      sibling_change_point_max_timestamp):
    return False, None
  return True, issue_number


def read_test_config(config_file_path: str) -> Dict:
  """
  Reads the config file in which the data required to
  run the change point analysis is specified.
  """
  with open(config_file_path, 'r') as stream:
    config = yaml.safe_load(stream)
  return config


def run(config_file_path: Optional[Dict] = None) -> None:
  """
  run is the entry point to run change point analysis on test metric
  data, which is read from config file, and if there is a performance
  regression observed for a test, an alert will filed with GitHub Issues.

  If config_file_path is None, then the run method will use default
  config file to read the required perf test parameters.

  """
  if config_file_path is None:
    config_file_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')

  tests_config: Dict[Dict[str, Any]] = read_test_config(config_file_path)

  # min_runs_between_change_points, num_runs_in_change_point_window can be
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
      raise ValueError(
          'For change point analysis, only big_query is'
          'accepted as source.')

    labels = params['labels']
    min_runs_between_change_points = params['min_runs_between_change_points']
    num_runs_in_change_point_window = params['num_runs_in_change_point_window']

    metric_values = metric_data[load_test_metrics_utils.VALUE_LABEL]
    timestamps = metric_data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL]

    cp_analyzer = ChangePointAnalysis(
        metric_name=metric_name, data=metric_values)

    change_points_idx = cp_analyzer.edivisive_means()
    if not change_points_idx:
      continue

    # Consider the latest change points to observe the latest perf alerts.
    change_points_idx.sort(reverse=True)
    change_point_index = change_points_idx[0]
    change_point_timestamp = timestamps[change_point_index]

    # check if the change point lies in the valid window.
    # window - Number of runs between the
    # num_runs_in_change_point_window run and the most recent run.
    if not is_change_point_in_valid_window(num_runs_in_change_point_window,
                                           change_point_index):
      logging.info(
          'Performance regression found for the test: %s. '
          'but not creating an alert since the Change Point '
          'lies outside the '
          'num_runs_in_change_point_window distance.' % test_name)
      continue

    # Look for an existing GitHub issue related to the current change point.
    # It can be interpreted sibling change point.
    # Sibling change point is a change point that lies in the distance of
    # min_runs_between_change_points in both directions from the current change
    # point index.
    # Here, distance can be interpreted as number of runs between two change
    # points. The idea here is that sibling change point will also point to
    # the same performance regression.
    min_timestamp_index = min(
        change_point_index + min_runs_between_change_points,
        len(timestamps) - 1)
    max_timestamp_index = max(
        0, change_point_index - min_runs_between_change_points)
    sibling_change_point_min_timestamp = timestamps[min_timestamp_index]
    sibling_change_point_max_timestamp = timestamps[max_timestamp_index]

    create_alert, last_created_issue_number = (
      find_existing_issue(
        metric_name=metric_name,
        test_name=test_name,
        change_point_timestamp=change_point_timestamp,
        sibling_change_point_max_timestamp=sibling_change_point_max_timestamp,
        sibling_change_point_min_timestamp=sibling_change_point_min_timestamp
      )
    )

    logging.info(
        "Create performance alert for the "
        "test %s: %s" % (test_name, create_alert))
    if create_alert:
      issue_description = get_issue_description(
          metric_name=metric_name,
          timestamps=timestamps,
          metric_values=metric_values,
          change_point_index=change_point_index,
          max_results_to_display=_NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION)
      issue_number, issue_url = create_or_comment_issue(
        title=TITLE_TEMPLATE.format(test_name, metric_name),
        description=issue_description,
        labels=labels,
        issue_number=last_created_issue_number
      )

      logging.info(
          'Performance regression is alerted on issue #%s. Link to '
          'the issue: %s' % (issue_number, issue_url))
      issue_creation_timestamp = pd.Timestamp(
          datetime.now().replace(tzinfo=timezone.utc))
      issue_metadata = GitHubIssueMetaData(
          issue_timestamp=issue_creation_timestamp,
          test_name=test_name,
          metric_name=metric_name,
          test_id=uuid.uuid4().hex,
          change_point=metric_values[change_point_index],
          issue_number=issue_number,
          issue_url=issue_url,
          change_point_timestamp=timestamps[change_point_index])

      # publish GH issue metadata to GH issue. This metadata would be
      # used for finding the sibling change point.
      bq_metrics_publisher = BigQueryMetricsPublisher(
          project_name=_BQ_PROJECT_NAME,
          dataset=_BQ_DATASET,
          table=test_name,
          bq_schema=SCHEMA)

      bq_metrics_publisher.publish([asdict(issue_metadata)])
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

  run(known_args.config_file_path)
