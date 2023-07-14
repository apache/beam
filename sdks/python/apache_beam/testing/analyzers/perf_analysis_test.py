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
# pytype: skip-file

import datetime
import logging
import os
import re
import unittest

import mock
import numpy as np
import pandas as pd

# pylint: disable=ungrouped-imports
try:
  import apache_beam.testing.analyzers.perf_analysis as analysis
  from apache_beam.testing.analyzers import constants
  from apache_beam.testing.analyzers import github_issues_utils
  from apache_beam.testing.analyzers.perf_analysis_utils import is_change_point_in_valid_window
  from apache_beam.testing.analyzers.perf_analysis_utils import is_perf_alert
  from apache_beam.testing.analyzers.perf_analysis_utils import e_divisive
  from apache_beam.testing.analyzers.perf_analysis_utils import find_latest_change_point_index
  from apache_beam.testing.analyzers.perf_analysis_utils import validate_config

except ImportError as e:
  analysis = None  # type: ignore


# mock methods.
def get_fake_data_with_no_change_point(**kwargs):
  num_samples = 20
  metric_values = [1] * num_samples
  timestamps = list(range(num_samples))
  return metric_values, timestamps


def get_fake_data_with_change_point(**kwargs):
  # change point will be at index 13.
  num_samples = 20
  metric_values = [0] * 12 + [3] + [4] * 7
  timestamps = [i for i in range(num_samples)]
  return metric_values, timestamps


def get_existing_issue_data(**kwargs):
  # change point found at index 13. So passing 13 in the
  # existing issue data in mock method.
  return pd.DataFrame([{
      constants._CHANGE_POINT_TIMESTAMP_LABEL: 13,
      constants._ISSUE_NUMBER: np.array([0])
  }])


@unittest.skipIf(
    analysis is None,
    'Missing dependencies. '
    'Test dependencies are missing for the Analyzer.')
class TestChangePointAnalysis(unittest.TestCase):
  def setUp(self) -> None:
    self.single_change_point_series = [0] * 10 + [1] * 10
    self.multiple_change_point_series = self.single_change_point_series + [
        2
    ] * 20
    self.timestamps = list(range(5))
    self.params = {
        'test_description': 'fake_description',
        'metrics_dataset': 'fake_dataset',
        'metrics_table': 'fake_table',
        'project': 'fake_project',
        'metric_name': 'fake_metric_name'
    }
    self.test_id = 'fake_id'

  def test_edivisive_means(self):
    change_point_indexes = e_divisive(self.single_change_point_series)
    self.assertEqual(change_point_indexes, [10])
    change_point_indexes = e_divisive(self.multiple_change_point_series)
    self.assertEqual(sorted(change_point_indexes), [10, 20])

  def test_is_changepoint_in_valid_window(self):
    changepoint_to_recent_run_window = 19
    change_point_index = 14

    is_valid = is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, True)

  def test_change_point_outside_inspection_window_is_not_a_valid_alert(self):
    changepoint_to_recent_run_window = 12
    change_point_index = 14

    is_valid = is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, False)

  def test_validate_config(self):
    test_keys = {
        'test_description',
        'metrics_dataset',
        'metrics_table',
        'project',
        'metric_name'
    }
    self.assertEqual(test_keys, constants._PERF_TEST_KEYS)
    self.assertTrue(validate_config(test_keys))

  def test_duplicate_change_point(self):
    change_point_index = 2
    min_runs_between_change_points = 1
    is_alert = is_perf_alert(
        previous_change_point_timestamps=[self.timestamps[0]],
        timestamps=self.timestamps,
        change_point_index=change_point_index,
        min_runs_between_change_points=min_runs_between_change_points)
    self.assertTrue(is_alert)

  def test_duplicate_change_points_are_not_valid_alerts(self):
    change_point_index = 2
    min_runs_between_change_points = 1
    is_alert = is_perf_alert(
        previous_change_point_timestamps=[self.timestamps[3]],
        timestamps=self.timestamps,
        change_point_index=change_point_index,
        min_runs_between_change_points=min_runs_between_change_points)
    self.assertFalse(is_alert)

    is_alert = is_perf_alert(
        previous_change_point_timestamps=[
            self.timestamps[0], self.timestamps[3]
        ],
        timestamps=self.timestamps,
        change_point_index=change_point_index,
        min_runs_between_change_points=min_runs_between_change_points)
    self.assertFalse(is_alert)

  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.fetch_metric_data',
      get_fake_data_with_no_change_point)
  def test_no_alerts_when_no_change_points(self):
    is_alert = analysis.run_change_point_analysis(
        params=self.params,
        test_name=self.test_id,
        big_query_metrics_fetcher=None)
    self.assertFalse(is_alert)

  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.fetch_metric_data',
      get_fake_data_with_change_point)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.get_existing_issues_data',
      return_value=None)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.'
      'publish_issue_metadata_to_big_query',
      return_value=None)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis'
      '.create_performance_alert',
      return_value=(0, ''))
  def test_alert_on_data_with_change_point(self, *args):
    is_alert = analysis.run_change_point_analysis(
        params=self.params,
        test_name=self.test_id,
        big_query_metrics_fetcher=None)
    self.assertTrue(is_alert)

  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.fetch_metric_data',
      get_fake_data_with_change_point)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.get_existing_issues_data',
      get_existing_issue_data)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.'
      'publish_issue_metadata_to_big_query',
      return_value=None)
  @mock.patch(
      'apache_beam.testing.analyzers.perf_analysis.create_performance_alert',
      return_value=(0, ''))
  def test_alert_on_data_with_reported_change_point(self, *args):
    is_alert = analysis.run_change_point_analysis(
        params=self.params,
        test_name=self.test_id,
        big_query_metrics_fetcher=None)
    self.assertFalse(is_alert)

  def test_change_point_has_anomaly_marker_in_gh_description(self):
    metric_values, timestamps = get_fake_data_with_change_point()
    timestamps = [datetime.datetime.fromtimestamp(ts) for ts in timestamps]
    change_point_index = find_latest_change_point_index(metric_values)

    description = github_issues_utils.get_issue_description(
        test_name=self.test_id,
        test_description=self.params['test_description'],
        metric_name=self.params['metric_name'],
        metric_values=metric_values,
        timestamps=timestamps,
        change_point_index=change_point_index,
        max_results_to_display=(
            constants._NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION))

    runs_info = next((
        line for line in description.split(2 * os.linesep)
        if re.match(r'timestamp: .*, metric_value: .*', line.strip())),
                     '')
    pattern = (r'timestamp: .+ (\d{4}), metric_value: (\d+.\d+) <---- Anomaly')
    match = re.search(pattern, runs_info)
    self.assertTrue(match)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  os.environ['GITHUB_TOKEN'] = 'fake_token'
  unittest.main()
