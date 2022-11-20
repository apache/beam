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
import logging
import os
import unittest

import mock
import pandas as pd

try:
  import apache_beam.testing.analyzers.perf_regression_analysis as analysis
except ImportError as e:
  analysis = None


class IgnoreChangePointObject:
  min_runs_between_changepoints = 2
  changepoint_to_recent_run_window = 2


def fake_data(query_template):
  metric_name = 'fake_metric_name'
  change_point = 0
  test_name = 'fake_test'
  change_point_timestamp = 'timestamp'
  df = pd.DataFrame([{
      analysis.CHANGE_POINT_LABEL: change_point,
      analysis.TEST_NAME: test_name,
      analysis.METRIC_NAME: metric_name,
      analysis.CHANGE_POINT_TIMESTAMP_LABEL: change_point_timestamp,
      analysis.ISSUE_NUMBER: '1',
      analysis.ISSUE_URL: 'Fake URL'
  }])
  return df


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

  def test_edivisive_means(self):
    cp_analyzer = analysis.ChangePointAnalysis(
        data=self.single_change_point_series, metric_name='fake_name')
    change_point_indexes = cp_analyzer.edivisive_means()
    self.assertEqual(change_point_indexes, [10])

    cp_analyzer = analysis.ChangePointAnalysis(
        data=self.multiple_change_point_series, metric_name='fake_name')
    change_point_indexes = cp_analyzer.edivisive_means()
    self.assertEqual(sorted(change_point_indexes), [10, 20])

  def test_is_changepoint_in_valid_window(self):

    changepoint_to_recent_run_window = 19
    change_point_index = 14

    is_valid = analysis.is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, True)

    changepoint_to_recent_run_window = 13
    is_valid = analysis.is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, False)

    changepoint_to_recent_run_window = 14
    is_valid = analysis.is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, True)

  @mock.patch(
      'apache_beam.testing.load_tests.'
      'load_test_metrics_utils.FetchMetrics.fetch_from_bq',
      fake_data)
  def test_has_sibling_change_point(self):
    metric_name = 'fake_metric_name'
    test_name = 'fake_test_name'
    alert_new_issue = analysis.find_existing_issue(
        metric_name=metric_name,
        test_name=test_name,
        change_point_timestamp=100,
        sibling_change_point_max_timestamp=150,
        sibling_change_point_min_timestamp=80)

    self.assertEqual(alert_new_issue[0], False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  os.environ['GITHUB_TOKEN'] = None
  unittest.main()
