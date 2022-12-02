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
import time
import unittest

try:
  import apache_beam.testing.analyzers.perf_analysis as analysis
  from apache_beam.testing.analyzers import constants
  from apache_beam.testing.analyzers.perf_analysis_utils import is_change_point_in_valid_window
  from apache_beam.testing.analyzers.perf_analysis_utils import is_perf_alert
  from apache_beam.testing.analyzers.perf_analysis_utils import e_divisive
  from apache_beam.testing.analyzers.perf_analysis_utils import validate_config
except ImportError as e:
  analysis = None


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

    changepoint_to_recent_run_window = 13
    is_valid = is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, False)

    changepoint_to_recent_run_window = 14
    is_valid = is_change_point_in_valid_window(
        changepoint_to_recent_run_window, change_point_index)
    self.assertEqual(is_valid, True)

  def test_validate_config(self):
    test_keys = {
        'test_name',
        'metrics_dataset',
        'metrics_table',
        'project',
        'metric_name'
    }
    self.assertEqual(test_keys, constants.PERF_TEST_KEYS)
    self.assertTrue(validate_config(test_keys))

  def test_is_perf_alert(self):
    timestamp_1 = time.time()
    timestamps = [timestamp_1 + i for i in range(4, -1, -1)]

    change_point_index = 2
    min_runs_between_change_points = 1

    is_alert = is_perf_alert(
        previous_change_point_timestamps=[timestamps[3]],
        timestamps=timestamps,
        change_point_index=change_point_index,
        min_runs_between_change_points=min_runs_between_change_points)
    self.assertFalse(is_alert)

    is_alert = is_perf_alert(
        previous_change_point_timestamps=[timestamps[0]],
        timestamps=timestamps,
        change_point_index=change_point_index,
        min_runs_between_change_points=min_runs_between_change_points)
    self.assertTrue(is_alert)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  os.environ['GITHUB_TOKEN'] = 'fake_token'
  unittest.main()
