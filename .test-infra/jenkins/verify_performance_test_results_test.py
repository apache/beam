#!/usr/bin/env python
#
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#   This script performs testing of scenarios from verify_performance_test_results.py
#

import unittest, mock
from mock import patch
from verify_performance_test_results import create_report

class VerifyAnalysisScript(unittest.TestCase):
    """Tests for `verify_performance_test_results.py`."""

    @patch('verify_performance_test_results.count_queries', return_value=0)
    def test_create_daily_report_when_no_data_was_uploaded(self, *args):
        """Testing creating report when no data was uploaded. Expected: Error message"""
        output_message = create_report(["test_bq_table"], "test", False)
        assert "no tests results uploaded in recent 24h." in output_message

    @patch('verify_performance_test_results.count_queries', return_value=1)
    @patch('verify_performance_test_results.get_average_from', return_value=10)
    @patch('verify_performance_test_results.get_stddev_from', return_value=10)
    def test_create_daily_report_when_single_entry_was_uploaded(self, *args):
        """Testing stddev value when single data entry was uploaded. Expected: 0"""
        output_message = create_report(["test_bq_table"], "test", False)
        assert ", stddev 0.00" in output_message

    @patch('verify_performance_test_results.count_queries', side_effect=[1, 0])
    @patch('verify_performance_test_results.get_average_from', return_value=10)
    @patch('verify_performance_test_results.get_stddev_from', return_value=10)
    def test_create_daily_report_when_no_historical_data_was_uploaded(self, *args):
        """Testing output when no historical data is available. Expected: no message."""
        output_message = create_report(["test_bq_table"], "test", False)
        self.assertEqual(output_message, "")

    @patch('verify_performance_test_results.count_queries', side_effect=[5, 5])
    @patch('verify_performance_test_results.get_average_from', side_effect=[200, 100])
    @patch('verify_performance_test_results.get_stddev_from', return_value=10)
    def test_create_daily_report_when_average_time_increases(self, *args):
        """Testing output when average time increases twice. Expected: 100% increase"""
        output_message = create_report(["test_bq_table"], "test", False)
        assert ", change +100.000%" in output_message

    #TODO: Add more testing scenarios, when single performance tests will be finished.

if __name__ == '__main__':
    unittest.main()