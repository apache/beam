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

from __future__ import print_function
import unittest
from mock import patch, mock_open
from datetime import datetime
from .dependency_check_report_generator import prioritize_dependencies


_PROJECT_ID = 'mock-apache-beam-testing'
_DATASET_ID = 'mock-beam_dependency_states'
_TABLE_ID = 'mock-java_dependency_states'
_SDK_TYPE = 'Java'

# initialize current/latest version release dates for low-priority (LP) and high-priority (HP) dependencies
_LP_CURR_VERSION_DATE = datetime.strptime('2000-01-01', '%Y-%m-%d')
_LATEST_VERSION_DATE = datetime.strptime('2000-01-02', '%Y-%m-%d')
_HP_CURR_VERSION_DATE = datetime.strptime('1999-01-01', '%Y-%m-%d')
_MOCKED_OWNERS_FILE = "deps: "


@patch('google.cloud.bigquery.Client')
@patch('dependency_check.bigquery_client_utils.BigQueryClientUtils.clean_stale_records_from_table')
class DependencyCheckReportGeneratorTest(unittest.TestCase):
  """Tests for `dependency_check_report_generator.py`."""

  def setUp(self):
    print("\n\nTest : " + self._testMethodName)


  @patch('dependency_check.bigquery_client_utils.BigQueryClientUtils')
  def test_empty_dep_input(self, *args):
    """
    Test on empty outdated dependencies.
    Expect: empty report
    """
    with patch('builtins.open', mock_open(read_data=_MOCKED_OWNERS_FILE)):
      report = prioritize_dependencies([], _SDK_TYPE)
      self.assertEqual(len(report), 0)


  @patch('dependency_check.dependency_check_report_generator.find_release_time_from_maven_central',
         side_effect = [_LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                        _LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                        _HP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                        _LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,])
  def test_normal_dep_input(self, *args):
    """
    Test on a normal outdated dependencies set.
    Expect: group1:artifact1, group2:artifact2, and group3:artifact3
    """
    deps = [
      " - group1:artifact1 [1.0.0 -> 3.0.0]",
      " - group2:artifact2 [1.0.0 -> 1.3.0]",
      " - group3:artifact3 [1.0.0 -> 1.1.0]",
      " - group4:artifact4 [1.0.0 -> 1.1.0]"
    ]
    with patch('builtins.open', mock_open(read_data=_MOCKED_OWNERS_FILE)):
      report = prioritize_dependencies(deps, _SDK_TYPE)
      self.assertEqual(len(report), 3)
      self.assertIn('group1:artifact1', report[0])
      self.assertIn('group2:artifact2', report[1])
      self.assertIn('group3:artifact3', report[2])


  @patch('dependency_check.dependency_check_report_generator.find_release_time_from_maven_central',
         side_effect = [_LP_CURR_VERSION_DATE,
                        _LATEST_VERSION_DATE,])
  def test_dep_with_nondigit_major_versions(self, *args):
    """
    Test on a outdated dependency with non-digit major number.
    Expect: group1:artifact1
    """
    deps = [" - group1:artifact1 [Release1-123 -> Release2-456]"]
    with patch('builtins.open', mock_open(read_data=_MOCKED_OWNERS_FILE)):
      report = prioritize_dependencies(deps, _SDK_TYPE)
      self.assertEqual(len(report), 1)
      self.assertIn('group1:artifact1', report[0])


  @patch('dependency_check.dependency_check_report_generator.find_release_time_from_maven_central',
         side_effect = [_LP_CURR_VERSION_DATE,
                        _LATEST_VERSION_DATE,])
  def test_dep_with_nondigit_minor_versions(self, *args):
    """
    Test on a outdated dependency with non-digit minor number.
    Expect: group1:artifact1
    """
    deps = [" - group1:artifact1 [0.rc1.0 -> 0.rc2.0]"]
    with patch('builtins.open', mock_open(read_data=_MOCKED_OWNERS_FILE)):
      report = prioritize_dependencies(deps, _SDK_TYPE)
      self.assertEqual(len(report), 1)
      self.assertIn('group1:artifact1', report[0])


  @patch('dependency_check.dependency_check_report_generator.find_release_time_from_maven_central',
         side_effect = [_HP_CURR_VERSION_DATE,_LATEST_VERSION_DATE,])
  def test_invalid_dep_input(self, *args):
    """
    Test on a invalid outdated dependencies format.
    Expect: Exception through out. And group2:artifact2 is picked.
    """
    deps = [
      "- group1:artifact1 (1.0.0, 2.0.0)",
      " - group2:artifact2 [1.0.0 -> 2.0.0]"
    ]
    with patch('builtins.open', mock_open(read_data=_MOCKED_OWNERS_FILE)):
      report = prioritize_dependencies(deps, _SDK_TYPE)
      self.assertEqual(len(report), 1)
      self.assertIn('group2:artifact2', report[0])


if __name__ == '__main__':
  unittest.main()
  
