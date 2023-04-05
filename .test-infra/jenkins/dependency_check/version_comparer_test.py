#!/usr/bin/env python
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

from . import version_comparer
import unittest

class VersionComparerTest(unittest.TestCase):
  """Tests for `version_comparer.py`."""

  def setUp(self):
    print("\n\nTest : " + self._testMethodName)

  def test_compare_major_verison_true(self):
    curr_ver = '1.0.0'
    latest_ver = '2.0.0'
    self.assertTrue(version_comparer.compare_dependency_versions(curr_ver, latest_ver))

  def test_compare_minor_version_true(self):
    curr_ver = '1.0.0'
    latest_ver = '1.3.0'
    self.assertTrue(version_comparer.compare_dependency_versions(curr_ver, latest_ver))

  def test_compare_non_semantic_version_true(self):
    curr_ver = '1.rc1'
    latest_ver = '1.rc2'
    self.assertTrue(version_comparer.compare_dependency_versions(curr_ver, latest_ver))

  def test_compare_minor_version_false(self):
    curr_ver = '1.0.0'
    latest_ver = '1.2.0'
    self.assertFalse(version_comparer.compare_dependency_versions(curr_ver, latest_ver))

  def test_compare_same_version_false(self):
    curr_ver = '1.0.0'
    latest_ver = '1.0.0'
    self.assertFalse(version_comparer.compare_dependency_versions(curr_ver, latest_ver))

if __name__ == '__main__':
  unittest.main()
