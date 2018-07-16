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

import unittest, mock
from jira_manager import JiraManager

class JiraManagerTest(unittest.TestCase):
  """Tests for `jira_manager.py`."""

  def setUp(self):
    print("Test name:", self._testMethodName)


  def test_find_owners_with_single_owner(self):
    pass

  def test_find_owners_with_multi_owners(self):
    pass

  def test_find_owners_with_no_owners_defined(self):
    pass

  def test_find_owners_with_no_dep_defined(self):

