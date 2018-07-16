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

import unittest
from mock import patch, mock_open
from jira_manager import JiraManager

class JiraManagerTest(unittest.TestCase):
  """Tests for `jira_manager.py`."""

  def setUp(self):
    print("Test name:", self._testMethodName)


  @patch('jira_manager.JiraClient')
  def test_find_owners_with_single_owner(self, *args):
    owners_yaml = """
                  deps:
                    dep0:
                      owners: owner0,
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      primary, owners = manager._find_owners('dep0')
      self.assertEqual(primary, 'owner0')
      self.assertEqual(len(owners), 0)


  @patch('jira_manager.JiraClient')
  def test_find_owners_with_multi_owners(self, *args):
    owners_yaml = """
                  deps:
                    dep0:
                      owners: owner0, owner1 , owner2,
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      primary, owners = manager._find_owners('dep0')
      self.assertEqual(primary, 'owner0')
      self.assertEqual(len(owners), 2)
      self.assertIn('owner1', owners)
      self.assertIn('owner2', owners)


  @patch('jira_manager.JiraClient')
  def test_find_owners_with_no_owners_defined(self, *args):
    owners_yaml = """
                  deps:
                    dep0:
                      owners:  
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      primary, owners = manager._find_owners('dep0')
      self.assertIsNone(primary)
      self.assertEqual(len(owners), 0)


  @patch('jira_manager.JiraClient')
  def test_find_owners_with_no_dep_defined(self, *args):
    owners_yaml = """
                  deps:
                    dep0:
                      owners:  
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      primary, owners = manager._find_owners('dep100')
      self.assertIsNone(primary)
      self.assertEqual(len(owners), 0)
    pass


if __name__ == '__main__':
  unittest.main()
