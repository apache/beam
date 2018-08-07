from __future__ import print_function
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
from mock import patch, mock_open, Mock
from jira_manager import JiraManager
from datetime import datetime


class MockedJiraIssue:
  def __init__(self, key, summary, description, status):
    self.key = key
    self.fields = self.MockedJiraIssueFields(summary, description, status)

  class MockedJiraIssueFields:
    def __init__(self, summary, description, status):
      self.summary = summary
      self.description = description
      self.status = self.MockedJiraIssueStatus(status)

    class MockedJiraIssueStatus:
      def __init__(self, status):
        self.name = status


@patch('jira_utils.jira_manager.JiraClient')
class JiraManagerTest(unittest.TestCase):
  """Tests for `jira_manager.py`."""

  def setUp(self):
    print("\n\nTest : " + self._testMethodName)


  def test_find_owners_with_single_owner(self, *args):
    """
    Test on _find_owners with single owner
    Expect: the primary owner is 'owner0', an empty list of other owners.
    """
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


  def test_find_owners_with_multi_owners(self, *args):
    """
    Test on _find_owners with multiple owners.
    Expect: the primary owner is 'owner0', a list contains 'owner1' and 'owner2'.
    """
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


  def test_find_owners_with_no_owners_defined(self, *args):
    """
    Test on _find_owners without owner.
    Expect: the primary owner is None, an empty list of other owners.
    """
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


  def test_find_owners_with_no_dep_defined(self, *args):
    """
    Test on _find_owners with non-defined dep.
    Expect: through out KeyErrors. The primary owner is None, an empty list of other owners.
    """
    owners_yaml = """
                  deps:
                    dep0:
                      owners:
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      primary, owners = manager._find_owners('dep1')
      self.assertIsNone(primary)
      self.assertEqual(len(owners), 0)


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  def test_run_with_creating_new_issue(self, *args):
    """
    Test JiraManager.run on creating a new issue.
    Expect: jira.create_issue is called once with certain parameters.
    """
    owners_yaml = """
                  deps:
                    dep0:
                      owners: owner0, owner1 , owner2,
                  """
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      manager.run('dep0', '1.0', 'Python')
      manager.jira.create_issue.assert_called_once_with(self._get_experct_summary('dep0', '1.0'),
                                                        ['dependencies'],
                                                        self._get_expected_description('dep0', '1.0', ['owner1', 'owner2']),
                                                        assignee='owner0')


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  def test_run_with_updating_existing_task(self, *args):
    """
    Test JiraManager.run on updating an existing issue.
    Expect: jira.update_issue is called once.
    """
    dep_name = 'dep0'
    dep_latest_version = '1.0'
    owners_yaml = """
                  deps:
                    dep0:
                      owners:
                  """
    summary = self._get_experct_summary(dep_name, dep_latest_version)
    description = self._get_expected_description(dep_name, dep_latest_version, [])

    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        return_value=[MockedJiraIssue('BEAM-1000', summary, description, 'Open')]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name, dep_latest_version, 'Python')
        manager.jira.update_issue.assert_called_once()


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  def test_run_with_creating_new_subtask(self, *args):
    """
    Test JiraManager.run on creating a new sub-task.
    Expect: jira.create_issue is called once with certain parameters.
    """
    dep_name = 'group0:artifact0'
    dep_latest_version = '1.0'
    owners_yaml = """
                  deps:
                    group0:artifact0:
                      group: group0
                      artifact: artifact0
                      owners: owner0
                  """
    summary = self._get_experct_summary('group0', None)
    description = self._get_expected_description(dep_name, dep_latest_version, [])

    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Open')],
                      []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name, dep_latest_version, 'Java', group_id='group0')
        manager.jira.create_issue.assert_called_once_with(self._get_experct_summary(dep_name, dep_latest_version),
                                                          ['dependencies'],
                                                          self._get_expected_description(dep_name, dep_latest_version, []),
                                                          assignee='owner0',
                                                          parent_key='BEAM-1000',
                                                          )


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  @patch('jira_utils.jira_manager.JiraClient.create_issue', side_effect = [MockedJiraIssue('BEAM-2000', 'summary', 'description', 'Open')])
  def test_run_with_reopening_existing_parent_issue(self, *args):
    """
    Test JiraManager.run on reopening a parent issue.
    Expect: jira.reopen_issue is called once.
    """
    dep_name = 'group0:artifact0'
    dep_latest_version = '1.0'
    owners_yaml = """
                  deps:
                    group0:artifact0:
                      group: group0
                      artifact: artifact0
                      owners: owner0
                  """
    summary = self._get_experct_summary('group0', None)
    description = self._get_expected_description(dep_name, dep_latest_version, [])
    with patch('__builtin__.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Closed')],
                      []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name, dep_latest_version, sdk_type='Java', group_id='group0')
        manager.jira.reopen_issue.assert_called_once()


  def _get_experct_summary(self, dep_name, dep_latest_version):
    summary =  'Beam Dependency Update Request: ' + dep_name
    if dep_latest_version:
      summary = summary + " " + dep_latest_version
    return summary


  def _get_expected_description(self, dep_name, dep_latest_version, owners):
    description = """\n\n{0}\n
        Please review and upgrade the {1} to the latest version {2} \n 
        cc: """.format(
      datetime.strptime('2000-01-01', '%Y-%m-%d'),
      dep_name,
      dep_latest_version
    )
    for owner in owners:
      description += "[~{0}], ".format(owner)
    return description


if __name__ == '__main__':
  unittest.main()
