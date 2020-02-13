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
import jira_utils
from mock import patch, mock_open, Mock
from .jira_manager import JiraManager
from datetime import datetime

MOCKED_DEP_CURRENT_VERSION = '0.1.0'
MOCKED_DEP_LATEST_VERSION = '1.0.0'

class MockedJiraIssue:
  def __init__(self, key, summary, description, status):
    self.key = key
    self.fields = self.MockedJiraIssueFields(summary, description, status)

  class MockedJiraIssueFields:
    def __init__(self, summary, description, status):
      self.summary = summary
      self.description = description
      self.status = self.MockedJiraIssueStatus(status)
      self.fixVersions = [self.MockedJiraIssueFixVersions()]
      if status == 'Closed':
        self.resolutiondate = '1999-01-01T00:00:00'

    class MockedJiraIssueStatus:
      def __init__(self, status):
        self.name = status

    class MockedJiraIssueFixVersions:
      def __init__(self):
        self.name = '2.8.0'


@patch('jira_utils.jira_manager.JiraClient')
class JiraManagerTest(unittest.TestCase):
  """Tests for `jira_manager.py`."""

  def setUp(self):
    print("\n\nTest : " + self._testMethodName)


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
    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      manager = JiraManager('url', 'username', 'password', owners_yaml)
      manager.run('dep0',
                  MOCKED_DEP_CURRENT_VERSION,
                  MOCKED_DEP_LATEST_VERSION,
                  'Python')
      manager.jira.create_issue.assert_called_once_with(self._get_experct_summary('dep0'),
                                                        ['dependencies'],
                                                        self._get_expected_description('dep0', MOCKED_DEP_LATEST_VERSION, ['owner0', 'owner1', 'owner2']),
                                                        )


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  def test_run_with_updating_existing_task(self, *args):
    """
    Test JiraManager.run on updating an existing issue.
    Expect: jira.update_issue is called once.
    """
    dep_name = 'dep0'
    owners_yaml = """
                  deps:
                    dep0:
                      owners:
                  """
    summary = self._get_experct_summary(dep_name)
    description = self._get_expected_description(dep_name, MOCKED_DEP_LATEST_VERSION, [])

    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        return_value=[MockedJiraIssue('BEAM-1000', summary, description, 'Open')]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name,
                    MOCKED_DEP_CURRENT_VERSION,
                    MOCKED_DEP_LATEST_VERSION,
                    'Python')
        manager.jira.update_issue.assert_called_once()


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  def test_run_with_creating_new_subtask(self, *args):
    """
    Test JiraManager.run on creating a new sub-task.
    Expect: jira.create_issue is called once with certain parameters.
    """
    dep_name = 'group0:artifact0'
    owners_yaml = """
                  deps:
                    group0:artifact0:
                      group: group0
                      artifact: artifact0
                      owners: owner0
                  """
    summary = self._get_experct_summary('group0')
    description = self._get_expected_description(dep_name, MOCKED_DEP_LATEST_VERSION, [])

    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Open')],
                      []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name,
                    MOCKED_DEP_CURRENT_VERSION,
                    MOCKED_DEP_LATEST_VERSION,
                    'Java',
                    group_id='group0')
        manager.jira.create_issue.assert_called_once_with(self._get_experct_summary(dep_name),
                                                          ['dependencies'],
                                                          self._get_expected_description(dep_name, MOCKED_DEP_LATEST_VERSION, ['owner0']),
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
    owners_yaml = """
                  deps:
                    group0:artifact0:
                      group: group0
                      artifact: artifact0
                      owners: owner0
                  """
    summary = self._get_experct_summary('group0')
    description = self._get_expected_description(dep_name, MOCKED_DEP_LATEST_VERSION, [])
    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
        side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Closed')],
                      []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name,
                    MOCKED_DEP_CURRENT_VERSION,
                    MOCKED_DEP_LATEST_VERSION,
                    sdk_type='Java',
                    group_id='group0')
        manager.jira.reopen_issue.assert_called_once()


  @patch('jira_utils.jira_manager.datetime', Mock(today=Mock(return_value=datetime.strptime('2000-01-01', '%Y-%m-%d'))))
  @patch.object(jira_utils.jira_manager.JiraManager,
                '_get_next_release_version', side_effect=['2.8.0.dev'])
  def test_run_with_reopening_issue_with_fixversions(self, *args):
    """
    Test JiraManager.run on reopening an issue when JIRA fixVersions hits the release version.
    Expect: jira.reopen_issue is called once.
    """
    dep_name = 'dep0'
    owners_yaml = """
                  deps:
                    dep0:
                      owners:
                  """
    summary = self._get_experct_summary(dep_name)
    description = self._get_expected_description(dep_name, MOCKED_DEP_LATEST_VERSION, [])
    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
                 side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Closed')],
                                []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name,
                    MOCKED_DEP_CURRENT_VERSION,
                    MOCKED_DEP_LATEST_VERSION,
                    'Python')
        manager.jira.reopen_issue.assert_called_once()


  @patch.object(jira_utils.jira_manager.JiraManager,
                '_get_next_release_version', side_effect=['2.9.0.dev'])
  def test_run_with_reopening_issue_with_new_release_available(self, *args):
    """
    Test JiraManager.run that reopens an issue once 3 versions releases after 6
    months since previous closure.
    Expect: jira.reopen_issue is called once.
    """
    dep_name = 'dep0'
    issue_closed_version = '1.0'
    dep_latest_version = '1.3'
    owners_yaml = """
                    deps:
                      dep0:
                        owners:
                    """
    summary = self._get_experct_summary(dep_name)
    description = self._get_expected_description(dep_name, issue_closed_version, [])
    with patch('builtins.open', mock_open(read_data=owners_yaml)):
      with patch('jira_utils.jira_manager.JiraManager._search_issues',
                 side_effect = [[MockedJiraIssue('BEAM-1000', summary, description, 'Closed')],
                                []]):
        manager = JiraManager('url', 'username', 'password', owners_yaml)
        manager.run(dep_name,
                    MOCKED_DEP_CURRENT_VERSION,
                    dep_latest_version,
                    'Python')
        manager.jira.reopen_issue.assert_called_once()


  def _get_experct_summary(self, dep_name):
    return 'Beam Dependency Update Request: ' + dep_name


  def _get_expected_description(self, dep_name, dep_latest_version, owners):
    description =  """\n\n ------------------------- {0} -------------------------\n
        Please consider upgrading the dependency {1}. \n
        The current version is {2}. The latest version is {3} \n
        cc: """.format(
        "2000-01-01 00:00:00",
        dep_name,
        MOCKED_DEP_CURRENT_VERSION,
        dep_latest_version
    )
    for owner in owners:
      description += "[~{0}], ".format(owner)
    description += ("\n Please refer to "
                    "[Beam Dependency Guide |https://beam.apache.org/contribute/dependencies/]"
                    "for more information. \n"
                    "Do Not Modify The Description Above. \n")
    return description


if __name__ == '__main__':
  unittest.main()
