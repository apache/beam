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


from jira import JIRA

class JiraClient:

  def __init__(self, options, basic_auth, project):
    self.jira = JIRA(options, basic_auth=basic_auth)
    self.project = project


  def get_issues_by_summary(self, summary):
    """
    Find issues by using the summary (issue title)
    Args:
      summary
    Return:
      A list of issues
    """
    try:
      issues = self.jira.search_issues("project={0} AND summary ~ '{1}'".format(self.project, summary))
    except Exception:
      raise
    return issues


  def get_issue_by_key(self, key):
    """
    Find issue by using the key (e.g BEAM-1234)
    Args:
      key
    Return:
      issue
    """
    try:
      issue = self.jira.issue(key)
    except Exception:
      raise
    return issue


  def create_issue(self, summary, components, description, issuetype='Bug', assignee=None, parent_key=None):
    """
    Create a new issue
    Args:
      summary - Issue title
      components - A list of components
      description (optional) - A string that describes the issue
      issuetype (optional) - Bug, Improvement, New Feature, Sub-task, Task, Wish, etc.
      assignee (optional) - A string of JIRA user name
      parent_key (optional) - The parent issue key is required when creating a subtask.
    Return:
      Issue created
    """
    fields = {
      'project': {'key': self.project},
      'summary': summary,
      'description': description,
      'issuetype': {'name': issuetype},
      'components': [],
    }
    for component in components:
      fields['components'].append({'name': component})
    if assignee is not None:
      fields['assignee'] = {'name': assignee}
    if parent_key is not None:
      fields['parent'] = {'key': parent_key}
      fields['issuetype'] = {'name': 'Sub-task'}
    try:
      new_issue = self.jira.create_issue(fields = fields)
    except Exception:
      raise
    return new_issue


  def update_issue(self, issue, summary=None, components=None, description=None, assignee=None, notify=True):
    """
    Create a new issue
    Args:
      issue - Jira issue object
      summary (optional) - Issue title
      components (optional) - A list of components
      description (optional) - A string that describes the issue
      assignee (optional) - A string of JIRA user name
      notify - Query parameter notifyUsers. If true send the email with notification that the issue was updated to users that watch it.
               Admin or project admin permissions are required to disable the notification.
    Return:
      Issue created
    """
    fields={}
    if summary:
      fields['summary'] = summary
    if description:
      fields['description'] = description
    if assignee:
      fields['assignee'] = {'name': assignee}
    if components:
      fields['components'] = []
      for component in components:
        fields['components'].append({'name': component})
    try:
      issue.update(fields=fields, notify=notify)
    except Exception:
      raise


  def reopen_issue(self, issue):
    """
    Reopen an issue
    Args:
      issue - Jira issue object
    """
    try:
      self.jira.transition_issue(issue.key, 3)
    except:
      raise
