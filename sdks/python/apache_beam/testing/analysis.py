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
import json
import os
import requests
import sys
from typing import Dict
from typing import List
from typing import Optional

from apache_beam.testing.load_tests import load_test_metrics_utils

TITLE_TEMPLATE = """
  Performance regression found in the test: {}
""".format(sys.argv[0])
# TODO: Add mean value before and mean value after.
_METRIC_DESCRIPTION = """
  Affected metric: {}
"""
_METRIC_INFO = """
  timestamp: {} metric_name: {}, metric_value: {}
"""
GH_ISSUE_LABELS = ['testing']


class ChangePointAnalysis:
  def __init__(self, data: List[Dict], metric_name: str):
    self.data = data
    self.metric_name = metric_name

  def get_failing_test_description(self):
    metric_description = _METRIC_DESCRIPTION.format(self.metric_name) + 2 * '\n'
    for data in self.data:
      metric_description += _METRIC_INFO.format(
          data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL],
          data[load_test_metrics_utils.METRICS_TYPE_LABEL],
          data[load_test_metrics_utils.VALUE_LABEL]) + '\n'
    return metric_description

  def find_change_point(self) -> bool:
    """
    TODO: Implementation of change point analysis using ruptures.
    pip install ruptures
    """
    return False


class GitHubIssues:
  def __init__(self, owner='apache', repo='beam'):
    self.owner = owner
    self.repo = repo
    self._github_token = os.environ['GITHUB_TOKEN']
    if not self._github_token:
      raise Exception(
          'A Github Personal Access token is required to create Github Issues.')
    self.headers = {
        "Authorization": 'token {}'.format(self._github_token),
        "Accept": "application/vnd.github+json"
    }

  def create_or_update_issue(
      self, title, description, labels: Optional[List] = None):
    """
    Create an issue with title, description with a label.
    If an issue is already present, comment on the issue instead of
    creating a duplicate issue.
    """
    last_created_issue = self.search_issue_with_title(title, labels)
    if last_created_issue['total_count']:
      self.comment_on_issue(last_created_issue=last_created_issue)
    else:
      url = "https://api.github.com/repos/{}/{}/issues".format(
          self.owner, self.repo)
      data = {
          'owner': self.owner,
          'repo': self.repo,
          'title': title,
          'body': description,
          'labels': labels
      }
      requests.post(url=url, data=json.dumps(data), headers=self.headers)

  def comment_on_issue(self, last_created_issue):
    """
    If there is an already present issue with the title name,
    update that issue with the new description.
    """
    items = last_created_issue['items'][0]
    comment_url = items['comments_url']
    issue_number = items['number']
    _COMMENT = 'Creating comment on already created issue.'
    data = {
        'owner': self.owner,
        'repo': self.repo,
        'body': _COMMENT,
        issue_number: issue_number,
    }
    requests.post(comment_url, json.dumps(data), headers=self.headers)

  def search_issue_with_title(self, title, labels=None):
    """
    Filter issues using title.
    """
    search_query = "repo:{}/{}+{} type:issue is:open".format(
        self.owner, self.repo, title)
    if labels:
      for label in labels:
        search_query = search_query + ' label:{}'.format(label)
    query = "https://api.github.com/search/issues?q={}".format(search_query)

    response = requests.get(url=query, headers=self.headers)
    return response.json()
