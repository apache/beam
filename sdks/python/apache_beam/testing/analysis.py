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

import requests
import os
import json
from typing import Optional
from typing import List

# def binary_segmentation_algo(data, breakpoints=2):
#   model = "l2"
#   algo = rpt.Binseg(model=model).fit(data)
#   result = algo.predict(breakpoints)
#   print(result)


class ChangePointAnalysis:
  def __init__(self, data):
    self.data = data

  def find_change_point(self):
    """
    TODO: Implementation of change point analysis using ruptures.
    pip install ruptures
    """
    return False


class GitHubIssues:
  def __init__(self, owner, repo):
    self.owner = owner
    self.repo = repo
    self.query = "https://api.github.com/repos/{}/{}/issues".format(
        self.owner, self.repo)

    self._github_token = os.environ['GITHUB_TOKEN']
    self.headers = {
        "Authorization": 'token {}'.format(self._github_token),
        "Accept": "application/vnd.github+json"
    }
    if not self._github_token:
      raise Exception(
          'A Github Personal Access token is required to create Github Issues.')

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
      data = {
          'owner': self.owner,
          'repo': self.repo,
          'title': title,
          'body': description,
          'labels': labels
      }
      requests.post(url=self.query, data=json.dumps(data), headers=self.headers)

  def comment_on_issue(self, last_created_issue):
    """
    If there is an already present issue with the title name,
    update that issue with the new description.
    """
    if last_created_issue['total_count']:
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
