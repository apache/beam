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
    self.query = 'https://api.github.com/repos/{}/{}/issues'.format(
        self.owner, self.repo)
    self._github_token = os.environ['GITHUB_TOKEN']
    self.headers = {
        "Authorization": 'token {}'.format(self._github_token),
        "Accept": "application/vnd.github+json"
    }
    if not self._github_token:
      raise Exception(
          'A Github Personal Access token is required to create Github Issues.')

  def create_issue(self, title, description, label: str = 'bug'):
    current_issue = self.search_issue_with_title(title, label)
    if current_issue['total_count']:
      issue_number = current_issue['items']['number']
      print(issue_number)
      raise NotImplementedError
    else:
      data = {
          'owner': self.owner,
          'repo': self.repo,
          'title': title,
          'body': description,
          'label': label
      }
      r = requests.post(
          url=self.query, data=json.dumps(data), headers=self.headers)
      assert r.status_code == 201

  def search_issue_with_title(self, title, label):
    search_query = "repo:{}/{}+{} type:issue is:open label:{}".format(
        self.owner, self.repo, title, label)
    query = "https://api.github.com/search/issues?q={}".format(search_query)

    response = requests.get(url=query, headers=self.headers)
    print(response.json())
    print(response.json()['total_count'])
    return response.json()

  def comment_on_issue(self):
    """
    If there is an already present issue with the title name,
    update that issue with the new description.
    """
    raise NotImplementedError


# my_issues = GitHubIssues(owner='AnandInguva', repo='beam')
#
# my_issues.create_issue(title='Found a ',
#   description='Testing creating issues with Github rest APIs',
#                      label='bug')
