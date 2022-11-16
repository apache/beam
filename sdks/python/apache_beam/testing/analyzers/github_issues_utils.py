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
import logging
import os
from typing import List
from typing import Optional
from typing import Tuple

import requests

try:
  _GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
except KeyError as e:
  _GITHUB_TOKEN = None
  logging.warning(
      'A Github Personal Access token is required '
      'to create Github Issues.')

_BEAM_REPO_OWNER = 'AnandInguva'
_BEAM_REPO_NAME = 'beam'
_HEADERS = {
    "Authorization": 'token {}'.format(_GITHUB_TOKEN),
    "Accept": "application/vnd.github+json"
}

# Fill the GitHub issue description with the below variables.
_ISSUE_DESCRIPTION_HEADER = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"


def create_or_comment_issue(
    title: str,
    description: str,
    labels: Optional[List] = None,
    issue_number: Optional[int] = None) -> Tuple[int, str]:
  """
  Create an issue with title, description with a label.
  If an issue is already created and is open,
  then comment on the issue instead of creating a duplicate issue.

  Args:
    title:  GitHub issue title.
    description: GitHub issue description.
    labels: Labels used to tag the GitHub issue.
    issue_number: GitHub issue number used to find the already created issue.
  """
  if issue_number:
    commented_on_issue, comment_url = comment_on_issue(
      issue_number=issue_number,
      comment_description=description)
    if commented_on_issue:
      return issue_number, comment_url

  # Issue number was not provided or issue with provided number
  # is closed. In that case, create a new issue.
  url = "https://api.github.com/repos/{}/{}/issues".format(
      _BEAM_REPO_OWNER, _BEAM_REPO_NAME)
  data = {
      'owner': _BEAM_REPO_OWNER,
      'repo': _BEAM_REPO_NAME,
      'title': title,
      'body': description,
  }
  if labels:
    data['labels'] = labels
  response = requests.post(
      url=url, data=json.dumps(data), headers=_HEADERS).json()
  return response['number'], response['html_url']


def comment_on_issue(issue_number: int,
                     comment_description: str) -> Tuple[bool, Optional[str]]:
  """
  This method looks for an issue with provided issue_number. If an open
  issue is found, comment on the open issue with provided description else
  do nothing.

  Args:
    issue_number: A GitHub issue number.
    comment_description: If an issue with issue_number is open,
      then comment on the issue with the using comment_description.
  """
  url = 'https://api.github.com/repos/{}/{}/issues/{}'.format(
      _BEAM_REPO_OWNER, _BEAM_REPO_NAME, issue_number)
  open_issue_response = requests.get(
      url,
      json.dumps({
          'owner': _BEAM_REPO_OWNER,
          'repo': _BEAM_REPO_NAME,
          'issue_number': issue_number
      }),
      headers=_HEADERS)
  status_code = open_issue_response.status_code
  open_issue_response = open_issue_response.json()
  if status_code == 200 and open_issue_response['state'] == 'open':
    data = {
        'owner': _BEAM_REPO_OWNER,
        'repo': _BEAM_REPO_NAME,
        'body': comment_description,
        issue_number: issue_number,
    }
    response = requests.post(
        open_issue_response['comments_url'], json.dumps(data), headers=_HEADERS)
    return True, response.json()['html_url']

  return False, None


def get_issue_description(
    metric_name: str,
    timestamps: List,
    metric_values: List,
    change_point_index: int,
    max_results_to_display: int = 5) -> str:
  """
  Args:
   metric_name: Metric name used for the Change Point Analysis.
   timestamps: Timestamps of the metrics when they were published to the
    Database.
   metric_values: Values of the metric for the previous runs.
   change_point_index: Index for the change point. The element in the
    index of the metric_values would be the change point.
   max_results_to_display: Max number of results to display from the change
    point index, in both directions of the change point index.

  Returns:
    str: Description used to fill the GitHub issues description.
  """

  # TODO: Add mean and median before and after the changepoint index.
  indices_to_display = []
  upper_bound = min(
      change_point_index + max_results_to_display, len(metric_values))
  for i in range(change_point_index, upper_bound):
    indices_to_display.append(i)
  lower_bound = max(0, change_point_index - max_results_to_display)
  for i in range(lower_bound, change_point_index):
    indices_to_display.append(i)
  indices_to_display.sort()
  description = _ISSUE_DESCRIPTION_HEADER.format(metric_name) + 2 * '\n'
  for i in indices_to_display:
    description += _METRIC_INFO.format(timestamps[i].ctime(), metric_values[i])
    if i == change_point_index:
      description += ' <---- Anomaly'
    description += '\n'
  return description
