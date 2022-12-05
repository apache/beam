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

import pandas as pd
import requests

try:
  _GITHUB_TOKEN: Optional[str] = os.environ['GITHUB_TOKEN']
except KeyError as e:
  _GITHUB_TOKEN = None
  logging.warning(
      'A Github Personal Access token is required '
      'to create Github Issues.')

# TODO: Change the REPO owner name to apache before merging.
_BEAM_GITHUB_REPO_OWNER = 'AnandInguva'
_BEAM_GITHUB_REPO_NAME = 'beam'
# Adding GitHub Rest API version to the header to maintain version stability.
# For more information, please look at
# https://github.blog/2022-11-28-to-infinity-and-beyond-enabling-the-future-of-githubs-rest-api-with-api-versioning/ # pylint: disable=line-too-long
_HEADERS = {
    "Authorization": 'token {}'.format(_GITHUB_TOKEN),
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

# Fill the GitHub issue description with the below variables.
_ISSUE_DESCRIPTION_HEADER = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
_AWAITING_TRIAGE_LABEL = 'awaiting triage'


def create_issue(
    title: str,
    description: str,
    labels: Optional[List[str]] = None,
) -> Tuple[int, str]:
  """
  Create an issue with title, description with a label.

  Args:
    title:  GitHub issue title.
    description: GitHub issue description.
    labels: Labels used to tag the GitHub issue.
  Returns:
    Tuple containing GitHub issue number and issue URL.
  """
  url = "https://api.github.com/repos/{}/{}/issues".format(
      _BEAM_GITHUB_REPO_OWNER, _BEAM_GITHUB_REPO_NAME)
  data = {
      'owner': _BEAM_GITHUB_REPO_OWNER,
      'repo': _BEAM_GITHUB_REPO_NAME,
      'title': title,
      'body': description,
  }
  if labels:
    data['labels'] = labels + [_AWAITING_TRIAGE_LABEL]
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
  Returns:
    Boolean, indicating a comment was added to issue, and URL directing to
     the comment.
  """
  url = 'https://api.github.com/repos/{}/{}/issues/{}'.format(
      _BEAM_GITHUB_REPO_OWNER, _BEAM_GITHUB_REPO_NAME, issue_number)
  open_issue_response = requests.get(
      url,
      json.dumps({
          'owner': _BEAM_GITHUB_REPO_OWNER,
          'repo': _BEAM_GITHUB_REPO_NAME,
          'issue_number': issue_number
      }),
      headers=_HEADERS).json()
  if open_issue_response['state'] == 'open':
    data = {
        'owner': _BEAM_GITHUB_REPO_OWNER,
        'repo': _BEAM_GITHUB_REPO_NAME,
        'body': comment_description,
        issue_number: issue_number,
    }
    response = requests.post(
        open_issue_response['comments_url'], json.dumps(data), headers=_HEADERS)
    return True, response.json()['html_url']
  return False, None


def add_label_to_issue(issue_number: int, labels: Optional[List[str]] = None):
  url = 'https://api.github.com/repos/{}/{}/issues/{}/labels'.format(
      _BEAM_GITHUB_REPO_OWNER, _BEAM_GITHUB_REPO_NAME, issue_number)
  if labels:
    requests.post(url, json.dumps({'labels': labels}), headers=_HEADERS)


def get_issue_description(
    metric_name: str,
    timestamps: List[pd.Timestamp],
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
      change_point_index + max_results_to_display + 1, len(metric_values))
  lower_bound = max(0, change_point_index - max_results_to_display)
  for i in range(lower_bound, upper_bound):
    indices_to_display.append(i)

  indices_to_display.sort()
  description = _ISSUE_DESCRIPTION_HEADER.format(metric_name) + 2 * '\n'
  for index_to_display in indices_to_display:
    description += _METRIC_INFO.format(
        timestamps[index_to_display].ctime(), metric_values[index_to_display])
    if index_to_display == change_point_index:
      description += ' <---- Anomaly'
    description += '\n'
  return description


def report_change_point_on_issues(
    title: str,
    issue_number: Optional[int],
    description: str,
    labels: Optional[List[str]] = None) -> Tuple[int, str]:
  """
  Looks for a GitHub issue with the issue number. First, we try to
  find the issue that's open and comment on it with the provided description.
  If that issue is closed, we create a new issue.
  """
  if issue_number is not None:
    commented_on_issue, issue_url = comment_on_issue(
          issue_number=issue_number,
          comment_description=description
          )
    if commented_on_issue:
      add_label_to_issue(
          issue_number=issue_number, labels=[_AWAITING_TRIAGE_LABEL])
      return issue_number, issue_url
  return create_issue(title=title, description=description, labels=labels)
