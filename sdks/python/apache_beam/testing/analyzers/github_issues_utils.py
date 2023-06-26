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

_BEAM_GITHUB_REPO_OWNER = 'apache'
_BEAM_GITHUB_REPO_NAME = 'beam'
# Adding GitHub Rest API version to the header to maintain version stability.
# For more information, please look at
# https://github.blog/2022-11-28-to-infinity-and-beyond-enabling-the-future-of-githubs-rest-api-with-api-versioning/ # pylint: disable=line-too-long
_HEADERS = {
    "Authorization": 'token {}'.format(_GITHUB_TOKEN),
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

_ISSUE_TITLE_TEMPLATE = """
  Performance Regression or Improvement: {}:{}
"""

_ISSUE_DESCRIPTION_TEMPLATE = """
  Performance change found in the
  test: `{}` for the metric: `{}`.

  For more information on how to triage the alerts, please look at
  `Triage performance alert issues` section of the [README](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/testing/analyzers/README.md#triage-performance-alert-issues).
"""
_METRIC_INFO_TEMPLATE = "timestamp: {}, metric_value: {}"
_AWAITING_TRIAGE_LABEL = 'awaiting triage'
_PERF_ALERT_LABEL = 'perf-alert'


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
      'labels': [_AWAITING_TRIAGE_LABEL, _PERF_ALERT_LABEL]
  }
  if labels:
    data['labels'].extend(labels)  # type: ignore
  response = requests.post(
      url=url, data=json.dumps(data), headers=_HEADERS).json()
  return response['number'], response['html_url']


def comment_on_issue(issue_number: int,
                     comment_description: str) -> Tuple[bool, str]:
  """
  This method looks for an issue with provided issue_number. If an open
  issue is found, comment on the open issue with provided description else
  do nothing.

  Args:
    issue_number: A GitHub issue number.
    comment_description: If an issue with issue_number is open,
      then comment on the issue with the using comment_description.
  Returns:
    tuple[bool, Optional[str]] indicating if a comment was added to
      issue, and the comment URL.
  """
  url = 'https://api.github.com/repos/{}/{}/issues/{}'.format(
      _BEAM_GITHUB_REPO_OWNER, _BEAM_GITHUB_REPO_NAME, issue_number)
  open_issue_response = requests.get(
      url,
      json.dumps({
          'owner': _BEAM_GITHUB_REPO_OWNER,
          'repo': _BEAM_GITHUB_REPO_NAME,
          'issue_number': issue_number
      },
                 default=str),
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
  return False, ''


def add_awaiting_triage_label(issue_number: int):
  url = 'https://api.github.com/repos/{}/{}/issues/{}/labels'.format(
      _BEAM_GITHUB_REPO_OWNER, _BEAM_GITHUB_REPO_NAME, issue_number)
  requests.post(
      url, json.dumps({'labels': [_AWAITING_TRIAGE_LABEL]}), headers=_HEADERS)


def get_issue_description(
    test_name: str,
    metric_name: str,
    timestamps: List[pd.Timestamp],
    metric_values: List,
    change_point_index: int,
    max_results_to_display: int = 5,
    test_description: Optional[str] = None,
) -> str:
  """
  Args:
   metric_name: Metric name used for the Change Point Analysis.
   timestamps: Timestamps of the metrics when they were published to the
    Database. Timestamps are expected in ascending order.
   metric_values: metric values for the previous runs.
   change_point_index: Index for the change point. The element in the
    index of the metric_values would be the change point.
   max_results_to_display: Max number of results to display from the change
    point index, in both directions of the change point index.

  Returns:
    str: Description used to fill the GitHub issues description.
  """

  # TODO: Add mean and median before and after the changepoint index.
  max_timestamp_index = min(
      change_point_index + max_results_to_display, len(metric_values) - 1)
  min_timestamp_index = max(0, change_point_index - max_results_to_display)

  description = _ISSUE_DESCRIPTION_TEMPLATE.format(
      test_name, metric_name) + 2 * '\n'

  description += (
      "`Test description:` " + f'{test_description}' +
      2 * '\n') if test_description else ''

  description += '```' + '\n'
  runs_to_display = [
      _METRIC_INFO_TEMPLATE.format(
          timestamps[i].ctime(), format(metric_values[i], '.2f'))
      for i in reversed(range(min_timestamp_index, max_timestamp_index + 1))
  ]

  runs_to_display[change_point_index - min_timestamp_index] += " <---- Anomaly"
  description += '\n'.join(runs_to_display) + '\n'
  description += '```' + '\n'
  return description


def report_change_point_on_issues(
    title: str,
    description: str,
    labels: Optional[List[str]] = None,
    existing_issue_number: Optional[int] = None,
) -> Tuple[int, str]:
  """
  Comments the description on the existing issue (if provided and still open),
   or creates a new issue.
  """
  if existing_issue_number is not None:
    commented_on_issue, issue_url = comment_on_issue(
          issue_number=existing_issue_number,
          comment_description=description
          )
    if commented_on_issue:
      add_awaiting_triage_label(issue_number=existing_issue_number)
      return existing_issue_number, issue_url
  return create_issue(title=title, description=description, labels=labels)
