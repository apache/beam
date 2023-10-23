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

from apache_beam.testing.analyzers import constants
from apache_beam.testing.analyzers.perf_analysis_utils import MetricContainer
from apache_beam.testing.analyzers.perf_analysis_utils import TestConfigContainer

try:
  _GITHUB_TOKEN: Optional[str] = os.environ['GITHUB_TOKEN']
except KeyError as e:
  _GITHUB_TOKEN = None
  logging.warning(
      'A Github Personal Access token is required '
      'to create Github Issues.')

_GITHUB_REPO_OWNER = os.environ.get('REPO_OWNER', 'apache')
_GITHUB_REPO_NAME = os.environ.get('REPO_NAME', 'beam')
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

_REQUEST_TIMEOUT_SECS = 60


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
      _GITHUB_REPO_OWNER, _GITHUB_REPO_NAME)
  data = {
      'owner': _GITHUB_REPO_OWNER,
      'repo': _GITHUB_REPO_NAME,
      'title': title,
      'body': description,
      'labels': [_AWAITING_TRIAGE_LABEL, _PERF_ALERT_LABEL]
  }
  if labels:
    data['labels'].extend(labels)  # type: ignore
  response = requests.post(
      url=url,
      data=json.dumps(data),
      headers=_HEADERS,
      timeout=_REQUEST_TIMEOUT_SECS).json()
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
      _GITHUB_REPO_OWNER, _GITHUB_REPO_NAME, issue_number)
  open_issue_response = requests.get(
      url,
      json.dumps({
          'owner': _GITHUB_REPO_OWNER,
          'repo': _GITHUB_REPO_NAME,
          'issue_number': issue_number
      },
                 default=str),
      headers=_HEADERS,
      timeout=_REQUEST_TIMEOUT_SECS).json()
  if open_issue_response['state'] == 'open':
    data = {
        'owner': _GITHUB_REPO_OWNER,
        'repo': _GITHUB_REPO_NAME,
        'body': comment_description,
        issue_number: issue_number,
    }

    response = requests.post(
        open_issue_response['comments_url'],
        json.dumps(data),
        headers=_HEADERS,
        timeout=_REQUEST_TIMEOUT_SECS)
    return True, response.json()['html_url']
  return False, ''


def add_awaiting_triage_label(issue_number: int):
  url = 'https://api.github.com/repos/{}/{}/issues/{}/labels'.format(
      _GITHUB_REPO_OWNER, _GITHUB_REPO_NAME, issue_number)
  requests.post(
      url,
      json.dumps({'labels': [_AWAITING_TRIAGE_LABEL]}),
      headers=_HEADERS,
      timeout=_REQUEST_TIMEOUT_SECS)


def get_issue_description(
    test_config_container: TestConfigContainer,
    metric_container: MetricContainer,
    change_point_index: int,
    max_results_to_display: int = 5,
) -> str:
  """
  Args:
    test_config_container: TestConfigContainer containing test metadata.
    metric_container: MetricContainer containing metric data.
    change_point_index: Index of the change point in the metric data.
    max_results_to_display: Max number of results to display from the change
      point index, in both directions of the change point index.

  Returns:
    str: Description used to fill the GitHub issues description.
  """

  # TODO: Add mean and median before and after the changepoint index.

  description = []

  description.append(
      _ISSUE_DESCRIPTION_TEMPLATE.format(
          test_config_container.test_id, test_config_container.metric_name))

  if test_config_container.test_name:
    description.append(("`test_name:` " + f'{test_config_container.test_name}'))

  if test_config_container.test_description:
    description.append(
        ("`Test description:` " + f'{test_config_container.test_description}'))

  description.append('```')

  runs_to_display = []
  max_timestamp_index = min(
      change_point_index + max_results_to_display,
      len(metric_container.values) - 1)
  min_timestamp_index = max(0, change_point_index - max_results_to_display)

  # run in reverse to display the most recent runs first.
  for i in reversed(range(min_timestamp_index, max_timestamp_index + 1)):
    row_template = _METRIC_INFO_TEMPLATE.format(
        metric_container.timestamps[i].ctime(),
        format(metric_container.values[i], '.2f'))
    if i == change_point_index:
      row_template += constants._ANOMALY_MARKER
    runs_to_display.append(row_template)

  description.append(os.linesep.join(runs_to_display))
  description.append('```')
  return (2 * os.linesep).join(description)


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
