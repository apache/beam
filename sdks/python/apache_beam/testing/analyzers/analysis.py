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
import argparse
import json
import logging
import os
import time
import uuid
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import google.api_core.exceptions
import numpy as np
import pandas as pd
import requests
import yaml

from apache_beam.testing.load_tests import load_test_metrics_utils
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsPublisher
from apache_beam.testing.load_tests.load_test_metrics_utils import FetchMetrics
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive

BQ_PROJECT_NAME = 'apache-beam-testing'
BQ_DATASET = 'beam_perf_storage'
OWNER = 'apache'
REPO = 'beam'

ID_LABEL = 'test_id'
ISSUE_CREATION_TIMESTAMP_LABEL = 'issue_timestamp'
CHANGEPOINT_TIMESTAMP_LABEL = 'change_point_timestamp'
CHANGE_POINT_LABEL = 'change_point'
TEST_NAME = 'test_name'
METRIC_NAME = 'metric_name'
ISSUE_NUMBER = 'issue_number'
ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions
NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10

_LOGGER = logging.getLogger(__name__)

SCHEMA = [{
    'name': ID_LABEL, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': ISSUE_CREATION_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGEPOINT_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGE_POINT_LABEL,
              'field_type': 'FLOAT64',
              'mode': 'REQUIRED'
          }, {
              'name': METRIC_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': TEST_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }, {
              'name': ISSUE_NUMBER, 'field_type': 'INT64', 'mode': 'REQUIRED'
          }, {
              'name': ISSUE_URL, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }]

TITLE_TEMPLATE = """
  Performance Regression: {}:{}
"""
# TODO: Add mean value before and mean value after.
_METRIC_DESCRIPTION = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
ISSUE_LABELS = ['perf-alerts']


class Metric:
  def __init__(
      self,
      issue_creation_timestamp,
      change_point_timestamp,
      test_name,
      metric_name,
      issue_number,
      issue_url,
      test_id,
      change_point):
    self.issue_creation_timestamp = issue_creation_timestamp
    self.change_point_timestamp = change_point_timestamp
    self.test_name = test_name
    self.metric_name = metric_name
    self.issue_number = issue_number
    self.issue_url = issue_url
    self.test_id = test_id
    self.change_point = change_point

  def as_dict(self):
    return {
        ISSUE_CREATION_TIMESTAMP_LABEL: self.issue_creation_timestamp,
        CHANGEPOINT_TIMESTAMP_LABEL: self.change_point_timestamp,
        TEST_NAME: self.test_name,
        METRIC_NAME: self.metric_name,
        ISSUE_NUMBER: self.issue_number,
        ID_LABEL: self.test_id,
        CHANGE_POINT_LABEL: self.change_point,
        ISSUE_URL: self.issue_url
    }


class ChangePointAnalysis:
  def __init__(
      self,
      data: Union[List[float], List[List[float]], np.ndarray],
      metric_name: str,
  ):
    self.data = data
    self.metric_name = metric_name

  def edivisive_means(
      self, pvalue: float = 0.05, permutations: int = 100) -> List:
    """
    Args:
     pvalue: p value for the permutation test.
     permutations: Number of permutations for the permutation test.

    Returns:
     The indices of change points.
    """
    return e_divisive(self.data, pvalue, permutations)


class RunChangePointAnalysis:
  def __init__(self, args):
    self.change_point_sibling_distance = args.change_point_sibling_distance
    self.changepoint_to_recent_run_window = (
        args.changepoint_to_recent_run_window)

  def is_changepoint_in_valid_window(self, change_point_index: int) -> bool:
    # If the change point is more than N runs behind the most recent run
    # then don't raise an alert for it.
    if self.changepoint_to_recent_run_window >= change_point_index:
      return True
    return False

  def has_sibling_change_point(
      self,
      change_point_index: int,
      metric_values: List,
      metric_name: str,
      test_name: str,
      change_point_timestamp,
  ) -> Optional[Tuple[bool, Optional[int]]]:
    """
    Finds the sibling change point index. If not,
    returns the original changepoint index.

    Sibling changepoint is a neighbor of latest
    changepoint, within the distance of change_point_sibling_distance.
    For sibling changepoint, a GitHub issue is already created.
    """

    # Search backward from the current changepoint
    sibling_indexes_to_search = []
    for i in range(change_point_index - 1, -1, -1):
      if change_point_index - i <= self.change_point_sibling_distance:
        sibling_indexes_to_search.append(i)
    # Search forward from the current changepoint
    for i in range(change_point_index + 1, len(metric_values)):
      if i - change_point_index <= self.change_point_sibling_distance:
        sibling_indexes_to_search.append(i)
    # Look for change points within change_point_sibling_distance.
    # Return the first change point found.
    query_template = """
    SELECT * FROM {project}.{dataset}.{table}
    WHERE {metric_name_id} = '{metric_name}'
    ORDER BY {timestamp} DESC
    LIMIT 10
    """.format(
        project=BQ_PROJECT_NAME,
        dataset=BQ_DATASET,
        metric_name_id=METRIC_NAME,
        metric_name=metric_name,
        timestamp=ISSUE_CREATION_TIMESTAMP_LABEL,
        table=test_name)
    try:
      df = FetchMetrics.fetch_from_bq(query_template=query_template)
    except google.api_core.exceptions.NotFound:
      # If no table found, that means this is first performance regression
      # on the current test:metric.
      return True, None
    previous_change_point = df[CHANGE_POINT_LABEL].tolist()[0]
    previous_change_point_timestamp = df[CHANGEPOINT_TIMESTAMP_LABEL].tolist(
    )[0]
    issue_number = df[ISSUE_NUMBER].tolist()[0]

    # Check if the current changepoint is equal to the
    # latest reported changepoint on GitHub Issues using the
    # value and the timestamp. If it's the same,
    # don't create or comment on issues.
    if ((previous_change_point == metric_values[change_point_index]) and
        (previous_change_point_timestamp == change_point_timestamp)):
      return False, issue_number

    alert = True
    for sibling_index in sibling_indexes_to_search:
      if metric_values[sibling_index] == previous_change_point:
        alert = False
        break
    return alert, issue_number

  def run_change_point_analysis(self, config):
    metric_name = config['metric_name']
    test_name = config['test_name'].replace('.', '_')
    if config['source'] == 'big_query':
      data: pd.DataFrame = FetchMetrics.fetch_from_bq(
          project_name=config['project'],
          dataset=config['metrics_dataset'],
          table=config['metrics_table'],
          metric_name=metric_name)

      metric_values = data[load_test_metrics_utils.VALUE_LABEL].to_list()
      timestamps = data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL].tolist()

      change_point_analyzer = ChangePointAnalysis(
          metric_values, metric_name=metric_name)
      change_points_idx = change_point_analyzer.edivisive_means()

      if not change_points_idx:
        logging.info(
            'No Performance regression found for the test: %s' % test_name)
        return

      # always consider the latest change points
      change_points_idx.sort(reverse=True)
      change_point_index = change_points_idx[0]
      change_point_timestamp = timestamps[change_point_index]

      if not self.is_changepoint_in_valid_window(change_point_index):
        # change point lies outside the window from the recent run.
        # Ignore this changepoint.
        logging.info(
            'Performance regression found for the test: %s. '
            'but not creating an alert since the Change Point '
            'lies outside the '
            'changepoint_to_recent_run_window distance' % test_name)
        return

      create_alert, previous_issue_number = self.has_sibling_change_point(
          change_point_index=change_point_index,
          metric_values=metric_values,
          metric_name=metric_name,
          test_name=test_name,
          change_point_timestamp=change_point_timestamp
      )

      logging.info(
          "Create alert for the test %s: %s" % (test_name, create_alert))

      # alert is created via comment on open issue or as a new issue.
      if 'labels' in config:
        labels = config['labels']
      else:
        labels = ISSUE_LABELS

      if create_alert:
        gh_issue = GitHubIssues()
        issue_number, issue_url = gh_issue.create_or_update_issue(
          title=TITLE_TEMPLATE.format(test_name, metric_name),
          description=gh_issue.issue_description(
          metric_name=metric_name,
          timestamps=timestamps,
          metric_values=metric_values,
          change_point_index=change_point_index,
          max_results_to_display=NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION
          ),
          labels=labels,
          issue_number=previous_issue_number)

        logging.info(
            'Performance regression is alerted on issue #%s. Link to '
            'the issue: %s' % (issue_number, issue_url))

        metric_dict = Metric(
            issue_creation_timestamp=time.time(),
            test_name=test_name,
            metric_name=metric_name,
            test_id=uuid.uuid4().hex,
            change_point=metric_values[change_point_index],
            issue_number=issue_number,
            issue_url=issue_url,
            change_point_timestamp=timestamps[change_point_index])

        bq_metrics_publisher = BigQueryMetricsPublisher(
            project_name=BQ_PROJECT_NAME,
            dataset=BQ_DATASET,
            table=test_name,
            bq_schema=SCHEMA)
        # Add a small delay once the table is created
        # https://github.com/googleapis/google-cloud-go/issues/975
        time.sleep(30)
        bq_metrics_publisher.publish([metric_dict.as_dict()])
        logging.info(
            'GitHub metadata is published to Big Query Dataset %s'
            ', table %s' % (BQ_DATASET, test_name))

    elif config['source'] == 'influxDB':
      raise NotImplementedError
    else:
      raise NotImplementedError

  def read_test_config(self, config_file_path):
    with open(config_file_path, 'r') as stream:
      config = yaml.safe_load(stream)

    for test_config in config.keys():
      self.run_change_point_analysis(config[test_config])


class GitHubIssues:
  def __init__(self, owner=OWNER, repo=REPO):
    self.owner = owner
    self.repo = repo
    try:
      self._github_token = os.environ['GITHUB_TOKEN']
    except KeyError as e:
      raise Exception(
          '{} + A Github Personal Access token is required '
          'to create Github Issues.'.format(e))
    self.headers = {
        "Authorization": 'token {}'.format(self._github_token),
        "Accept": "application/vnd.github+json"
    }

  def create_or_update_issue(
      self,
      title,
      description,
      labels: Optional[List] = None,
      issue_number: Optional[int] = None):
    """
    Create an issue with title, description with a label.
    If an issue is already present, comment on the issue instead of
    creating a duplicate issue.
    """
    if issue_number:
      commented_on_issue, response = self.comment_on_issue(
          issue_number=issue_number, description=description)
      if commented_on_issue:
        return issue_number, response.json()['html_url']

    # Issue number was not provided or Issue with provided number
    # is closed. In that case, create a new issue.
    url = "https://api.github.com/repos/{}/{}/issues".format(
        self.owner, self.repo)
    data = {
        'owner': self.owner,
        'repo': self.repo,
        'title': title,
        'body': description,
    }
    if labels:
      data['labels'] = labels
    response = requests.post(
        url=url, data=json.dumps(data), headers=self.headers).json()
    return response['number'], response['html_url']

  def comment_on_issue(self, issue_number,
                       description) -> Tuple[bool, Optional[requests.Response]]:
    """
    If there is an already created issue,
    update that issue with a comment.
    """
    url = 'https://api.github.com/repos/{}/{}/issues/{}'.format(
        self.owner, self.repo, issue_number)
    open_issue_response = requests.get(
        url,
        json.dumps({
            'owner': self.owner,
            'repo': self.repo,
            'issue_number': issue_number
        }),
        headers=self.headers).json()

    if open_issue_response['state'] == 'open':
      data = {
          'owner': self.owner,
          'repo': self.repo,
          'body': description,
          issue_number: issue_number,
      }
      response = requests.post(
          open_issue_response['comments_url'],
          json.dumps(data),
          headers=self.headers)
      return True, response

    return False, None

  def issue_description(
      self,
      metric_name: str,
      timestamps: List,
      metric_values: List,
      change_point_index: int,
      max_results_to_display: int = 5):
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
    description = _METRIC_DESCRIPTION.format(metric_name) + 2 * '\n'
    for i in indices_to_display:
      description += _METRIC_INFO.format(
          timestamps[i].ctime(), metric_values[i])
      if i == change_point_index:
        description += ' <---- Anomaly'
      description += '\n'
    return description


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--change_point_sibling_distance',
      type=int,
      default=2,
      help='Search for a sibling changepoint in both directions '
      'from the current change point index.')
  parser.add_argument(
      '--changepoint_to_recent_run_window',
      type=int,
      default=7,
      help='Only allow creating alerts when regression '
      'happens if the run corresponding to the regressions is '
      'within changepoint_to_recent_run_window.')
  known_args, unknown_args = parser.parse_known_args()
  if unknown_args:
    _LOGGER.warning('Discarding unknown arguments : %s ' % unknown_args)
  config_file_path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')
  RunChangePointAnalysis(known_args).read_test_config(
      config_file_path=config_file_path)
