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
import os
import time
import uuid

import pandas
import requests
import sys
import yaml
import logging
from typing import List
from typing import Optional
from typing import Union

import numpy as np
from apache_beam.testing.load_tests import load_test_metrics_utils
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsPublisher
from apache_beam.testing.load_tests.load_test_metrics_utils import FetchMetrics
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive

BQ_PROJECT_NAME = 'apache-beam-testing'
BQ_DATASET = 'beam-perf-storage'

ID_LABEL = 'test_id'
SUBMIT_TIMESTAMP_LABEL = 'timestamp'
CHANGE_POINT_LABEL = 'change_point'
TEST_NAME = 'test_name'
METRIC_NAME = 'metric_name'
ISSUE_URL = 'issue_url'

_LOGGER = logging.getLogger(__name__)

SCHEMA = [{
    'name': ID_LABEL, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': SUBMIT_TIMESTAMP_LABEL,
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
              'name': TEST_NAME, 'field_type': 'FLOAT', 'mode': 'REQUIRED'
          }, {
              'name': ISSUE_URL, 'field_type': 'STRING', 'mode': 'REQUIRED'
          }]

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
GH_ISSUE_LABELS = ['testing', 'P2']


class Metric:
  def __init__(
      self,
      submit_timestamp,
      test_name,
      metric_name,
      issue_url,
      test_id,
      change_point):
    """
    Args:
      submit_timestamp (float): date-time of saving metric to database.
      test_name: Name of the perf test.
      metric_name: metric_name
      issue_url: URL to the GitHub issue.
      test_id (uuid): unique id to identify test run.
      change_point (object): Change point observed.
    """
    self.submit_timestamp = submit_timestamp
    self.test_name = test_name
    self.metric_name = metric_name
    self.issue_url = issue_url
    self.test_id = test_id
    self.change_point = change_point

  def as_dict(self):
    return {
        SUBMIT_TIMESTAMP_LABEL: self.submit_timestamp,
        TEST_NAME: self.test_name,
        METRIC_NAME: self.metric_name,
        ISSUE_URL: self.issue_url,
        ID_LABEL: self.test_id,
        CHANGE_POINT_LABEL: self.change_point
    }


class GitHubIssues:
  def __init__(self, owner='AnandInguva', repo='beam'):
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
      response = requests.post(
          url=url, data=json.dumps(data), headers=self.headers).json()
      return response

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


class ChangePointAnalysis:
  def __init__(
      self,
      data: pandas.DataFrame,
      metric_name: str,
  ):
    self.data = data
    self.metric_name = metric_name

  @staticmethod
  def edivisive_means(
      series: Union[List[float], List[List[float]], np.ndarray],
      pvalue: float = 0.05,
      permutations: int = 100):
    return e_divisive(series, pvalue, permutations)

  def find_change_points(self, analysis='edivisive') -> List:
    if analysis == 'edivisive':
      metric_values = self.data[load_test_metrics_utils.VALUE_LABEL].tolist()
      change_points = ChangePointAnalysis.edivisive_means(metric_values)
      return change_points
    else:
      raise NotImplementedError

  def get_failing_test_description(self):
    metric_description = _METRIC_DESCRIPTION.format(self.metric_name) + 2 * '\n'
    for data in self.data:
      metric_description += _METRIC_INFO.format(
          data[load_test_metrics_utils.SUBMIT_TIMESTAMP_LABEL],
          data[load_test_metrics_utils.METRICS_TYPE_LABEL],
          data[load_test_metrics_utils.VALUE_LABEL]) + '\n'
    return metric_description


class RunChangePointAnalysis:
  def __init__(self, args):
    self.change_point_sibling_distance = args.change_point_sibling_distance
    self.changepoint_to_recent_run_window = (
        args.changepoint_to_recent_run_window)
    self.test_name = sys.argv[0]

  def has_latest_sibling_change_point(
      self, metric_values, metric_name, sibling_indexes_to_search) -> bool:
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
        timestamp=SUBMIT_TIMESTAMP_LABEL,
        table=self.test_name)
    df = FetchMetrics.fetch_from_bq(query_template=query_template)
    latest_change_point = df[CHANGE_POINT_LABEL]

    for sibling_index in sibling_indexes_to_search:
      # Verify the logic.
      if metric_values[sibling_index] == latest_change_point:
        return True

    return False

  def is_changepoint_in_valid_window(self, change_point_index: int) -> bool:
    # If the change point is more than N runs behind the most recent run
    # then don't raise an alert for it.
    if self.changepoint_to_recent_run_window > change_point_index:
      return True
    return False

  def has_sibling_change_point(
      self, changepoint_index, metric_values, metric_name) -> Optional[int]:
    """
    Finds the sibling change point index. If not,
    returns the original changepoint index.

    Sibling changepoint is a neighbor of latest
    changepoint, within the distance of change_point_sibling_distance.
    For sibling changepoint, a GitHub issue is already created.
    """

    # Search backward from the current changepoint
    sibling_indexes_to_search = []
    for i in range(changepoint_index - 1, -1, -1):
      if changepoint_index - i >= self.change_point_sibling_distance:
        sibling_indexes_to_search.append(i)
    # Search forward from the current changepoint
    for i in range(changepoint_index + 1, len(metric_values)):
      if i - changepoint_index <= self.change_point_sibling_distance:
        sibling_indexes_to_search.append(i)
    # Look for change points within change_point_sibling_distance.
    # Return the first change point found.
    alert_new_issue = self.has_latest_sibling_change_point(
        metric_values=metric_values,
        metric_name=metric_name,
        sibling_indexes_to_search=sibling_indexes_to_search)

    if not alert_new_issue:
      return False
    return True

  def run(self, file_path):
    test_name = sys.argv[0]
    with open(file_path, 'r') as stream:
      config = yaml.safe_load(stream)
    metric_name = config['metric_name']
    if config['source'] == 'big_query':
      metric_values = FetchMetrics.fetch_from_bq(
          project_name=config['project'],
          dataset=config['metrics_dataset'],
          table=config['metrics_table'],
          metric_name=metric_name)

      change_point_analyzer = ChangePointAnalysis(
          metric_values, metric_name=metric_name)
      change_points_idx = change_point_analyzer.find_change_points()

      if not change_points_idx:
        return

      # always consider the latest change points
      change_points_idx.sort(reverse=True)
      change_point_index = change_points_idx[0]

      if not self.changepoint_to_recent_run_window(change_point_index):
        return

      create_perf_alert = self.has_sibling_change_point(
          change_point_index,
          metric_values=metric_values,
          metric_name=metric_name)

      if create_perf_alert:
        gh_issue = GitHubIssues()
        try:
          response = gh_issue.create_or_update_issue(
              title=TITLE_TEMPLATE,
              description=change_point_analyzer.get_failing_test_description(),
              labels=GH_ISSUE_LABELS)

          bq_metrics_publisher = BigQueryMetricsPublisher(
              project_name=BQ_PROJECT_NAME, dataset=BQ_DATASET, table=test_name)
          metric_dict = Metric(
              submit_timestamp=time.time(),
              test_name=test_name,
              metric_name=metric_name,
              test_id=uuid.uuid4().hex,
              change_point=metric_values[
                  load_test_metrics_utils.METRICS_TYPE_LABEL]
              [change_point_index],
              issue_url=response['items']['number'])
          bq_metrics_publisher.publish(metric_dict.as_dict())

        except:
          raise Exception(
              'Performance regression detected. Failed to file '
              'a Github issue.')
    elif config['source'] == 'influxDB':
      raise NotImplementedError
    else:
      raise NotImplementedError


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
      'changepoint_to_recent_run_window',
      type=int,
      default=7,
      help='Only allow creating alerts when regression '
      'happens if the run corresponding to the regressions is '
      'within changepoint_to_recent_run_window.')
  known_args, unknown_args = parser.parse_known_args()

  _LOGGER.warning('Discarding unknown arguments : %s ' % unknown_args)
  file_path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')
  RunChangePointAnalysis(known_args).run(file_path=file_path, )
