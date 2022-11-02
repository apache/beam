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
import yaml
import logging
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy as np
from apache_beam.testing.load_tests import load_test_metrics_utils
from apache_beam.testing.load_tests.load_test_metrics_utils import FetchMetrics
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive

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


class ChangePointAnalysis:
  def __init__(
      self,
      data: List[Dict],
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
      metric_values = [
          value[load_test_metrics_utils.VALUE_LABEL] for value in self.data
      ][::-1]
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
  def __init__(self, change_point_sibling_distance: int = 2):
    self.change_point_sibling_distance = change_point_sibling_distance

  def _find_sibling_change_point(self, changepoint_index,
                                 metric_values) -> Optional[int]:
    """
    Finds the sibling change point index. If not,
    returns the original changepoint index.

    Sibling changepoint is a neighbor of latest
    changepoint, within the distance of change_point_sibling_distance.
    For sibling changepoint, a GitHub issue is already created.
    """
    ########################################################
    # TODO: Implement logic to find sibling change point.
    ########################################################

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
    # for sibling_index in sibling_indexes_to_search:
    #   raise NotImplementedError
    return 0

  def run(self, file_path):
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
        print(metric_values)
        print(len(metric_values))
        return

      # always consider the latest change points
      change_points_idx.sort(reverse=True)
      change_point_index = change_points_idx[0]

      sibling_change_point_index = self._find_sibling_change_point(
          change_point_index, metric_values=metric_values)

      create_perf_alert = True
      if sibling_change_point_index:
        create_perf_alert = False

      if create_perf_alert:
        gh_issue = GitHubIssues()
        try:
          gh_issue.create_or_update_issue(
              title=TITLE_TEMPLATE,
              description=change_point_analyzer.get_failing_test_description(),
              labels=GH_ISSUE_LABELS)
        except:
          raise Exception(
              'Performance regression detected. Failed to file '
              'a Github issue.')
    elif config['source'] == 'influxDB':
      raise NotImplementedError
    else:
      raise NotImplementedError


if __name__ == '__main__':
  # TODO: Add argparse.
  logging.basicConfig(level=logging.INFO)
  file_path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'tests_config.yaml')
  RunChangePointAnalysis().run(file_path=file_path)
