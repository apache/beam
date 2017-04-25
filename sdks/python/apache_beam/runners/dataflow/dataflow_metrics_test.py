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
"""
Tests corresponding to the DataflowRunner implementation of MetricsResult,
the DataflowMetrics class.
"""
import types
import unittest

import mock

from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.dataflow import dataflow_metrics


class DictToObject(object):
  """Translate from a dict(list()) structure to an object structure"""
  def __init__(self, data):
    for name, value in data.iteritems():
      setattr(self, name, self._wrap(value))

  def _wrap(self, value):
    if isinstance(value, (tuple, list, set, frozenset)):
      return type(value)([self._wrap(v) for v in value])
    return DictToObject(value) if isinstance(value, dict) else value


class TestDataflowMetrics(unittest.TestCase):

  STRUCTURED_COUNTER_LIST = {"metrics": [
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "namespace",
                     "value": "__main__.WordExtractingDoFn"},
                    {"key": "step",
                     "value": "s2"},
                    {"key": "tentative",
                     "value": "true"}]
                },
                "name": "word_lengths",
                "origin": "user"
               },
       "scalar": {"integer_value": 109475},
       "updateTime": "2017-03-22T18:47:06.402Z"
      },
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "namespace",
                     "value": "__main__.WordExtractingDoFn"},
                    {"key": "step",
                     "value": "s2"}]
                },
                "name": "word_lengths",
                "origin": "user"
               },
       "scalar": {"integer_value": 109475},
       "updateTime": "2017-03-22T18:47:06.402Z"
      },
  ]}

  BASIC_COUNTER_LIST = {"metrics": [
      {"name": {"context":
                {"additionalProperties":[
                    {"key": "original_name",
                     "value": "user-split-split/__main__.WordExtractingDoFn/"
                              "empty_lines_TentativeAggregateValue"},
                    {"key": "step", "value": "split"}]},
                "name": "split/__main__.WordExtractingDoFn/empty_lines",
                "origin": "user"},
       "scalar": {"integer_value": 1080},
       "updateTime": "2017-02-23T01:13:36.659Z"},
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "original_name",
                     "value": "user-split-split/__main__.WordExtractingDoFn/"
                              "empty_lines_TentativeAggregateValue"},
                    {"key": "step", "value": "split"},
                    {"key": "tentative", "value": "true"}]},
                "name": "split/__main__.WordExtractingDoFn/empty_lines",
                "origin": "user"},
       "scalar": {"integer_value": 1080},
       "updateTime": "2017-02-23T01:13:36.659Z"},
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "original_name",
                     "value": "user-split-split/__main__.WordExtractingDoFn/"
                              "words_TentativeAggregateValue"},
                    {"key": "step", "value": "split"}]},
                "name": "longstepname/split/__main__.WordExtractingDoFn/words",
                "origin": "user"},
       "scalar": {"integer_value": 26181},
       "updateTime": "2017-02-23T01:13:36.659Z"},
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "original_name",
                     "value": "user-split-longstepname/split/"
                              "__main__.WordExtractingDoFn/"
                              "words_TentativeAggregateValue"},
                    {"key": "step", "value": "split"},
                    {"key": "tentative", "value": "true"}]},
                "name": "longstepname/split/__main__.WordExtractingDoFn/words",
                "origin": "user"},
       "scalar": {"integer_value": 26185},
       "updateTime": "2017-02-23T01:13:36.659Z"},
      {"name": {"context":
                {"additionalProperties": [
                    {"key": "original_name",
                     "value": "user-split-split/__main__.WordExtractingDoFn/"
                              "secretdistribution(DIST)"},
                    {"key": "step", "value": "split"},
                    {"key": "tentative", "value": "true"}]},
                "name":
                "split/__main__.WordExtractingDoFn/secretdistribution(DIST)",
                "origin": "user"},
       "scalar": {"integer_value": 15},
       "updateTime": "2017-02-23T01:13:36.659Z"}
  ]}

  def setup_mock_client_result(self, counter_list=None):
    if counter_list is None:
      counter_list = self.BASIC_COUNTER_LIST

    mock_client = mock.Mock()
    mock_query_result = DictToObject(counter_list)
    mock_client.get_job_metrics.return_value = mock_query_result
    mock_job_result = mock.Mock()
    mock_job_result.job_id.return_value = 1
    mock_job_result._is_in_terminal_state.return_value = False
    return mock_client, mock_job_result

  def test_cache_functions(self):
    mock_client, mock_job_result = self.setup_mock_client_result()
    dm = dataflow_metrics.DataflowMetrics(mock_client, mock_job_result)

    # At first creation, we should always query dataflow.
    self.assertTrue(dm._cached_metrics is None)

    # Right after querying, we still query again.
    dm.query()
    self.assertTrue(dm._cached_metrics is None)

    # The job has ended. The query should not run again after this.
    mock_job_result._is_in_terminal_state.return_value = True
    dm.query()
    self.assertTrue(dm._cached_metrics)

  def test_query_structured_counters(self):
    mock_client, mock_job_result = self.setup_mock_client_result(
        self.STRUCTURED_COUNTER_LIST)
    dm = dataflow_metrics.DataflowMetrics(mock_client, mock_job_result)
    dm._translate_step_name = types.MethodType(lambda self, x: 'split', dm)
    query_result = dm.query()
    expected_counters = [
        MetricResult(
            MetricKey('split',
                      MetricName('__main__.WordExtractingDoFn',
                                 'word_lengths')),
            109475, 109475),
        ]
    self.assertEqual(query_result['counters'], expected_counters)

  def test_query_counters(self):
    mock_client, mock_job_result = self.setup_mock_client_result()
    dm = dataflow_metrics.DataflowMetrics(mock_client, mock_job_result)
    query_result = dm.query()
    expected_counters = [
        MetricResult(
            MetricKey('split',
                      MetricName('__main__.WordExtractingDoFn', 'empty_lines')),
            1080, 1080),
        MetricResult(
            MetricKey('longstepname/split',
                      MetricName('__main__.WordExtractingDoFn', 'words')),
            26181, 26185),
        ]
    self.assertEqual(sorted(query_result['counters'],
                            key=lambda x: x.key.metric.name),
                     sorted(expected_counters,
                            key=lambda x: x.key.metric.name))

if __name__ == '__main__':
  unittest.main()
