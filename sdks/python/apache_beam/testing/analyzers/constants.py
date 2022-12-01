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

BQ_PROJECT_NAME = 'apache-beam-testing'
BQ_DATASET = 'beam_perf_storage'

UNIQUE_ID = 'test_id'
ISSUE_CREATION_TIMESTAMP_LABEL = 'issue_timestamp'
CHANGE_POINT_TIMESTAMP_LABEL = 'change_point_timestamp'
CHANGE_POINT_LABEL = 'change_point'
TEST_NAME = 'test_name'
METRIC_NAME = 'metric_name'
ISSUE_NUMBER = 'issue_number'
ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions.
NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10
NUM_DATA_POINTS_TO_RUN_CHANGE_POINT_ANALYSIS = 100
# Variables used for finding duplicate change points.
DEFAULT_MIN_RUNS_BETWEEN_CHANGE_POINTS = 5
DEFAULT_NUM_RUMS_IN_CHANGE_POINT_WINDOW = 20

PERF_ALERT_LABEL = 'perf-alert'

PERF_TEST_KEYS = {
    'test_name', 'metrics_dataset', 'metrics_table', 'project', 'metric_name'
}

SCHEMA = [{
    'name': UNIQUE_ID, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': ISSUE_CREATION_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': CHANGE_POINT_TIMESTAMP_LABEL,
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
  Performance Regression or Improvement: {}:{}
"""
# TODO: Add Median value before and Median value after.
_METRIC_DESCRIPTION = """
  Affected metric: `{}`
"""
_METRIC_INFO = "timestamp: {}, metric_value: `{}`"
ISSUE_LABELS = ['perf-alert']
