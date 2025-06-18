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

"""The file defines global variables for performance analysis."""

_BQ_PROJECT_NAME = 'apache-beam-testing'
_BQ_DATASET = 'beam_perf_storage'

_UNIQUE_ID = 'test_id'
_ISSUE_CREATION_TIMESTAMP_LABEL = 'issue_timestamp'
_CHANGE_POINT_TIMESTAMP_LABEL = 'change_point_timestamp'
_CHANGE_POINT_LABEL = 'change_point'
_TEST_NAME = 'test_name'
_METRIC_NAME = 'metric_name'
_ISSUE_NUMBER = 'issue_number'
_ISSUE_URL = 'issue_url'
# number of results to display on the issue description
# from change point index in both directions.
_NUM_RESULTS_TO_DISPLAY_ON_ISSUE_DESCRIPTION = 10
_NUM_DATA_POINTS_TO_RUN_CHANGE_POINT_ANALYSIS = 100
# Variables used for finding duplicate change points.
_DEFAULT_MIN_RUNS_BETWEEN_CHANGE_POINTS = 3
_DEFAULT_NUM_RUMS_IN_CHANGE_POINT_WINDOW = 14
_DEFAULT_MEDIAN_ABS_DEVIATION_THRESHOLD = 2

_PERF_TEST_KEYS = {
    'test_description',
    'metrics_dataset',
    'metrics_table',
    'project',
    'metric_name'
}

_SCHEMA = [{
    'name': _UNIQUE_ID, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
           {
               'name': _ISSUE_CREATION_TIMESTAMP_LABEL,
               'field_type': 'TIMESTAMP',
               'mode': 'REQUIRED'
           },
           {
               'name': _CHANGE_POINT_TIMESTAMP_LABEL,
               'field_type': 'TIMESTAMP',
               'mode': 'REQUIRED'
           },
           {
               'name': _CHANGE_POINT_LABEL,
               'field_type': 'FLOAT64',
               'mode': 'REQUIRED'
           }, {
               'name': _METRIC_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
           }, {
               'name': _TEST_NAME, 'field_type': 'STRING', 'mode': 'REQUIRED'
           }, {
               'name': _ISSUE_NUMBER, 'field_type': 'INT64', 'mode': 'REQUIRED'
           }, {
               'name': _ISSUE_URL, 'field_type': 'STRING', 'mode': 'REQUIRED'
           }]

_ANOMALY_MARKER = ' <---- Anomaly'
_EDGE_SEGMENT_SIZE = 3
