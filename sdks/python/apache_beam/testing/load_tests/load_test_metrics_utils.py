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
Utility functions used for integrating Metrics API into load tests pipelines.
"""

from __future__ import absolute_import

import logging
import time

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

import apache_beam as beam
from apache_beam.metrics import Metrics

START_TIME_LABEL = 'runtime_start'
END_TIME_LABEL = 'runtime_end'
RUNTIME_LABEL = 'runtime'
SUBMIT_TIMESTAMP_LABEL = 'submit_timestamp'
LOAD_TEST_DATASET_NAME = 'python_load_tests'


def _get_schema_field(schema_field):
  return SchemaField(
      name=schema_field['name'],
      field_type=schema_field['type'],
      mode=schema_field['mode'])


class BigQueryMetricsCollector(object):
  def __init__(self, project_name, namespace, schema_map):
    self._namespace = namespace
    bq_client = bigquery.Client(project=project_name)
    bq_dataset = bq_client.dataset(LOAD_TEST_DATASET_NAME)
    if not bq_dataset.exists():
      raise ValueError(
          'Dataset {} does not exist in your project. '
          'You have to create table first.'
          .format(namespace))

    schemas = [{'name': SUBMIT_TIMESTAMP_LABEL,
                'type': 'TIMESTAMP',
                'mode': 'REQUIRED'}] + schema_map

    self._schema_names = [schema['name'] for schema in schemas]

    bq_schemas = [_get_schema_field(schema) for schema in schemas]

    self._bq_table = bq_dataset.table(namespace, bq_schemas)
    if not self._bq_table.exists():
      self._bq_table.create()

  def save_metrics(self, result):
    metrics = result.metrics().query()
    counters = metrics['counters']
    counters_list = []
    if len(counters) > 0:
      counters_list = self._prepare_counter_metrics(counters)

    distributions = metrics['distributions']
    dist_list = []
    if len(distributions) > 0:
      dist_list = self._prepare_runtime_metrics(distributions)

    timestamp = {'label': SUBMIT_TIMESTAMP_LABEL, 'value': time.time()}

    insert_list = [timestamp] + dist_list + counters_list
    rows_tuple = tuple(self._match_inserts_by_schema(insert_list))

    self.insert_metrics(rows_tuple)

  def _prepare_counter_metrics(self, counters):
    for counter in counters:
      logging.info("Counter:  %s", counter)
    counters_list = []
    for counter in counters:
      counter_commited = counter.committed
      counter_label = str(counter.key.metric.name)
      counters_list.append({'label': counter_label, 'value': counter_commited})

    return counters_list

  # prepares distributions of start and end metrics to show test runtime
  def _prepare_runtime_metrics(self, distributions):
    dist_pivot = {}
    for dist in distributions:
      logging.info("Distribution: %s", dist)
      dist_pivot.update(self._get_start_end_time(dist))

    runtime_in_s = dist_pivot[END_TIME_LABEL] - dist_pivot[START_TIME_LABEL]
    runtime_in_s = float(runtime_in_s)
    return [{'label': RUNTIME_LABEL, 'value': runtime_in_s}]

  def _match_inserts_by_schema(self, insert_list):
    for name in self._schema_names:
      yield self._get_element_by_schema(name, insert_list)

  def _get_element_by_schema(self, schema_name, list):
    for metric in list:
      if metric['label'] == schema_name:
        return metric['value']
    return None

  def insert_metrics(self, rows_tuple):
    job = self._bq_table.insert_data(rows=[rows_tuple])
    if len(job) > 0 and len(job[0]['errors']) > 0:
      for err in job[0]['errors']:
        raise ValueError(err['message'])

  def _get_start_end_time(self, distribution):
    if distribution.key.metric.name == START_TIME_LABEL:
      return {distribution.key.metric.name: distribution.committed.min}
    elif distribution.key.metric.name == END_TIME_LABEL:
      return {distribution.key.metric.name: distribution.committed.max}


class MeasureTime(beam.DoFn):
  def __init__(self, namespace):
    self.namespace = namespace
    self.runtime_start = Metrics.distribution(self.namespace, START_TIME_LABEL)
    self.runtime_end = Metrics.distribution(self.namespace, END_TIME_LABEL)

  def start_bundle(self):
    self.runtime_start.update(time.time())

  def finish_bundle(self):
    self.runtime_end.update(time.time())

  def process(self, element):
    yield element


class count_metrics(object):
  def __init__(self, namespace, counter_name):
    self.counter = Metrics.counter(namespace, counter_name)

  def __call__(self, fn):
    def decorated(*args):
      element = args[1]
      _, value = element
      for i in range(len(value)):
        self.counter.inc(i)
      return fn(*args)

    return decorated
