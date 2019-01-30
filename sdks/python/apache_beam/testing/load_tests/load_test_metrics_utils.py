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

Metrics are send to BigQuery in following format:
test_id | submit_timestamp | metric_type | value

The 'test_id' is common for all metrics for one run.
Currently it is possible to have following metrics types:
* runtime
* total_bytes_count


"""

from __future__ import absolute_import

import logging
import time
import uuid

import apache_beam as beam
from apache_beam.metrics import Metrics

try:
  from google.cloud import bigquery
  from google.cloud.bigquery.schema import SchemaField
  from google.cloud.exceptions import NotFound
except ImportError:
  bigquery = None
  SchemaField = None
  NotFound = None

RUNTIME_METRIC = 'runtime'
COUNTER_LABEL = 'total_bytes_count'

ID_LABEL = 'test_id'
SUBMIT_TIMESTAMP_LABEL = 'timestamp'
METRICS_TYPE_LABEL = 'metric'
VALUE_LABEL = 'value'

SCHEMA = [
    {'name': ID_LABEL,
     'field_type': 'STRING',
     'mode': 'REQUIRED'
    },
    {'name': SUBMIT_TIMESTAMP_LABEL,
     'field_type': 'TIMESTAMP',
     'mode': 'REQUIRED'
    },
    {'name': METRICS_TYPE_LABEL,
     'field_type': 'STRING',
     'mode': 'REQUIRED'
    },
    {'name': VALUE_LABEL,
     'field_type': 'FLOAT',
     'mode': 'REQUIRED'
    }
]


def get_element_by_schema(schema_name, insert_list):
  for element in insert_list:
    if element['label'] == schema_name:
      return element['value']


class BigQueryClient(object):
  def __init__(self, project_name, table, dataset):
    self._namespace = table

    self._bq_client = bigquery.Client(project=project_name)

    self._schema_names = self._get_schema_names()
    schema = self._prepare_schema()

    self._get_or_create_table(schema, dataset)

  def match_and_save(self, rows):
    outputs = self._bq_client.insert_rows(self._bq_table, rows)
    if len(outputs) > 0:
      for output in outputs:
        errors = output['errors']
        for err in errors:
          logging.error(err['message'])
          raise ValueError(
              'Unable save rows in BigQuery: {}'.format(err['message']))

  def _get_dataset(self, dataset_name):
    bq_dataset_ref = self._bq_client.dataset(dataset_name)
    try:
      bq_dataset = self._bq_client.get_dataset(bq_dataset_ref)
    except NotFound:
      raise ValueError(
          'Dataset {} does not exist in your project. '
          'You have to create table first.'
          .format(dataset_name))
    return bq_dataset

  def _get_or_create_table(self, bq_schemas, dataset):
    if self._namespace == '':
      raise ValueError('Namespace cannot be empty.')

    dataset = self._get_dataset(dataset)
    table_ref = dataset.table(self._namespace)

    try:
      self._bq_table = self._bq_client.get_table(table_ref)
    except NotFound:
      table = bigquery.Table(table_ref, schema=bq_schemas)
      self._bq_table = self._bq_client.create_table(table)

  def _prepare_schema(self):
    return [SchemaField(**row) for row in SCHEMA]

  def _get_schema_names(self):
    return [schema['name'] for schema in SCHEMA]


class MetricsMonitor(object):
  def __init__(self, project_name, table, dataset):
    if project_name is not None:
      self.bq = BigQueryClient(project_name, table, dataset)

  def send_metrics(self, result):
    metrics = result.metrics().query()

    insert_dicts = self._prepare_all_metrics(metrics)

    self.bq.match_and_save(insert_dicts)

  def _prepare_all_metrics(self, metrics):
    common_metric_rows = {SUBMIT_TIMESTAMP_LABEL: time.time(),
                          ID_LABEL: uuid.uuid4().hex
                         }
    insert_rows = []

    counters = metrics['counters']
    if len(counters) > 0:
      insert_rows += self._prepare_counter_metrics(counters, common_metric_rows)

    distributions = metrics['distributions']
    if len(distributions) > 0:
      insert_rows += self._prepare_runtime_metrics(distributions,
                                                   common_metric_rows)

    return insert_rows

  def _prepare_counter_metrics(self, counters, common_metric_rows):
    for counter in counters:
      logging.info("Counter:  %s", counter)
    counters_list = []
    for counter in counters:
      counter_commited = counter.committed
      counter_label = str(counter.key.metric.name)
      counters_list.extend([
          dict({VALUE_LABEL: counter_commited,
                METRICS_TYPE_LABEL: counter_label
               },
               **common_metric_rows)])

    return counters_list

  def _prepare_runtime_metrics(self, distributions, common_metric_rows):
    min_values = []
    max_values = []
    for dist in distributions:
      logging.info("Distribution: %s", dist)
      min_values.append(dist.committed.min)
      max_values.append(dist.committed.max)
    # finding real start
    min_value = min(min_values)
    # finding real end
    max_value = max(max_values)

    runtime_in_s = max_value - min_value
    logging.info("Runtime: %s", runtime_in_s)
    runtime_in_s = float(runtime_in_s)
    return [dict({VALUE_LABEL: runtime_in_s,
                  METRICS_TYPE_LABEL: RUNTIME_METRIC
                 }, **common_metric_rows)]


class MeasureTime(beam.DoFn):
  def __init__(self, namespace):
    self.namespace = namespace
    self.runtime = Metrics.distribution(self.namespace, RUNTIME_METRIC)

  def start_bundle(self):
    self.runtime.update(time.time())

  def finish_bundle(self):
    self.runtime.update(time.time())

  def process(self, element):
    yield element


def count_bytes(f):
  def repl(*args):
    namespace = args[2]
    counter = Metrics.counter(namespace, COUNTER_LABEL)
    element = args[1]
    _, value = element
    for i in range(len(value)):
      counter.inc(i)
    return f(*args)

  return repl
