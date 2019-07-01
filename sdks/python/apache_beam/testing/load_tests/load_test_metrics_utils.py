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


class MetricsReader(object):
  publishers = []

  def __init__(self, project_name=None, bq_table=None, bq_dataset=None):
    self.publishers.append(ConsoleMetricsPublisher())
    check = project_name and bq_table and bq_dataset
    if check:
      bq_publisher = BigQueryMetricsPublisher(
          project_name, bq_table, bq_dataset)
      self.publishers.append(bq_publisher)

  def publish_metrics(self, result):
    metrics = result.metrics().query()
    insert_dicts = self._prepare_all_metrics(metrics)
    if len(insert_dicts):
      for publisher in self.publishers:
        publisher.publish(insert_dicts)

  def _prepare_all_metrics(self, metrics):
    submit_timestamp = time.time()
    metric_id = uuid.uuid4().hex

    insert_rows = []

    for counter in metrics['counters']:
      counter_dict = CounterMetric(counter, submit_timestamp, metric_id)\
        .as_dict()
      insert_rows.append(counter_dict)

    dists = metrics['distributions']
    if len(dists) > 0:
      runtime = RuntimeMetric(dists, submit_timestamp, metric_id)\
        .as_dict()
      insert_rows.append(runtime)

    return insert_rows


class Metric(object):
  value = None
  label = None

  def __init__(self, submit_timestamp, metric_id):
    self.submit_timestamp = submit_timestamp
    self.metric_id = metric_id

  def as_dict(self):
    return {SUBMIT_TIMESTAMP_LABEL: self.submit_timestamp,
            ID_LABEL: self.metric_id,
            VALUE_LABEL: self.value,
            METRICS_TYPE_LABEL: self.label
           }


class CounterMetric(Metric):
  def __init__(self, counter_dict, submit_timestamp, metric_id):
    super(CounterMetric, self).__init__(submit_timestamp, metric_id)
    self.value = counter_dict.committed
    self.label = str(counter_dict.key.metric.name)


class RuntimeMetric(Metric):
  def __init__(self, runtime_list, submit_timestamp, metric_id):
    super(RuntimeMetric, self).__init__(submit_timestamp, metric_id)
    self.value = self._prepare_runtime_metrics(runtime_list)
    self.label = RUNTIME_METRIC

  def _prepare_runtime_metrics(self, distributions):
    min_values = []
    max_values = []
    for dist in distributions:
      min_values.append(dist.committed.min)
      max_values.append(dist.committed.max)
    # finding real start
    min_value = min(min_values)
    # finding real end
    max_value = max(max_values)

    runtime_in_s = float(max_value - min_value)
    return runtime_in_s


class ConsoleMetricsPublisher(object):
  def publish(self, results):
    if len(results) > 0:
      log = "Load test results for test: %s and timestamp: %s:" \
            % (results[0][ID_LABEL], results[0][SUBMIT_TIMESTAMP_LABEL])
      logging.info(log)
      for result in results:
        log = "Metric: %s Value: %s" \
              % (result[METRICS_TYPE_LABEL], result[VALUE_LABEL])
        logging.info(log)
    else:
      logging.info("No test results were collected.")


class BigQueryMetricsPublisher(object):
  def __init__(self, project_name, table, dataset):
    self.bq = BigQueryClient(project_name, table, dataset)

  def publish(self, results):
    outputs = self.bq.save(results)
    if len(outputs) > 0:
      for output in outputs:
        errors = output['errors']
        for err in errors:
          logging.error(err['message'])
          raise ValueError(
              'Unable save rows in BigQuery: {}'.format(err['message']))


class BigQueryClient(object):
  def __init__(self, project_name, table, dataset):
    self._namespace = table
    self._client = bigquery.Client(project=project_name)
    self._schema_names = self._get_schema_names()
    schema = self._prepare_schema()
    self._get_or_create_table(schema, dataset)

  def _get_schema_names(self):
    return [schema['name'] for schema in SCHEMA]

  def _prepare_schema(self):
    return [SchemaField(**row) for row in SCHEMA]

  def _get_or_create_table(self, bq_schemas, dataset):
    if self._namespace == '':
      raise ValueError('Namespace cannot be empty.')

    dataset = self._get_dataset(dataset)
    table_ref = dataset.table(self._namespace)

    try:
      self._bq_table = self._client.get_table(table_ref)
    except NotFound:
      table = bigquery.Table(table_ref, schema=bq_schemas)
      self._bq_table = self._client.create_table(table)

  def _get_dataset(self, dataset_name):
    bq_dataset_ref = self._client.dataset(dataset_name)
    try:
      bq_dataset = self._client.get_dataset(bq_dataset_ref)
    except NotFound:
      raise ValueError(
          'Dataset {} does not exist in your project. '
          'You have to create table first.'
          .format(dataset_name))
    return bq_dataset

  def save(self, results):
    return self._client.insert_rows(self._bq_table, results)


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
