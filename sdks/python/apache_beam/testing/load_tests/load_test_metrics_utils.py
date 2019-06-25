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
from itertools import groupby

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

def group_by_metrics_name(sorted_metrics):
  """ Function groups metrics from pipeline result by metric name.

  Args:
    sorted_metrics(list): list of all metrics from pipeline result sorted by name

  Returns:
    map of key (name) and iterator of metrics with this name
  """
  return groupby(sorted_metrics, lambda x: x.key.metric.name)


def parse_step(step_name):
  """Funtion replaces white spaces and removes 'Step:' label

  Args:
    step_name(str): step name passed in metric ParDo

  Returns:
    lower case step name without namespace and step label
  """
  return step_name.lower().replace(' ', '_').strip('step:_')


class MetricsReader(object):
  """
  A :class:`MetricReader` retrieves metrics from pipeline result,
  prepares it for publishers and setup publishers.
  """
  publishers = []

  def __init__(self, project_name=None, bq_table=None, bq_dataset=None,
               filters=None):
    """Initializes :class:`MetricsReader` .

    Args:
      project_name (str): project with BigQuery where metrics will be saved
      bq_table (str): BigQuery table where metrics will be saved
      bq_dataset (str): BigQuery dataset where metrics will be saved
    """
    self.publishers.append(ConsoleMetricsPublisher())
    self._namespace = bq_table
    check = project_name and bq_table and bq_dataset
    if check:
      bq_publisher = BigQueryMetricsPublisher(
          project_name, bq_table, bq_dataset)
      self.publishers.append(bq_publisher)
    self.filters = filters

  def publish_metrics(self, result):
    metrics = result.metrics().query(self.filters)
    insert_dicts = self._prepare_all_metrics(metrics)
    if len(insert_dicts):
      for publisher in self.publishers:
        publisher.publish(insert_dicts)

  def _prepare_all_metrics(self, metrics):
    metric_id = uuid.uuid4().hex

    insert_rows = self._get_counters(metrics['counters'], metric_id)
    insert_rows += self._get_distributions(metrics['distributions'], metric_id)
    return insert_rows

  def _get_counters(self, counters, metric_id):
    submit_timestamp = time.time()
    return [CounterMetric(counter, submit_timestamp, metric_id).as_dict()
      for counter
      in counters]

  def _get_distributions(self, distributions, metric_id):
    sorted_dists = sorted(distributions, key=lambda x: x.key.metric.name)
    rows = []
    for label, dists in group_by_metrics_name(sorted_dists):
      dists = list(dists)
      if label == RUNTIME_METRIC:
        metric = RuntimeMetric(dists, metric_id)
        rows.append(metric.as_dict())
      else:
        rows += self._get_generic_distributions(dists, metric_id)
    return rows

  def _get_generic_distributions(self, generic_dists, metric_id):
    return sum((self._get_all_distributions_by_type(dist, metric_id)
               for dist
               in generic_dists),
               [])

  def _get_all_distributions_by_type(self, dist, metric_id):
    submit_timestamp = time.time()
    dist_types = ['mean', 'max', 'min', 'sum']
    return [self._get_distribution_dict(dist_type, submit_timestamp, dist, metric_id)
            for dist_type
            in dist_types]

  def _get_distribution_dict(self, type, submit_timestamp, dist, metric_id):
    return DistributionMetric(dist, submit_timestamp, metric_id, type).as_dict()

class Metric(object):
  """Metric base class in ready-to-save format."""

  def __init__(self, metric, submit_timestamp, metric_id, value, label = None):
    """Initializes :class:`Metric`

    Args:
      metric (object): metric object from MetricResult
      submit_timestamp (float): date-time of saving metric to database
      metric_id (uuid): unique id to identify test run
      value: value of metric
      label: custom metric name to be saved in database
    """
    self.submit_timestamp = submit_timestamp
    self.metric_id = metric_id
    self.label = label or metric.key.metric.namespace + \
            '_' + parse_step(metric.key.step) + \
            '_' + metric.key.metric.name
    self.value = value

  def as_dict(self):
    return {SUBMIT_TIMESTAMP_LABEL: self.submit_timestamp,
            ID_LABEL: self.metric_id,
            VALUE_LABEL: self.value,
            METRICS_TYPE_LABEL: self.label
           }


class CounterMetric(Metric):
  """The Counter Metric in ready-to-publish format.

  Args:
    counter_metric (object): counter metric object from MetricResult
    submit_timestamp (float): date-time of saving metric to database
    metric_id (uuid): unique id to identify test run
  """
  def __init__(self, counter_metric, submit_timestamp, metric_id):
    value = counter_metric.committed
    super(CounterMetric, self).__init__(counter_metric, submit_timestamp, metric_id, value)


class DistributionMetric(Metric):
  """The Distribution Metric in ready-to-publish format.

  Args:
    dist_metric (object): distribution metric object from MetricResult
    submit_timestamp (float): date-time of saving metric to database
    metric_id (uuid): unique id to identify test run
  """
  def __init__(self, dist_metric, submit_timestamp, metric_id, metric_type):
    custom_label = dist_metric.key.metric.namespace + \
                   '_' + parse_step(dist_metric.key.step) + \
                   '_' + metric_type + \
                   '_' + dist_metric.key.metric.name
    value = getattr(dist_metric.committed, metric_type)
    super(DistributionMetric, self).__init__(dist_metric, submit_timestamp, metric_id, value, custom_label)


class RuntimeMetric(Metric):
  """The Distribution Metric in ready-to-publish format.

  Args:
    runtime_list (list): list of distributions metrics from MetricResult with runtime name
    metric_id (uuid): unique id to identify test run
  """
  def __init__(self, runtime_list, metric_id):
    value = self._prepare_runtime_metrics(runtime_list)
    submit_timestamp = time.time()
    # Label does not include step name, because it is one value calculated out of many steps
    label = runtime_list[0].key.metric.namespace + \
            '_' + RUNTIME_METRIC
    super(RuntimeMetric, self).__init__(runtime_list, submit_timestamp, metric_id, value, label)

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
  """A :class:`ConsoleMetricsPublisher` publishes collected metrics to console output."""
  def publish(self, results):
    if len(results) > 0:
      log = "Load test results for test: %s and timestamp: %s:" \
            % (results[0][ID_LABEL], results[0][SUBMIT_TIMESTAMP_LABEL])
      logging.info(log)
      for result in results:
        log = "Metric: %s Value: %d" \
              % (result[METRICS_TYPE_LABEL], result[VALUE_LABEL])
        logging.info(log)
    else:
      logging.info("No test results were collected.")


class BigQueryMetricsPublisher(object):
  """A :class:`BigQueryMetricsPublisher` publishes collected metrics to BigQuery output."""
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
  """A :class:`BigQueryClient` publishes collected metrics to BigQuery output."""
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
  """A distribution metric prepared to be added to pipeline as ParDo to measure runtime."""
  def __init__(self, namespace):
    self.namespace = namespace
    self.runtime = Metrics.distribution(self.namespace, RUNTIME_METRIC)

  def start_bundle(self):
    self.runtime.update(time.time())

  def finish_bundle(self):
    self.runtime.update(time.time())

  def process(self, element):
    yield element


class MeasureBytes(beam.DoFn):
  LABEL = 'total_bytes'

  def __init__(self, namespace, extractor=None):
    self.namespace = namespace
    self.counter = Metrics.counter(self.namespace, self.LABEL)
    self.extractor = extractor if extractor else lambda x: (yield x)

  def process(self, element, *args):
    for value in self.extractor(element, *args):
      self.counter.inc(len(value))
    yield element


class CountMessages(beam.DoFn):
  LABEL = 'total_messages'

  def __init__(self, namespace):
    self.namespace = namespace
    self.counter = Metrics.counter(self.namespace, self.LABEL)

  def process(self, element):
    self.counter.inc(1)
    yield element
