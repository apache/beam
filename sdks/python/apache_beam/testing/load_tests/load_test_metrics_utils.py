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

# pytype: skip-file

from __future__ import absolute_import

import json
import logging
import time
import uuid
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

import requests
from requests.auth import HTTPBasicAuth

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

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

SCHEMA = [{
    'name': ID_LABEL, 'field_type': 'STRING', 'mode': 'REQUIRED'
},
          {
              'name': SUBMIT_TIMESTAMP_LABEL,
              'field_type': 'TIMESTAMP',
              'mode': 'REQUIRED'
          },
          {
              'name': METRICS_TYPE_LABEL,
              'field_type': 'STRING',
              'mode': 'REQUIRED'
          }, {
              'name': VALUE_LABEL, 'field_type': 'FLOAT', 'mode': 'REQUIRED'
          }]

_LOGGER = logging.getLogger(__name__)


def parse_step(step_name):
  """Replaces white spaces and removes 'Step:' label

  Args:
    step_name(str): step name passed in metric ParDo

  Returns:
    lower case step name without namespace and step label
  """
  return step_name.lower().replace(' ', '_').strip('step:_')


def split_metrics_by_namespace_and_name(metrics, namespace, name):
  """Splits metrics list namespace and name.

  Args:
    metrics: list of metrics from pipeline result
    namespace(str): filter metrics by namespace
    name(str): filter metrics by name

  Returns:
    two lists - one of metrics which are matching filters
    and second of not matching
  """
  matching_metrics = []
  not_matching_metrics = []
  for dist in metrics:
    if dist.key.metric.namespace == namespace\
        and dist.key.metric.name == name:
      matching_metrics.append(dist)
    else:
      not_matching_metrics.append(dist)
  return matching_metrics, not_matching_metrics


def get_generic_distributions(generic_dists, metric_id):
  """Creates flatten list of distributions per its value type.
  A generic distribution is the one which is not processed but saved in
  the most raw version.

  Args:
    generic_dists: list of distributions to be saved
    metric_id(uuid): id of the current test run

  Returns:
    list of dictionaries made from :class:`DistributionMetric`
  """
  return sum((
      get_all_distributions_by_type(dist, metric_id) for dist in generic_dists),
             [])


def get_all_distributions_by_type(dist, metric_id):
  """Creates new list of objects with type of each distribution
  metric value.

  Args:
    dist(object): DistributionMetric object to be parsed
    metric_id(uuid): id of the current test run
  Returns:
    list of :class:`DistributionMetric` objects
  """
  submit_timestamp = time.time()
  dist_types = ['count', 'max', 'min', 'sum']
  distribution_dicts = []
  for dist_type in dist_types:
    try:
      distribution_dicts.append(
          get_distribution_dict(dist_type, submit_timestamp, dist, metric_id))
    except ValueError:
      # Ignore metrics with 'None' values.
      continue
  return distribution_dicts


def get_distribution_dict(metric_type, submit_timestamp, dist, metric_id):
  """Function creates :class:`DistributionMetric`

  Args:
    metric_type(str): type of value from distribution metric which will
      be saved (ex. max, min, mean, sum)
    submit_timestamp: timestamp when metric is saved
    dist(object) distribution object from pipeline result
    metric_id(uuid): id of the current test run

  Returns:
    dictionary prepared for saving according to schema
  """
  return DistributionMetric(dist, submit_timestamp, metric_id,
                            metric_type).as_dict()


class MetricsReader(object):
  """
  A :class:`MetricsReader` retrieves metrics from pipeline result,
  prepares it for publishers and setup publishers.
  """
  publishers = []  # type: List[Any]

  def __init__(
      self,
      project_name=None,
      bq_table=None,
      bq_dataset=None,
      publish_to_bq=False,
      influxdb_options=None,  # type: Optional[InfluxDBMetricsPublisherOptions]
      namespace=None,
      filters=None):
    """Initializes :class:`MetricsReader` .

    Args:
      project_name (str): project with BigQuery where metrics will be saved
      bq_table (str): BigQuery table where metrics will be saved
      bq_dataset (str): BigQuery dataset where metrics will be saved
      namespace (str): Namespace of the metrics
      filters: MetricFilter to query only filtered metrics
    """
    self._namespace = namespace
    self.publishers.append(ConsoleMetricsPublisher())

    check = project_name and bq_table and bq_dataset and publish_to_bq
    if check:
      bq_publisher = BigQueryMetricsPublisher(
          project_name, bq_table, bq_dataset)
      self.publishers.append(bq_publisher)
    if influxdb_options and influxdb_options.validate():
      self.publishers.append(InfluxDBMetricsPublisher(influxdb_options))
    else:
      _LOGGER.info(
          'Missing InfluxDB options. Metrics will not be published to '
          'InfluxDB')
    self.filters = filters

  def publish_metrics(self, result):
    metrics = result.metrics().query(self.filters)

    # Metrics from pipeline result are stored in map with keys: 'gauges',
    # 'distributions' and 'counters'.
    # Under each key there is list of objects of each metric type. It is
    # required to prepare metrics for publishing purposes. Expected is to have
    # a list of dictionaries matching the schema.
    insert_dicts = self._prepare_all_metrics(metrics)
    if len(insert_dicts) > 0:
      for publisher in self.publishers:
        publisher.publish(insert_dicts)

  def publish_values(self, labeled_values):
    """The method to publish simple labeled values.

    Args:
      labeled_values (List[Tuple(str, int)]): list of (label, value)
    """
    metric_dicts = [
        Metric(time.time(), uuid.uuid4().hex, value, label=label).as_dict()
        for label,
        value in labeled_values
    ]

    for publisher in self.publishers:
      publisher.publish(metric_dicts)

  def _prepare_all_metrics(self, metrics):
    metric_id = uuid.uuid4().hex

    insert_rows = self._get_counters(metrics['counters'], metric_id)
    insert_rows += self._get_distributions(metrics['distributions'], metric_id)
    return insert_rows

  def _get_counters(self, counters, metric_id):
    submit_timestamp = time.time()
    return [
        CounterMetric(counter, submit_timestamp, metric_id).as_dict()
        for counter in counters
    ]

  def _get_distributions(self, distributions, metric_id):
    rows = []
    matching_namsespace, not_matching_namespace = \
      split_metrics_by_namespace_and_name(distributions, self._namespace,
                                          RUNTIME_METRIC)
    if len(matching_namsespace) > 0:
      runtime_metric = RuntimeMetric(matching_namsespace, metric_id)
      rows.append(runtime_metric.as_dict())
    if len(not_matching_namespace) > 0:
      rows += get_generic_distributions(not_matching_namespace, metric_id)
    return rows


class Metric(object):
  """Metric base class in ready-to-save format."""
  def __init__(
      self, submit_timestamp, metric_id, value, metric=None, label=None):
    """Initializes :class:`Metric`

    Args:
      metric (object): object of metric result
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
    return {
        SUBMIT_TIMESTAMP_LABEL: self.submit_timestamp,
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
    value = counter_metric.result
    super(CounterMetric,
          self).__init__(submit_timestamp, metric_id, value, counter_metric)


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
    value = getattr(dist_metric.result, metric_type)
    if value is None:
      msg = '%s: the result is expected to be an integer, ' \
            'not None.' % custom_label
      _LOGGER.debug(msg)
      raise ValueError(msg)
    super(DistributionMetric, self) \
      .__init__(submit_timestamp, metric_id, value, dist_metric, custom_label)


class RuntimeMetric(Metric):
  """The Distribution Metric in ready-to-publish format.

  Args:
    runtime_list: list of distributions metrics from MetricResult
      with runtime name
    metric_id(uuid): unique id to identify test run
  """
  def __init__(self, runtime_list, metric_id):
    value = self._prepare_runtime_metrics(runtime_list)
    submit_timestamp = time.time()
    # Label does not include step name, because it is one value calculated
    # out of many steps
    label = runtime_list[0].key.metric.namespace + \
            '_' + RUNTIME_METRIC
    super(RuntimeMetric,
          self).__init__(submit_timestamp, metric_id, value, None, label)

  def _prepare_runtime_metrics(self, distributions):
    min_values = []
    max_values = []
    for dist in distributions:
      min_values.append(dist.result.min)
      max_values.append(dist.result.max)
    # finding real start
    min_value = min(min_values)
    # finding real end
    max_value = max(max_values)

    runtime_in_s = float(max_value - min_value)
    return runtime_in_s


class ConsoleMetricsPublisher(object):
  """A :class:`ConsoleMetricsPublisher` publishes collected metrics
  to console output."""
  def publish(self, results):
    if len(results) > 0:
      log = "Load test results for test: %s and timestamp: %s:" \
            % (results[0][ID_LABEL], results[0][SUBMIT_TIMESTAMP_LABEL])
      _LOGGER.info(log)
      for result in results:
        log = "Metric: %s Value: %d" \
              % (result[METRICS_TYPE_LABEL], result[VALUE_LABEL])
        _LOGGER.info(log)
    else:
      _LOGGER.info("No test results were collected.")


class BigQueryMetricsPublisher(object):
  """A :class:`BigQueryMetricsPublisher` publishes collected metrics
  to BigQuery output."""
  def __init__(self, project_name, table, dataset):
    self.bq = BigQueryClient(project_name, table, dataset)

  def publish(self, results):
    outputs = self.bq.save(results)
    if len(outputs) > 0:
      for output in outputs:
        errors = output['errors']
        for err in errors:
          _LOGGER.error(err['message'])
          raise ValueError(
              'Unable save rows in BigQuery: {}'.format(err['message']))


class BigQueryClient(object):
  """A :class:`BigQueryClient` publishes collected metrics to
  BigQuery output."""
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
          'You have to create table first.'.format(dataset_name))
    return bq_dataset

  def save(self, results):
    return self._client.insert_rows(self._bq_table, results)


class InfluxDBMetricsPublisherOptions(object):
  def __init__(
      self,
      measurement,  # type: str
      db_name,  # type: str
      hostname,  # type: str
      user=None,  # type: Optional[str]
      password=None  # type: Optional[str]
    ):
    self.measurement = measurement
    self.db_name = db_name
    self.hostname = hostname
    self.user = user
    self.password = password

  def validate(self):
    # type: () -> bool
    return bool(self.measurement) and bool(self.db_name)

  def http_auth_enabled(self):
    # type: () -> bool
    return self.user is not None and self.password is not None


class InfluxDBMetricsPublisher(object):
  """Publishes collected metrics to InfluxDB database."""
  def __init__(
      self,
      options  # type: InfluxDBMetricsPublisherOptions
  ):
    self.options = options

  def publish(self, results):
    # type: (List[Mapping[str, Union[float, str, int]]]) -> None
    url = '{}/write'.format(self.options.hostname)
    payload = self._build_payload(results)
    query_str = {'db': self.options.db_name, 'precision': 's'}

    auth = HTTPBasicAuth(self.options.user, self.options.password) if \
      self.options.http_auth_enabled() else None

    try:
      response = requests.post(url, params=query_str, data=payload, auth=auth)
    except requests.exceptions.RequestException as e:
      _LOGGER.warning('Failed to publish metrics to InfluxDB: ' + str(e))
    else:
      if response.status_code != 204:
        content = json.loads(response.content)
        _LOGGER.warning(
            'Failed to publish metrics to InfluxDB. Received status code %s '
            'with an error message: %s' %
            (response.status_code, content['error']))

  def _build_payload(self, results):
    # type: (List[Mapping[str, Union[float, str, int]]]) -> str
    def build_kv(mapping, key):
      return '{}={}'.format(key, mapping[key])

    points = []
    for result in results:
      comma_separated = [
          self.options.measurement,
          build_kv(result, METRICS_TYPE_LABEL),
          build_kv(result, ID_LABEL),
      ]
      point = ','.join(comma_separated) + ' ' + build_kv(result, VALUE_LABEL) \
              + ' ' + str(int(result[SUBMIT_TIMESTAMP_LABEL]))
      points.append(point)
    return '\n'.join(points)


class MeasureTime(beam.DoFn):
  """A distribution metric prepared to be added to pipeline as ParDo
   to measure runtime."""
  def __init__(self, namespace):
    """Initializes :class:`MeasureTime`.

      namespace(str): namespace of  metric
    """
    self.namespace = namespace
    self.runtime = Metrics.distribution(self.namespace, RUNTIME_METRIC)

  def start_bundle(self):
    self.runtime.update(time.time())

  def finish_bundle(self):
    self.runtime.update(time.time())

  def process(self, element):
    yield element


class MeasureBytes(beam.DoFn):
  """Metric to measure how many bytes was observed in pipeline."""
  LABEL = 'total_bytes'

  def __init__(self, namespace, extractor=None):
    """Initializes :class:`MeasureBytes`.

    Args:
      namespace(str): metric namespace
      extractor: function to extract elements to be count
    """
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


class MeasureLatency(beam.DoFn):
  """A distribution metric which captures the latency based on the timestamps
  of the processed elements."""
  LABEL = 'latency'

  def __init__(self, namespace):
    """Initializes :class:`MeasureLatency`.

      namespace(str): namespace of  metric
    """
    self.namespace = namespace
    self.latency_ms = Metrics.distribution(self.namespace, self.LABEL)
    self.time_fn = time.time

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    self.latency_ms.update(
        int(self.time_fn() * 1000) - (timestamp.micros // 1000))
    yield element


class AssignTimestamps(beam.DoFn):
  """DoFn to assigned timestamps to elements."""
  def __init__(self):
    # Avoid having to use save_main_session
    self.time_fn = time.time
    self.timestamp_val_fn = TimestampedValue
    self.timestamp_fn = Timestamp

  def process(self, element):
    yield self.timestamp_val_fn(
        element, self.timestamp_fn(micros=int(self.time_fn() * 1000000)))
