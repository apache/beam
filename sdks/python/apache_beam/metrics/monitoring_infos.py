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

# cython: language_level=3
# cython: profile=True

# pytype: skip-file

from __future__ import absolute_import

import collections
import time
from functools import reduce
from typing import FrozenSet
from typing import Hashable
from typing import List

from apache_beam.coders import coder_impl
from apache_beam.coders import coders
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.cells import GaugeData
from apache_beam.metrics.cells import GaugeResult
from apache_beam.portability import common_urns
from apache_beam.portability.api import metrics_pb2

SAMPLED_BYTE_SIZE_URN = (
    common_urns.monitoring_info_specs.SAMPLED_BYTE_SIZE.spec.urn)
ELEMENT_COUNT_URN = common_urns.monitoring_info_specs.ELEMENT_COUNT.spec.urn
START_BUNDLE_MSECS_URN = (
    common_urns.monitoring_info_specs.START_BUNDLE_MSECS.spec.urn)
PROCESS_BUNDLE_MSECS_URN = (
    common_urns.monitoring_info_specs.PROCESS_BUNDLE_MSECS.spec.urn)
FINISH_BUNDLE_MSECS_URN = (
    common_urns.monitoring_info_specs.FINISH_BUNDLE_MSECS.spec.urn)
TOTAL_MSECS_URN = common_urns.monitoring_info_specs.TOTAL_MSECS.spec.urn
USER_COUNTER_URN = common_urns.monitoring_info_specs.USER_SUM_INT64.spec.urn
USER_DISTRIBUTION_URN = (
    common_urns.monitoring_info_specs.USER_DISTRIBUTION_INT64.spec.urn)
USER_GAUGE_URN = common_urns.monitoring_info_specs.USER_LATEST_INT64.spec.urn
USER_METRIC_URNS = set(
    [USER_COUNTER_URN, USER_DISTRIBUTION_URN, USER_GAUGE_URN])
WORK_REMAINING_URN = common_urns.monitoring_info_specs.WORK_REMAINING.spec.urn
WORK_COMPLETED_URN = common_urns.monitoring_info_specs.WORK_COMPLETED.spec.urn
DATA_CHANNEL_READ_INDEX = (
    common_urns.monitoring_info_specs.DATA_CHANNEL_READ_INDEX.spec.urn)

# TODO(ajamato): Implement the remaining types, i.e. Double types
# Extrema types, etc. See:
# https://s.apache.org/beam-fn-api-metrics
SUM_INT64_TYPE = common_urns.monitoring_info_types.SUM_INT64_TYPE.urn
DISTRIBUTION_INT64_TYPE = (
    common_urns.monitoring_info_types.DISTRIBUTION_INT64_TYPE.urn)
LATEST_INT64_TYPE = common_urns.monitoring_info_types.LATEST_INT64_TYPE.urn
PROGRESS_TYPE = common_urns.monitoring_info_types.PROGRESS_TYPE.urn

COUNTER_TYPES = set([SUM_INT64_TYPE])
DISTRIBUTION_TYPES = set([DISTRIBUTION_INT64_TYPE])
GAUGE_TYPES = set([LATEST_INT64_TYPE])

# TODO(migryz) extract values from beam_fn_api.proto::MonitoringInfoLabels
PCOLLECTION_LABEL = (
    common_urns.monitoring_info_labels.PCOLLECTION.label_props.name)
PTRANSFORM_LABEL = (
    common_urns.monitoring_info_labels.TRANSFORM.label_props.name)
NAMESPACE_LABEL = (
    common_urns.monitoring_info_labels.NAMESPACE.label_props.name)
NAME_LABEL = (common_urns.monitoring_info_labels.NAME.label_props.name)


def extract_counter_value(monitoring_info_proto):
  """Returns the counter value of the monitoring info."""
  if not is_counter(monitoring_info_proto):
    raise ValueError('Unsupported type %s' % monitoring_info_proto.type)

  # Only SUM_INT64_TYPE is currently supported.
  return coders.VarIntCoder().decode(monitoring_info_proto.payload)


def extract_gauge_value(monitoring_info_proto):
  """Returns a tuple containing (timestamp, value)"""
  if not is_gauge(monitoring_info_proto):
    raise ValueError('Unsupported type %s' % monitoring_info_proto.type)

  # Only LATEST_INT64_TYPE is currently supported.
  return _decode_gauge(coders.VarIntCoder(), monitoring_info_proto.payload)


def extract_distribution(monitoring_info_proto):
  """Returns a tuple of (count, sum, min, max).

  Args:
    proto: The monitoring info for the distribution.
  """
  if not is_distribution(monitoring_info_proto):
    raise ValueError('Unsupported type %s' % monitoring_info_proto.type)

  # Only DISTRIBUTION_INT64_TYPE is currently supported.
  return _decode_distribution(
      coders.VarIntCoder(), monitoring_info_proto.payload)


def create_labels(ptransform=None, namespace=None, name=None, pcollection=None):
  """Create the label dictionary based on the provided values.

  Args:
    ptransform: The ptransform id used as a label.
    pcollection: The pcollection id used as a label.
  """
  labels = {}
  if ptransform:
    labels[PTRANSFORM_LABEL] = ptransform
  if namespace:
    labels[NAMESPACE_LABEL] = namespace
  if name:
    labels[NAME_LABEL] = name
  if pcollection:
    labels[PCOLLECTION_LABEL] = pcollection
  return labels


def int64_user_counter(
    namespace, name, metric, ptransform=None) -> metrics_pb2.MonitoringInfo:
  """Return the counter monitoring info for the specifed URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The payload field to use in the monitoring info or an int value.
    ptransform: The ptransform id used as a label.
  """
  labels = create_labels(ptransform=ptransform, namespace=namespace, name=name)
  if isinstance(metric, int):
    metric = coders.VarIntCoder().encode(metric)
  return create_monitoring_info(
      USER_COUNTER_URN, SUM_INT64_TYPE, metric, labels)


def int64_counter(
    urn,
    metric,
    ptransform=None,
    pcollection=None) -> metrics_pb2.MonitoringInfo:
  """Return the counter monitoring info for the specifed URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The payload field to use in the monitoring info or an int value.
    ptransform: The ptransform id used as a label.
    pcollection: The pcollection id used as a label.
  """
  labels = create_labels(ptransform=ptransform, pcollection=pcollection)
  if isinstance(metric, int):
    metric = coders.VarIntCoder().encode(metric)
  return create_monitoring_info(urn, SUM_INT64_TYPE, metric, labels)


def int64_user_distribution(namespace, name, metric, ptransform=None):
  """Return the distribution monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The DistributionData for the metric.
    ptransform: The ptransform id used as a label.
  """
  labels = create_labels(ptransform=ptransform, namespace=namespace, name=name)
  payload = _encode_distribution(
      coders.VarIntCoder(), metric.count, metric.sum, metric.min, metric.max)
  return create_monitoring_info(
      USER_DISTRIBUTION_URN, DISTRIBUTION_INT64_TYPE, payload, labels)


def int64_distribution(
    urn,
    metric,
    ptransform=None,
    pcollection=None) -> metrics_pb2.MonitoringInfo:
  """Return a distribution monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The DistributionData for the metric.
    ptransform: The ptransform id used as a label.
    pcollection: The pcollection id used as a label.
  """
  labels = create_labels(ptransform=ptransform, pcollection=pcollection)
  payload = _encode_distribution(
      coders.VarIntCoder(), metric.count, metric.sum, metric.min, metric.max)
  return create_monitoring_info(urn, DISTRIBUTION_INT64_TYPE, payload, labels)


def int64_user_gauge(
    namespace, name, metric, ptransform=None) -> metrics_pb2.MonitoringInfo:
  """Return the gauge monitoring info for the URN, metric and labels.

  Args:
    namespace: User-defined namespace of counter.
    name: Name of counter.
    metric: The GaugeData containing the metrics.
    ptransform: The ptransform id used as a label.
  """
  labels = create_labels(ptransform=ptransform, namespace=namespace, name=name)
  if isinstance(metric, GaugeData):
    coder = coders.VarIntCoder()
    value = metric.value
    timestamp = metric.timestamp
  else:
    raise TypeError(
        'Expected GaugeData metric type but received %s with value %s' %
        (type(metric), metric))
  payload = _encode_gauge(coder, timestamp, value)
  return create_monitoring_info(
      USER_GAUGE_URN, LATEST_INT64_TYPE, payload, labels)


def int64_gauge(urn, metric, ptransform=None) -> metrics_pb2.MonitoringInfo:
  """Return the gauge monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: An int representing the value. The current time will be used for
            the timestamp.
    ptransform: The ptransform id used as a label.
  """
  labels = create_labels(ptransform=ptransform)
  if isinstance(metric, int):
    value = metric
    time_ms = int(time.time()) * 1000
  else:
    raise TypeError(
        'Expected int metric type but received %s with value %s' %
        (type(metric), metric))
  coder = coders.VarIntCoder()
  payload = coder.encode(time_ms) + coder.encode(value)
  return create_monitoring_info(urn, LATEST_INT64_TYPE, payload, labels)


def create_monitoring_info(
    urn, type_urn, payload, labels=None) -> metrics_pb2.MonitoringInfo:
  """Return the gauge monitoring info for the URN, type, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    type_urn: The URN of the type of the monitoring info/metric.
        i.e. beam:metrics:sum_int_64, beam:metrics:latest_int_64.
    payload: The payload field to use in the monitoring info.
    labels: The label dictionary to use in the MonitoringInfo.
  """
  return metrics_pb2.MonitoringInfo(
      urn=urn, type=type_urn, labels=labels or dict(), payload=payload)


def is_counter(monitoring_info_proto):
  """Returns true if the monitoring info is a coutner metric."""
  return monitoring_info_proto.type in COUNTER_TYPES


def is_gauge(monitoring_info_proto):
  """Returns true if the monitoring info is a gauge metric."""
  return monitoring_info_proto.type in GAUGE_TYPES


def is_distribution(monitoring_info_proto):
  """Returns true if the monitoring info is a distrbution metric."""
  return monitoring_info_proto.type in DISTRIBUTION_TYPES


def is_user_monitoring_info(monitoring_info_proto):
  """Returns true if the monitoring info is a user metric."""
  return monitoring_info_proto.urn in USER_METRIC_URNS


def extract_metric_result_map_value(monitoring_info_proto):
  """Returns the relevant GaugeResult, DistributionResult or int value.

  These are the proper format for use in the MetricResult.query() result.
  """
  # Returns a metric result (AKA the legacy format).
  # from the MonitoringInfo
  if is_counter(monitoring_info_proto):
    return extract_counter_value(monitoring_info_proto)
  if is_distribution(monitoring_info_proto):
    (count, sum, min, max) = extract_distribution(monitoring_info_proto)
    return DistributionResult(DistributionData(sum, count, min, max))
  if is_gauge(monitoring_info_proto):
    (timestamp, value) = extract_gauge_value(monitoring_info_proto)
    return GaugeResult(GaugeData(value, timestamp))


def parse_namespace_and_name(monitoring_info_proto):
  """Returns the (namespace, name) tuple of the URN in the monitoring info."""
  # Remove the URN prefix which indicates that it is a user counter.
  if is_user_monitoring_info(monitoring_info_proto):
    labels = monitoring_info_proto.labels
    return labels[NAMESPACE_LABEL], labels[NAME_LABEL]

  # If it is not a user counter, just use the first part of the URN, i.e. 'beam'
  split = monitoring_info_proto.urn.split(':', 1)
  return split[0], split[1]


def get_step_name(monitoring_info_proto):
  """Returns a step name for the given monitoring info or None if step name
  cannot be specified."""
  # Right now only metrics that have a PTRANSFORM are taken into account
  return monitoring_info_proto.labels.get(PTRANSFORM_LABEL)


def to_key(
    monitoring_info_proto: metrics_pb2.MonitoringInfo) -> FrozenSet[Hashable]:
  """Returns a key based on the URN and labels.

  This is useful in maps to prevent reporting the same MonitoringInfo twice.
  """
  key_items: List[Hashable] = list(monitoring_info_proto.labels.items())
  key_items.append(monitoring_info_proto.urn)
  return frozenset(key_items)


def sum_payload_combiner(payload_a, payload_b):
  coder = coders.VarIntCoder()
  return coder.encode(coder.decode(payload_a) + coder.decode(payload_b))


def distribution_payload_combiner(payload_a, payload_b):
  coder = coders.VarIntCoder()
  (count_a, sum_a, min_a, max_a) = _decode_distribution(coder, payload_a)
  (count_b, sum_b, min_b, max_b) = _decode_distribution(coder, payload_b)
  return _encode_distribution(
      coder,
      count_a + count_b,
      sum_a + sum_b,
      min(min_a, min_b),
      max(max_a, max_b))


_KNOWN_COMBINERS = {
    SUM_INT64_TYPE: sum_payload_combiner,
    DISTRIBUTION_INT64_TYPE: distribution_payload_combiner,
}


def consolidate(metrics, key=to_key):
  grouped = collections.defaultdict(list)
  for metric in metrics:
    grouped[key(metric)].append(metric)
  for values in grouped.values():
    if len(values) == 1:
      yield values[0]
    else:
      combiner = _KNOWN_COMBINERS.get(values[0].type)
      if combiner:

        def merge(a, b):
          # pylint: disable=cell-var-from-loop
          return metrics_pb2.MonitoringInfo(
              urn=a.urn,
              type=a.type,
              labels=dict((label, value) for label,
                          value in a.labels.items()
                          if b.labels.get(label) == value),
              payload=combiner(a.payload, b.payload))

        yield reduce(merge, values)
      else:
        for value in values:
          yield value


def _decode_gauge(coder, payload):
  """Returns a tuple of (timestamp, value)."""
  timestamp_coder = coders.VarIntCoder().get_impl()
  stream = coder_impl.create_InputStream(payload)
  time_ms = timestamp_coder.decode_from_stream(stream, True)
  return (time_ms / 1000.0, coder.get_impl().decode_from_stream(stream, True))


def _encode_gauge(coder, timestamp, value):
  timestamp_coder = coders.VarIntCoder().get_impl()
  stream = coder_impl.create_OutputStream()
  timestamp_coder.encode_to_stream(int(timestamp * 1000), stream, True)
  coder.get_impl().encode_to_stream(value, stream, True)
  return stream.get()


def _decode_distribution(value_coder, payload):
  """Returns a tuple of (count, sum, min, max)."""
  count_coder = coders.VarIntCoder().get_impl()
  value_coder = value_coder.get_impl()
  stream = coder_impl.create_InputStream(payload)
  return (
      count_coder.decode_from_stream(stream, True),
      value_coder.decode_from_stream(stream, True),
      value_coder.decode_from_stream(stream, True),
      value_coder.decode_from_stream(stream, True))


def _encode_distribution(value_coder, count, sum, min, max):
  count_coder = coders.VarIntCoder().get_impl()
  value_coder = value_coder.get_impl()
  stream = coder_impl.create_OutputStream()
  count_coder.encode_to_stream(count, stream, True)
  value_coder.encode_to_stream(sum, stream, True)
  value_coder.encode_to_stream(min, stream, True)
  value_coder.encode_to_stream(max, stream, True)
  return stream.get()
