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

from google.protobuf import timestamp_pb2

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
USER_COUNTER_URN = (common_urns.monitoring_info_specs.USER_COUNTER.spec.urn)
USER_DISTRIBUTION_COUNTER_URN = (
    common_urns.monitoring_info_specs.USER_DISTRIBUTION_COUNTER.spec.urn)

# TODO(ajamato): Implement the remaining types, i.e. Double types
# Extrema types, etc. See:
# https://s.apache.org/beam-fn-api-metrics
SUM_INT64_TYPE = common_urns.monitoring_info_types.SUM_INT64_TYPE.urn
DISTRIBUTION_INT64_TYPE = (
    common_urns.monitoring_info_types.DISTRIBUTION_INT64_TYPE.urn)
LATEST_INT64_TYPE = common_urns.monitoring_info_types.LATEST_INT64_TYPE.urn

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
TAG_LABEL = "TAG"


def to_timestamp_proto(timestamp_secs):
  """Converts seconds since epoch to a google.protobuf.Timestamp.

  Args:
    timestamp_secs: The timestamp in seconds since epoch.
  """
  seconds = int(timestamp_secs)
  nanos = int((timestamp_secs - seconds) * 10**9)
  return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)


def to_timestamp_secs(timestamp_proto):
  """Converts a google.protobuf.Timestamp to seconds since epoch.

  Args:
    timestamp_proto: The google.protobuf.Timestamp.
  """
  return timestamp_proto.seconds + timestamp_proto.nanos * 10**-9


def extract_counter_value(monitoring_info_proto):
  """Returns the int coutner value of the monitoring info."""
  if is_counter(monitoring_info_proto) or is_gauge(monitoring_info_proto):
    return monitoring_info_proto.metric.counter_data.int64_value
  return None


def extract_distribution(monitoring_info_proto):
  """Returns the relevant DistributionInt64 or DistributionDouble.

  Args:
    monitoring_info_proto: The monitoring infor for the distribution.
  """
  if is_distribution(monitoring_info_proto):
    return monitoring_info_proto.metric.distribution_data.int_distribution_data
  return None


def create_labels(ptransform=None, tag=None, namespace=None, name=None):
  """Create the label dictionary based on the provided tags.

  Args:
    ptransform: The ptransform/step name.
    tag: he output tag name, used as a label.
  """
  labels = {}
  if tag:
    labels[TAG_LABEL] = tag
  if ptransform:
    labels[PTRANSFORM_LABEL] = ptransform
  if namespace:
    labels[NAMESPACE_LABEL] = namespace
  if name:
    labels[NAME_LABEL] = name
  return labels


def int64_user_counter(namespace, name, metric, ptransform=None, tag=None):
  # type: (...) -> metrics_pb2.MonitoringInfo

  """Return the counter monitoring info for the specifed URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The metric proto field to use in the monitoring info.
        Or an int value.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(
      ptransform=ptransform, tag=tag, namespace=namespace, name=name)
  if isinstance(metric, int):
    metric = metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(int64_value=metric))
  return create_monitoring_info(
      USER_COUNTER_URN, SUM_INT64_TYPE, metric, labels)


def int64_counter(urn, metric, ptransform=None, tag=None):
  # type: (...) -> metrics_pb2.MonitoringInfo

  """Return the counter monitoring info for the specifed URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The metric proto field to use in the monitoring info.
        Or an int value.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(ptransform=ptransform, tag=tag)
  if isinstance(metric, int):
    metric = metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(int64_value=metric))
  return create_monitoring_info(urn, SUM_INT64_TYPE, metric, labels)


def int64_user_distribution(namespace, name, metric, ptransform=None, tag=None):
  """Return the distribution monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The metric proto field to use in the monitoring info.
        Or an int value.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(
      ptransform=ptransform, tag=tag, namespace=namespace, name=name)
  return create_monitoring_info(
      USER_DISTRIBUTION_COUNTER_URN, DISTRIBUTION_INT64_TYPE, metric, labels)


def int64_distribution(urn, metric, ptransform=None, tag=None):
  # type: (...) -> metrics_pb2.MonitoringInfo

  """Return a distribution monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The metric proto field to use in the monitoring info.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(ptransform=ptransform, tag=tag)
  return create_monitoring_info(urn, DISTRIBUTION_INT64_TYPE, metric, labels)


def int64_user_gauge(namespace, name, metric, ptransform=None, tag=None):
  # type: (...) -> metrics_pb2.MonitoringInfo

  """Return the gauge monitoring info for the URN, metric and labels.

  Args:
    namespace: User-defined namespace of counter.
    name: Name of counter.
    metric: The metric proto field to use in the monitoring info.
        Or an int value.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(
      ptransform=ptransform, tag=tag, namespace=namespace, name=name)
  return create_monitoring_info(
      USER_COUNTER_URN, LATEST_INT64_TYPE, metric, labels)


def int64_gauge(urn, metric, ptransform=None, tag=None):
  """Return the gauge monitoring info for the URN, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    metric: The metric proto field to use in the monitoring info.
    ptransform: The ptransform/step name used as a label.
    tag: The output tag name, used as a label.
  """
  labels = create_labels(ptransform=ptransform, tag=tag)
  if isinstance(metric, int):
    metric = metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(int64_value=metric))
  return create_monitoring_info(urn, LATEST_INT64_TYPE, metric, labels)


def create_monitoring_info(urn, type_urn, metric_proto, labels=None):
  # type: (...) -> metrics_pb2.MonitoringInfo

  """Return the gauge monitoring info for the URN, type, metric and labels.

  Args:
    urn: The URN of the monitoring info/metric.
    type_urn: The URN of the type of the monitoring info/metric.
        i.e. beam:metrics:sum_int_64, beam:metrics:latest_int_64.
    metric_proto: The metric proto field to use in the monitoring info.
        Or an int value.
    labels: The label dictionary to use in the MonitoringInfo.
  """
  return metrics_pb2.MonitoringInfo(
      urn=urn,
      type=type_urn,
      labels=labels or dict(),
      metric=metric_proto,
      timestamp=to_timestamp_proto(time.time()))


def is_counter(monitoring_info_proto):
  """Returns true if the monitoring info is a coutner metric."""
  return monitoring_info_proto.type in COUNTER_TYPES


def is_distribution(monitoring_info_proto):
  """Returns true if the monitoring info is a distrbution metric."""
  return monitoring_info_proto.type in DISTRIBUTION_TYPES


def is_gauge(monitoring_info_proto):
  """Returns true if the monitoring info is a gauge metric."""
  return monitoring_info_proto.type in GAUGE_TYPES


def _is_user_monitoring_info(monitoring_info_proto):
  return monitoring_info_proto.urn == USER_COUNTER_URN


def _is_user_distribution_monitoring_info(monitoring_info_proto):
  return monitoring_info_proto.urn == USER_DISTRIBUTION_COUNTER_URN


def is_user_monitoring_info(monitoring_info_proto):
  """Returns true if the monitoring info is a user metric."""

  return _is_user_monitoring_info(
      monitoring_info_proto) or _is_user_distribution_monitoring_info(
          monitoring_info_proto)


def extract_metric_result_map_value(monitoring_info_proto):
  """Returns the relevant GaugeResult, DistributionResult or int value.

  These are the proper format for use in the MetricResult.query() result.
  """
  # Returns a metric result (AKA the legacy format).
  # from the MonitoringInfo
  if is_counter(monitoring_info_proto):
    return extract_counter_value(monitoring_info_proto)
  if is_distribution(monitoring_info_proto):
    distribution_data = extract_distribution(monitoring_info_proto)
    return DistributionResult(
        DistributionData(
            distribution_data.sum,
            distribution_data.count,
            distribution_data.min,
            distribution_data.max))
  if is_gauge(monitoring_info_proto):
    timestamp_secs = to_timestamp_secs(monitoring_info_proto.timestamp)
    return GaugeResult(
        GaugeData(extract_counter_value(monitoring_info_proto), timestamp_secs))


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


def to_key(monitoring_info_proto):
  # type: (metrics_pb2.MonitoringInfo) -> FrozenSet[Hashable]

  """Returns a key based on the URN and labels.

  This is useful in maps to prevent reporting the same MonitoringInfo twice.
  """
  key_items = list(monitoring_info_proto.labels.items())  # type: List[Hashable]
  key_items.append(monitoring_info_proto.urn)
  return frozenset(key_items)


def distribution_combiner(metric_a, metric_b):
  a_data = metric_a.distribution_data.int_distribution_data
  b_data = metric_b.distribution_data.int_distribution_data
  return metrics_pb2.Metric(
      distribution_data=metrics_pb2.DistributionData(
          int_distribution_data=metrics_pb2.IntDistributionData(
              count=a_data.count + b_data.count,
              sum=a_data.sum + b_data.sum,
              min=min(a_data.min, b_data.min),
              max=max(a_data.max, b_data.max))))


_KNOWN_COMBINERS = {
    SUM_INT64_TYPE: lambda a,
    b: metrics_pb2.Metric(
        counter_data=metrics_pb2.CounterData(
            int64_value=a.counter_data.int64_value + b.counter_data.int64_value)
    ),
    DISTRIBUTION_INT64_TYPE: distribution_combiner,
}


def max_timestamp(a, b):
  if a.ToNanoseconds() > b.ToNanoseconds():
    return a
  else:
    return b


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
              metric=combiner(a.metric, b.metric),
              timestamp=max_timestamp(a.timestamp, b.timestamp))

        yield reduce(merge, values)
      else:
        for value in values:
          yield value
