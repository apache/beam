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
This file contains internal metric cell classes. A metric cell is used to
accumulate in-memory changes to a metric. It represents a specific metric
in a single context.

For internal use only. No backwards compatibility guarantees.
"""

# pytype: skip-file

from typing import TYPE_CHECKING
from typing import Optional

from apache_beam.metrics.cells import MetricCell
from apache_beam.metrics.cells import MetricCellFactory
from apache_beam.utils.histogram import Histogram

if TYPE_CHECKING:
  from apache_beam.utils.histogram import BucketType


class HistogramCell(MetricCell):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks the current value and delta for a histogram metric.

  Each cell tracks the state of a metric independently per context per bundle.
  Therefore, each metric has a different cell in each bundle, that is later
  aggregated.

  This class is thread safe since underlying histogram object is thread safe.
  """
  def __init__(self, bucket_type):
    self._bucket_type = bucket_type
    self.data = HistogramData.identity_element(bucket_type)

  def reset(self):
    self.data = HistogramData.identity_element(self._bucket_type)

  def combine(self, other: 'HistogramCell') -> 'HistogramCell':
    result = HistogramCell(self._bucket_type)
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    self.data.histogram.record(value)

  def get_cumulative(self) -> 'HistogramData':
    return self.data.get_cumulative()

  def to_runner_api_monitoring_info(self, name, transform_id):
    # Histogram metric is currently worker-local and internal
    # use only. This method should be implemented when runners
    # support Histogram metric reporting.
    return None


class HistogramCellFactory(MetricCellFactory):
  def __init__(self, bucket_type):
    self._bucket_type = bucket_type

  def __call__(self):
    return HistogramCell(self._bucket_type)

  def __eq__(self, other):
    if not isinstance(other, HistogramCellFactory):
      return False
    return self._bucket_type == other._bucket_type

  def __hash__(self):
    return hash(self._bucket_type)


class HistogramResult(object):
  def __init__(self, data: 'HistogramData') -> None:
    self.data = data

  def __eq__(self, other):
    if isinstance(other, HistogramResult):
      return self.data == other.data
    else:
      return False

  def __hash__(self):
    return hash(self.data)

  def __repr__(self):
    return '<HistogramResult({})>'.format(
        self.data.histogram.get_percentile_info())

  @property
  def p99(self):
    return self.data.histogram.p99()

  @property
  def p95(self):
    return self.data.histogram.p95()

  @property
  def p90(self):
    return self.data.histogram.p90()


class HistogramData(object):
  """For internal use only; no backwards-compatibility guarantees.

  The data structure that holds data about a histogram metric.

  This object is not thread safe, so it's not supposed to be modified
  outside the HistogramCell.
  """
  def __init__(self, histogram):
    self.histogram = histogram

  def __eq__(self, other):
    return self.histogram == other.histogram

  def __hash__(self):
    return hash(self.histogram)

  def __repr__(self):
    return 'HistogramData({})'.format(self.histogram.get_percentile_info())

  def get_cumulative(self) -> 'HistogramData':
    return HistogramData(self.histogram)

  def combine(self, other: Optional['HistogramData']) -> 'HistogramData':
    if other is None:
      return self

    return HistogramData(self.histogram.combine(other.histogram))

  @staticmethod
  def identity_element(bucket_type) -> HistogramData:
    return HistogramData(Histogram(bucket_type))
