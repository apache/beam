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

import logging
import math
import threading
from collections import Counter

from apache_beam.portability.api import metrics_pb2

_LOGGER = logging.getLogger(__name__)


class Histogram(object):
  """A histogram that supports estimated percentile with linear interpolation.
  """
  def __init__(self, bucket_type):
    self._lock = threading.Lock()
    self._bucket_type = bucket_type
    self._buckets = Counter()
    self._num_records = 0
    self._num_top_records = 0
    self._num_bot_records = 0

  def clear(self):
    with self._lock:
      self._buckets = Counter()
      self._num_records = 0
      self._num_top_records = 0
      self._num_bot_records = 0

  def copy(self):
    with self._lock:
      histogram = Histogram(self._bucket_type)
      histogram._num_records = self._num_records
      histogram._num_top_records = self._num_top_records
      histogram._num_bot_records = self._num_bot_records
      histogram._buckets = self._buckets.copy()
      return histogram

  def combine(self, other):
    if not isinstance(other,
                      Histogram) or self._bucket_type != other._bucket_type:
      raise RuntimeError('failed to combine histogram.')
    other_histogram = other.copy()
    with self._lock:
      histogram = Histogram(self._bucket_type)
      histogram._num_records = self._num_records + other_histogram._num_records
      histogram._num_top_records = (
          self._num_top_records + other_histogram._num_top_records)
      histogram._num_bot_records = (
          self._num_bot_records + other_histogram._num_bot_records)
      histogram._buckets = self._buckets + other_histogram._buckets
      return histogram

  def record(self, *args):
    for arg in args:
      self._record(arg)

  def _record(self, value):
    range_from = self._bucket_type.range_from()
    range_to = self._bucket_type.range_to()
    with self._lock:
      if value >= range_to:
        _LOGGER.warning('record is out of upper bound %s: %s', range_to, value)
        self._num_top_records += 1
      elif value < range_from:
        _LOGGER.warning(
            'record is out of lower bound %s: %s', range_from, value)
        self._num_bot_records += 1
      else:
        index = self._bucket_type.bucket_index(value)
        self._buckets[index] = self._buckets.get(index, 0) + 1
        self._num_records += 1

  def total_count(self):
    return self._num_records + self._num_top_records + self._num_bot_records

  def p99(self):
    return self.get_linear_interpolation(0.99)

  def p90(self):
    return self.get_linear_interpolation(0.90)

  def p50(self):
    return self.get_linear_interpolation(0.50)

  def get_percentile_info(self):
    def _format(f):
      if f == float('-inf'):
        return '<%s' % self._bucket_type.range_from()
      elif f == float('inf'):
        return '>=%s' % self._bucket_type.range_to()
      else:
        return str(int(round(f)))  # pylint: disable=bad-option-value

    with self._lock:
      if self.total_count():
        return (
            'Total count: %s, '
            'P99: %s, P90: %s, P50: %s' % (
                self.total_count(),
                _format(self._get_linear_interpolation(0.99)),
                _format(self._get_linear_interpolation(0.90)),
                _format(self._get_linear_interpolation(0.50))))
      else:
        return ('Total count: %s' % (self.total_count(), ))

  def get_linear_interpolation(self, percentile):
    """Calculate percentile estimation based on linear interpolation.

    It first finds the bucket which includes the target percentile and
    projects the estimated point in the bucket by assuming all the elements
    in the bucket are uniformly distributed.

    Args:
      percentile: The target percentile of the value returning from this
        method. Should be a floating point number greater than 0 and less
        than 1.
    """
    if percentile > 1 or percentile < 0:
      raise ValueError('percentile should be between 0 and 1.')
    with self._lock:
      return self._get_linear_interpolation(percentile)

  def _get_linear_interpolation(self, percentile):
    total_num_records = self.total_count()
    if total_num_records == 0:
      raise RuntimeError('histogram has no record.')

    index = 0
    record_sum = self._num_bot_records
    if record_sum / total_num_records >= percentile:
      return float('-inf')
    while index < self._bucket_type.num_buckets():
      record_sum += self._buckets.get(index, 0)
      if record_sum / total_num_records >= percentile:
        break
      index += 1
    if index == self._bucket_type.num_buckets():
      return float('inf')

    frac_percentile = percentile - (
        record_sum - self._buckets[index]) / total_num_records
    bucket_percentile = self._buckets[index] / total_num_records
    frac_bucket_size = frac_percentile * self._bucket_type.bucket_size(
        index) / bucket_percentile
    return (
        self._bucket_type.range_from() +
        self._bucket_type.accumulated_bucket_size(index) + frac_bucket_size)

  def __eq__(self, other):
    if not isinstance(other, Histogram):
      return False

    def nonzero_buckets(buckets):
      return {k: v for k, v in buckets.items() if v != 0}

    return (
        self._bucket_type == other._bucket_type and
        self._num_records == other._num_records and
        self._num_top_records == other._num_top_records and
        self._num_bot_records == other._num_bot_records and
        nonzero_buckets(self._buckets) == nonzero_buckets(other._buckets))

  def __hash__(self):
    return hash((
        self._bucket_type,
        self._num_records,
        self._num_top_records,
        self._num_bot_records,
        frozenset(self._buckets.items())))

  def to_runner_api(self) -> metrics_pb2.HistogramValue:
    return metrics_pb2.HistogramValue(
        count=self.total_count(),
        bucket_counts=[
            self._buckets.get(idx, 0)
            for idx in range(self._bucket_type.num_buckets())
        ],
        bucket_options=self._bucket_type.to_runner_api())

  @classmethod
  def from_runner_api(cls, proto: metrics_pb2.HistogramValue):
    bucket_options_proto = proto.bucket_options
    if bucket_options_proto.linear is not None:
      bucket_options = LinearBucket.from_runner_api(bucket_options_proto)
    else:
      raise NotImplementedError
    histogram = cls(bucket_options)
    with histogram._lock:
      for bucket_index, count in enumerate(proto.bucket_counts):
        histogram._buckets[bucket_index] = count
      histogram._num_records = sum(proto.bucket_counts)
    return histogram


class BucketType(object):
  def range_from(self):
    """Lower bound of a starting bucket."""
    raise NotImplementedError

  def range_to(self):
    """Upper bound of an ending bucket."""
    raise NotImplementedError

  def num_buckets(self):
    """The number of buckets."""
    raise NotImplementedError

  def bucket_index(self, value):
    """Get the bucket array index for the given value."""
    raise NotImplementedError

  def bucket_size(self, index):
    """Get the bucket size for the given bucket array index."""
    raise NotImplementedError

  def accumulated_bucket_size(self, end_index):
    """Get the accumulated bucket size from bucket index 0 until endIndex.

    Generally, this can be calculated as
    `sigma(0 <= i < endIndex) getBucketSize(i)`. However, a child class could
    provide better optimized calculation.
    """
    raise NotImplementedError

  def to_runner_api(self):
    """Convert to the runner API representation."""
    raise NotImplementedError

  @classmethod
  def from_runner_api(cls, proto):
    raise NotImplementedError


class LinearBucket(BucketType):
  def __init__(self, start, width, num_buckets):
    """Create a histogram with linear buckets.

    Args:
      start: Lower bound of a starting bucket.
      width: Bucket width. Smaller width implies a better resolution for
        percentile estimation.
      num_buckets: The number of buckets. Upper bound of an ending bucket is
        defined by start + width * numBuckets.
    """
    self._start = start
    self._width = width
    self._num_buckets = num_buckets

  def range_from(self):
    return self._start

  def range_to(self):
    return self._start + self._width * self._num_buckets

  def num_buckets(self):
    return self._num_buckets

  def bucket_index(self, value):
    return math.floor((value - self._start) / self._width)

  def bucket_size(self, index):
    return self._width

  def accumulated_bucket_size(self, end_index):
    return self._width * end_index

  def __eq__(self, other):
    if not isinstance(other, LinearBucket):
      return False
    return (
        self._start == other._start and self._width == other._width and
        self._num_buckets == other._num_buckets)

  def __hash__(self):
    return hash((self._start, self._width, self._num_buckets))

  def to_runner_api(self):
    return metrics_pb2.HistogramValue.BucketOptions(
        linear=metrics_pb2.HistogramValue.BucketOptions.Linear(
            number_of_buckets=self._num_buckets,
            width=self._width,
            start=self._start))

  @classmethod
  def from_runner_api(cls, proto):
    return LinearBucket(
        start=proto.linear.start,
        width=proto.linear.width,
        num_buckets=proto.linear.number_of_buckets)
