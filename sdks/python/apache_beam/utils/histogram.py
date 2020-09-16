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

from __future__ import absolute_import
from __future__ import division

import logging
import math
import threading

_LOGGER = logging.getLogger(__name__)


class Histogram(object):
  """A histogram that supports estimated percentile with linear interpolation.

  This class is considered experimental and may break or receive backwards-
  incompatible changes in future versions of the Apache Beam SDK.
  """
  def __init__(self, bucket_type):
    self._lock = threading.Lock()
    self._bucket_type = bucket_type
    self._buckets = {}
    self._num_records = 0
    self._num_top_records = 0
    self._num_bot_records = 0

  def clear(self):
    with self._lock:
      self._buckets = {}
      self._num_records = 0
      self._num_top_records = 0
      self._num_bot_records = 0

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

  def get_percentile_info(self, elem_type, unit):
    def _format(f):
      if f == float('-inf'):
        return '<%s' % self._bucket_type.range_from()
      elif f == float('inf'):
        return '>=%s' % self._bucket_type.range_to()
      else:
        return str(int(round(f)))  # pylint: disable=round-builtin

    with self._lock:
      return (
          'Total number of %s: %s, '
          'P99: %s%s, P90: %s%s, P50: %s%s' % (
              elem_type,
              self.total_count(),
              _format(self._get_linear_interpolation(0.99)),
              unit,
              _format(self._get_linear_interpolation(0.90)),
              unit,
              _format(self._get_linear_interpolation(0.50)),
              unit))

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
