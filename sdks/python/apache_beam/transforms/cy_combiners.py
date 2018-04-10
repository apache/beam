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

"""A library of basic cythonized CombineFn subclasses.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import math

from apache_beam.transforms import core


class AccumulatorCombineFn(core.CombineFn):
  # singleton?
  def create_accumulator(self):
    return self._accumulator_type()

  @staticmethod
  def add_input(accumulator, element):
    accumulator.add_input(element)
    return accumulator

  def merge_accumulators(self, accumulators):
    accumulator = self._accumulator_type()
    accumulator.merge(accumulators)
    return accumulator

  @staticmethod
  def extract_output(accumulator):
    return accumulator.extract_output()

  def __eq__(self, other):
    return (isinstance(other, AccumulatorCombineFn)
            and self._accumulator_type is other._accumulator_type)

  def __hash__(self):
    return hash(self._accumulator_type)


_63 = 63  # Avoid large literals in C source code.
globals()['INT64_MAX'] = 2**_63 - 1
globals()['INT64_MIN'] = -2**_63


class CountAccumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, unused_element):
    self.value += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    return self.value


class SumInt64Accumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, element):
    global INT64_MAX, INT64_MIN  # pylint: disable=global-variable-not-assigned
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    self.value += element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    if not INT64_MIN <= self.value <= INT64_MAX:
      self.value %= 2**64
      if self.value >= INT64_MAX:
        self.value -= 2**64
    return self.value


class MinInt64Accumulator(object):
  def __init__(self):
    self.value = INT64_MAX

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    if element < self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value < self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MaxInt64Accumulator(object):
  def __init__(self):
    self.value = INT64_MIN

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    if element > self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value > self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MeanInt64Accumulator(object):
  def __init__(self):
    self.sum = 0
    self.count = 0

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    self.sum += element
    self.count += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.sum += accumulator.sum
      self.count += accumulator.count

  def extract_output(self):
    if not INT64_MIN <= self.sum <= INT64_MAX:
      self.sum %= 2**64
      if self.sum >= INT64_MAX:
        self.sum -= 2**64
    return self.sum / self.count if self.count else _NAN


class CountCombineFn(AccumulatorCombineFn):
  _accumulator_type = CountAccumulator


class SumInt64Fn(AccumulatorCombineFn):
  _accumulator_type = SumInt64Accumulator


class MinInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MinInt64Accumulator


class MaxInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MaxInt64Accumulator


class MeanInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MeanInt64Accumulator


_POS_INF = float('inf')
_NEG_INF = float('-inf')
_NAN = float('nan')


class SumDoubleAccumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, element):
    element = float(element)
    self.value += element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    return self.value


class MinDoubleAccumulator(object):
  def __init__(self):
    self.value = _POS_INF

  def add_input(self, element):
    element = float(element)
    if element < self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value < self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MaxDoubleAccumulator(object):
  def __init__(self):
    self.value = _NEG_INF

  def add_input(self, element):
    element = float(element)
    if element > self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value > self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MeanDoubleAccumulator(object):
  def __init__(self):
    self.sum = 0
    self.count = 0

  def add_input(self, element):
    element = float(element)
    self.sum += element
    self.count += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.sum += accumulator.sum
      self.count += accumulator.count

  def extract_output(self):
    return self.sum / self.count if self.count else _NAN


class SumFloatFn(AccumulatorCombineFn):
  _accumulator_type = SumDoubleAccumulator


class MinFloatFn(AccumulatorCombineFn):
  _accumulator_type = MinDoubleAccumulator


class MaxFloatFn(AccumulatorCombineFn):
  _accumulator_type = MaxDoubleAccumulator


class MeanFloatFn(AccumulatorCombineFn):
  _accumulator_type = MeanDoubleAccumulator


class AllAccumulator(object):
  def __init__(self):
    self.value = True

  def add_input(self, element):
    self.value &= not not element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value &= accumulator.value

  def extract_output(self):
    return self.value


class AnyAccumulator(object):
  def __init__(self):
    self.value = False

  def add_input(self, element):
    self.value |= not not element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value |= accumulator.value

  def extract_output(self):
    return self.value


class AnyCombineFn(AccumulatorCombineFn):
  _accumulator_type = AnyAccumulator


class AllCombineFn(AccumulatorCombineFn):
  _accumulator_type = AllAccumulator


MAX_LONG_10_FOR_LEADING_ZEROS = [19, 18, 18, 18, 18, 17, 17, 17, 16, 16, 16, 15,
                                 15, 15, 15, 14, 14, 14, 13, 13, 13, 12, 12, 12,
                                 12, 11, 11, 11, 10, 10, 10, 9, 9, 9, 9, 8, 8,
                                 8, 7, 7, 7, 6, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3,
                                 3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0]

LONG_SIZE = 64


def compare_to(x, y):
  """return the sign bit of x-y"""
  if x < y:
    return 1
  return 0


def get_log10_round_to_floor(element):
  number_of_leading_zeros = LONG_SIZE - element.bit_length()
  y = MAX_LONG_10_FOR_LEADING_ZEROS[number_of_leading_zeros]
  return y - compare_to(element, math.pow(10, y))


class DistributionAccumulator(object):
  """Distribution Counter:
  contains value distribution statistics and methods for incrementing
  """
  def __init__(self):
    global INT64_MAX # pylint: disable=global-variable-not-assigned
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    """Histogram buckets of value counts for a distribution(1,2,5 bucketing)"""
    self.buckets = []
    """Starting index of the first stored bucket"""
    self.first_bucket_offset = 0
    """There are 3 buckets for every power of ten: 1, 2, 5"""
    self.buckets_per_10 = 3

  def add_input(self, element):
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    bucket_index = self.calculate_bucket_index(element)
    size_of_bucket = len(self.buckets)
    self.increment_bucket(bucket_index)
    if size_of_bucket == 0:
      self.first_bucket_offset = bucket_index
    else:
      self.first_bucket_offset = min(self.first_bucket_offset, bucket_index)

  def calculate_bucket_index(self, element):
    """Calculate the bucket index for the given element"""
    if element == 0:
      return 0
    log10_floor = get_log10_round_to_floor(element)
    power_of_ten = math.pow(10, log10_floor)
    if element < 2 * power_of_ten:
      bucket_offset = 0  # [0, 2)
    elif element < 5 * power_of_ten:
      bucket_offset = 1  # [2, 5)
    else:
      bucket_offset = 2  # [5, 10)
    return 1 + (log10_floor * self.buckets_per_10) + bucket_offset

  def increment_bucket(self, bucket_index):
    """Increment the bucket for the given index
    If the bucket at the given index is already in the list,
    this will increment the existing value.
    If the specified index is outside of the current bucket range,
    the bucket list will be extended to incorporate the new bucket
    """
    if not self.buckets:
      self.buckets.append(1)
    elif bucket_index < self.first_bucket_offset:
      new_buckets = []
      new_buckets.append(1)
      new_buckets.extend(
          [0] * (self.first_bucket_offset - bucket_index - 1))
      self.buckets = new_buckets + self.buckets
    elif bucket_index >= self.first_bucket_offset + len(self.buckets):
      self.buckets.extend(
          [0] * (bucket_index - self.first_bucket_offset - len(self.buckets)))
      self.buckets.append(1)
    else:
      self.buckets[bucket_index - self.first_bucket_offset] += 1


class DistributionCounterFn(AccumulatorCombineFn):
  _accumulator_type = DistributionAccumulator
