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

from apache_beam.transforms import core
from decimal import *
import math


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


# Distribution Counter
class DistributionAccumulator(object):
  def __init__(self):
    self.min = 0
    self.max = 0
    self.count = 0
    self.sum = 0
    self.sum_of_squares = 0
    # 1,2,5 bucketing
    self.buckets = []
    self.first_bucket_offset = 0
    self.buckets_per_10 = 3

  @staticmethod
  def get_log10_round_to_floor(element):
    ctx = Context(prec=25, rounding=ROUND_FLOOR)
    ten = Decimal(10)
    result = ctx.divide(Decimal(element).ln(ctx), ten.ln(ctx))
    return result.to_integral_exact(rounding=ROUND_FLOOR)

  def add_input(self, element):
    if element < 0:
      return
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    self.sum_of_squares += math.pow(element, 2)
    bucket_index = self.calculate_bucket_index(element)
    new_buckets = self.increment_bucket(bucket_index)
    if not self.buckets:
      self.first_bucket_offset = bucket_index
    else:
      self.first_bucket_offset = min(self.first_bucket_offset, bucket_index)
    self.buckets = new_buckets

  def calculate_bucket_index(self, element):
    if element == 0:
      return 0
    log10_floor = self.get_log10_round_to_floor(element)
    power_of_ten = math.pow(10, log10_floor)
    if element < 2 * power_of_ten:
      bucket_offset = 0
    elif element < 5 * power_of_ten:
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + (log10_floor * self.buckets_per_10) + bucket_offset

  def increment_bucket(self, bucket_index):
    new_buckets = []
    if not self.buckets:
      new_buckets.append(1)
    elif bucket_index < self.first_bucket_offset:
      new_buckets.append(1)
      new_buckets.extend([0 for i in range(bucket_index+1, self.first_bucket_offset)])
      new_buckets.extend(self.buckets)
    elif bucket_index >= self.first_bucket_offset + len(self.buckets):
      new_buckets.extend(self.buckets)
      new_buckets.extend([0 for i in range(self.first_bucket_offset + len(self.buckets), bucket_index)])
      new_buckets.append(1)
    else:
      cur_index = self.first_bucket_offset
      for cur_value in self.buckets:
        if bucket_index == cur_index:
          new_buckets.append(cur_value + 1)
        else:
          new_buckets.append(cur_value)
        cur_index += 1
    return new_buckets


class DistributionCounterFn(AccumulatorCombineFn):
  _accumulator_type = DistributionAccumulator

