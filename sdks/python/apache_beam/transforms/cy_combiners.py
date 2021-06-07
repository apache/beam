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

"""A library of basic cythonized CombineFn subclasses.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

import operator

from apache_beam.transforms import core

try:
  from apache_beam.transforms.cy_dataflow_distribution_counter import DataflowDistributionCounter
except ImportError:
  from apache_beam.transforms.py_dataflow_distribution_counter import DataflowDistributionCounter


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
    return (
        isinstance(other, AccumulatorCombineFn) and
        self._accumulator_type is other._accumulator_type)

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
    return self.sum // self.count if self.count else _NAN


class DistributionInt64Accumulator(object):
  def __init__(self):
    self.sum = 0
    self.count = 0
    self.min = INT64_MAX
    self.max = INT64_MIN

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    self.sum += element
    self.count += 1
    self.min = min(self.min, element)
    self.max = max(self.max, element)

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.sum += accumulator.sum
      self.count += accumulator.count
      self.min = min(self.min, accumulator.min)
      self.max = max(self.max, accumulator.max)

  def extract_output(self):
    if not INT64_MIN <= self.sum <= INT64_MAX:
      self.sum %= 2**64
      if self.sum >= INT64_MAX:
        self.sum -= 2**64
    mean = self.sum // self.count if self.count else _NAN
    return mean, self.sum, self.count, self.min, self.max


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


class DistributionInt64Fn(AccumulatorCombineFn):
  _accumulator_type = DistributionInt64Accumulator


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
    return self.sum // self.count if self.count else _NAN


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


class DataflowDistributionCounterFn(AccumulatorCombineFn):
  """A subclass of cy_combiners.AccumulatorCombineFn.

  Make DataflowDistributionCounter able to report to Dataflow service via
  CounterFactory.

  When cythonized DataflowDistributinoCounter available, make
  CounterFn combine with cythonized module, otherwise, combine with python
  version.
  """
  _accumulator_type = DataflowDistributionCounter


class ComparableValue(object):
  """A way to allow comparing elements in a rich fashion."""

  __slots__ = (
      'value', '_less_than_fn', '_comparable_value', 'requires_hydration')

  def __init__(self, value, less_than_fn, key_fn, _requires_hydration=False):
    self.value = value
    self.hydrate(less_than_fn, key_fn)
    self.requires_hydration = _requires_hydration

  def hydrate(self, less_than_fn, key_fn):
    self._less_than_fn = less_than_fn if less_than_fn else operator.lt
    self._comparable_value = key_fn(self.value) if key_fn else self.value
    self.requires_hydration = False

  def __lt__(self, other):
    assert not self.requires_hydration
    assert self._less_than_fn is other._less_than_fn
    return self._less_than_fn(self._comparable_value, other._comparable_value)

  def __repr__(self):
    return 'ComparableValue[%s]' % str(self.value)

  def __reduce__(self):
    # Since we can't pickle the Compare and Key Fn we pass None and we signify
    # that this object _requires_hydration.
    return ComparableValue, (self.value, None, None, True)
