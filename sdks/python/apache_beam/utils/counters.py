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

# cython: profile=False
# cython: overflowcheck=True

"""Counters collect the progress of the Worker for reporting to the service.

For internal use only; no backwards-compatibility guarantees.
"""

import threading
from collections import namedtuple

from apache_beam.transforms import cy_combiners

# Information identifying the IO being measured by a counter.
IOTargetName = namedtuple('IOTargetName', ['side_input_step_name',
                                           'side_input_index',
                                           'original_shuffle_step_name'])


def side_input_id(step_name, input_index):
  """Create an IOTargetName that identifies the reading of a side input."""
  return IOTargetName(step_name, input_index, None)


def shuffle_id(step_name):
  """Create an IOTargetName that identifies a GBK step."""
  return IOTargetName(None, None, step_name)


_CounterName = namedtuple('_CounterName', ['name',
                                           'stage_name',
                                           'step_name',
                                           'system_name',
                                           'namespace',
                                           'origin',
                                           'output_index',
                                           'io_target'])


class CounterName(_CounterName):
  """Naming information for a counter."""
  SYSTEM = object()
  USER = object()

  def __new__(cls, name, stage_name=None, step_name=None,
              system_name=None, namespace=None,
              origin=None, output_index=None, io_target=None):
    origin = origin or CounterName.SYSTEM
    return super(CounterName, cls).__new__(cls, name, stage_name, step_name,
                                           system_name, namespace,
                                           origin, output_index, io_target)

  def __str__(self):
    return '%s' % self._str_internal()

  def __repr__(self):
    return '<CounterName<%s> at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    if self.origin == CounterName.USER:
      return 'user-%s-%s' % (self.step_name, self.name)
    elif self.origin == CounterName.SYSTEM and self.output_index:
      return '%s-out%s-%s' % (self.step_name, self.output_index, self.name)
    else:
      return '%s-%s-%s' % (self.stage_name, self.step_name, self.name)


class Counter(object):
  """A counter aggregates a series of values.

  The aggregation kind of the Counter is specified when the Counter
  is created.  The values aggregated must be of an appropriate for the
  aggregation used.  Aggregations supported are listed in the code.

  (The aggregated value will be reported to the Dataflow service.)

  Do not create directly; call CounterFactory.get_counter instead.

  Attributes:
    name: the name of the counter, a string
    combine_fn: the CombineFn to use for aggregation
    accumulator: the accumulator created for the combine_fn
  """

  # Handy references to common counters.
  SUM = cy_combiners.SumInt64Fn()
  MEAN = cy_combiners.MeanInt64Fn()

  def __init__(self, name, combine_fn):
    """Creates a Counter object.

    Args:
      name: the name of this counter. It may be a string,
            or a CounterName object.
      combine_fn: the CombineFn to use for aggregation
    """
    self.name = name
    self.combine_fn = combine_fn
    self.accumulator = combine_fn.create_accumulator()
    self._add_input = self.combine_fn.add_input

  def update(self, value):
    self.accumulator = self._add_input(self.accumulator, value)

  def value(self):
    return self.combine_fn.extract_output(self.accumulator)

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s %s %s' % (self.name, self.combine_fn.__class__.__name__,
                         self.value())


class AccumulatorCombineFnCounter(Counter):
  """Counter optimized for a mutating accumulator that holds all the logic."""

  def __init__(self, name, combine_fn):
    assert isinstance(combine_fn, cy_combiners.AccumulatorCombineFn)
    super(AccumulatorCombineFnCounter, self).__init__(name, combine_fn)
    self._fast_add_input = self.accumulator.add_input

  def update(self, value):
    self._fast_add_input(value)


class CounterFactory(object):
  """Keeps track of unique counters."""

  def __init__(self):
    self.counters = {}

    # Lock to be acquired when accessing the counters map.
    self._lock = threading.Lock()

  def get_counter(self, name, combine_fn):
    """Returns a counter with the requested name.

    Passing in the same name will return the same counter; the
    combine_fn must agree.

    Args:
      name: the name of this counter.  Typically has three parts:
        "step-output-counter".
      combine_fn: the CombineFn to use for aggregation
    Returns:
      A new or existing counter with the requested name.
    """
    with self._lock:
      counter = self.counters.get(name, None)
      if counter:
        assert counter.combine_fn == combine_fn
      else:
        if isinstance(combine_fn, cy_combiners.AccumulatorCombineFn):
          counter = AccumulatorCombineFnCounter(name, combine_fn)
        else:
          counter = Counter(name, combine_fn)
        self.counters[name] = counter
      return counter

  def get_counters(self):
    """Returns the current set of counters.

    Returns:
      An iterable that contains the current set of counters. To make sure that
      multiple threads can iterate over the set of counters, we return a new
      iterable here. Note that the actual set of counters may get modified after
      this method returns hence the returned iterable may be stale.
    """
    with self._lock:
      return self.counters.values()
