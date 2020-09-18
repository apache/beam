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

# pytype: skip-file

from __future__ import absolute_import

import threading
from builtins import hex
from builtins import object
from collections import namedtuple
from typing import TYPE_CHECKING
from typing import Dict

from apache_beam.transforms import cy_combiners

if TYPE_CHECKING:
  from apache_beam.transforms import core

# Information identifying the IO being measured by a counter.
#
# A CounterName with IOTarget helps identify the IO being measured by a
# counter.
#
# It may represent the consumption of Shuffle IO, or the consumption of
# side inputs. The way in which each is represented is explained in the
# documentation of the side_input_id, and shuffle_id functions.
IOTargetName = namedtuple(
    'IOTargetName', ['requesting_step_name', 'input_index'])


def side_input_id(step_name: str, input_index: int) -> IOTargetName:
  """Create an IOTargetName that identifies the reading of a side input.

  Given a step "s4" that receives two side inputs, then the CounterName
  that represents the consumption of side input number 2 is:
  * step_name: s4    <---|
  * input_index: 2   <---|-- Identifying the side input itself
  * requesting_step_name: s4   <-- Identifying the step that reads from it.

  If "s4" emits the whole AsIter of the side input, down to a step, say "s5",
  then the requesting_step_name of the subsequent consumption will be "s5".
  """
  return IOTargetName(step_name, input_index)


def shuffle_id(step_name: str) -> IOTargetName:
  """Create an IOTargetName that identifies a GBK step.

  Given a step "s6" that is downstream from a GBK "s5", then "s6" will read
  from shuffle. The CounterName that quantifies the consumption of data from
  shuffle has:
  * step_name: s5
  * requesting_step_name: s6

  If "s6" emits the whole iterable down to a step, say "s7", and "s7" continues
  to consume data from the iterable, then a new CounterName will be:
  * step_name: s5    <--- Identifying the GBK
  * requesting_step_name: s6
  """
  return IOTargetName(step_name, None)


_CounterName = namedtuple(
    '_CounterName',
    [
        'name',
        'stage_name',
        'step_name',
        'system_name',
        'namespace',
        'origin',
        'output_index',
        'io_target'
    ])


class CounterName(_CounterName):
  """Naming information for a counter."""
  SYSTEM = object()
  USER = object()

  def __new__(
      cls,
      name,
      stage_name=None,
      step_name=None,
      system_name=None,
      namespace=None,
      origin=None,
      output_index=None,
      io_target=None):
    origin = origin or CounterName.SYSTEM
    return super(CounterName, cls).__new__(
        cls,
        name,
        stage_name,
        step_name,
        system_name,
        namespace,
        origin,
        output_index,
        io_target)

  def __repr__(self):
    return '<CounterName<%s> at %s>' % (self._str_internal(), hex(id(self)))

  def __str__(self):
    return self._str_internal()

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
  BEAM_DISTRIBUTION = cy_combiners.DistributionInt64Fn()

  # Dataflow Distribution Accumulator Fn.
  # TODO(BEAM-4045): Generalize distribution counter if necessary.
  DATAFLOW_DISTRIBUTION = cy_combiners.DataflowDistributionCounterFn()

  def __init__(self, name: CounterName, combine_fn: core.CombineFn) -> None:
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

  def reset(self, value):
    self.accumulator = self.combine_fn.create_accumulator()

  def value(self):
    return self.combine_fn.extract_output(self.accumulator)

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s %s %s' % (
        self.name, self.combine_fn.__class__.__name__, self.value())


class AccumulatorCombineFnCounter(Counter):
  """Counter optimized for a mutating accumulator that holds all the logic."""
  def __init__(
      self, name: CounterName,
      combine_fn: cy_combiners.AccumulatorCombineFn) -> None:
    assert isinstance(combine_fn, cy_combiners.AccumulatorCombineFn)
    super(AccumulatorCombineFnCounter, self).__init__(name, combine_fn)
    self.reset()

  def update(self, value):
    self._fast_add_input(value)

  def reset(self):
    self.accumulator = self.combine_fn.create_accumulator()
    self._fast_add_input = self.accumulator.add_input


class CounterFactory(object):
  """Keeps track of unique counters."""
  def __init__(self):
    self.counters: Dict[CounterName, Counter] = {}

    # Lock to be acquired when accessing the counters map.
    self._lock = threading.Lock()

  def get_counter(
      self, name: CounterName, combine_fn: core.CombineFn) -> Counter:
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

  def reset(self):
    # Counters are cached in state sampler states.
    with self._lock:
      for counter in self.counters.values():
        counter.reset()

  def get_counters(self):
    """Returns the current set of counters.

    Returns:
      An iterable that contains the current set of counters. To make sure that
      multiple threads can iterate over the set of counters, we return a new
      iterable here. Note that the actual set of counters may get modified after
      this method returns hence the returned iterable may be stale.
    """
    with self._lock:
      return self.counters.values()  # pylint: disable=dict-values-not-iterating
