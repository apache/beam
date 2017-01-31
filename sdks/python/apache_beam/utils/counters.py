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

"""Counters collect the progress of the Worker for reporting to the service."""

import threading
from apache_beam.transforms import cy_combiners


class Counter(object):
  """A counter aggregates a series of values.

  The aggregation kind of the Counter is specified when the Counter
  is created.  The values aggregated must be of an appropriate for the
  aggregation used.  Aggregations supported are listed in the code.

  (The aggregated value will be reported to the Dataflow service.)

  Do not create directly; call CounterFactory.get_counter instead.

  Attributes:
    name: the name of the counter, a string
    aggregation_kind: one of the aggregation kinds defined by this class.
    total: the total size of all the items passed to update()
    elements: the number of times update() was called
  """

  # Handy references to common counters.
  SUM = cy_combiners.SumInt64Fn()
  MEAN = cy_combiners.MeanInt64Fn()

  def __init__(self, name, combine_fn):
    """Creates a Counter object.

    Args:
      name: the name of this counter.  Typically has three parts:
        "step-output-counter".
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


# Counters that represent Accumulators have names starting with this
USER_COUNTER_PREFIX = 'user-'


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

  def get_aggregator_counter(self, step_name, aggregator):
    """Returns an AggregationCounter for this step's aggregator.

    Passing in the same values will return the same counter.

    Args:
      step_name: the name of this step.
      aggregator: an Aggregator object.
    Returns:
      A new or existing counter.
    """
    return self.get_counter(
        '%s%s-%s' % (USER_COUNTER_PREFIX, step_name, aggregator.name),
        aggregator.combine_fn)

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

  def get_aggregator_values(self, aggregator_or_name):
    """Returns dict of step names to values of the aggregator."""
    with self._lock:
      return get_aggregator_values(
          aggregator_or_name, self.counters, lambda counter: counter.value())


def get_aggregator_values(aggregator_or_name, counter_dict,
                          value_extractor=None):
  """Extracts the named aggregator value from a set of counters.

  Args:
    aggregator_or_name: an Aggregator object or the name of one.
    counter_dict: a dict object of {name: value_wrapper}
    value_extractor: a function to convert the value_wrapper into a value.
      If None, no extraction is done and the value is return unchanged.

  Returns:
    dict of step names to values of the aggregator.
  """
  name = aggregator_or_name
  if value_extractor is None:
    value_extractor = lambda x: x
  if not isinstance(aggregator_or_name, basestring):
    name = aggregator_or_name.name
    return {n: value_extractor(c) for n, c in counter_dict.iteritems()
            if n.startswith(USER_COUNTER_PREFIX)
            and n.endswith('-%s' % name)}
