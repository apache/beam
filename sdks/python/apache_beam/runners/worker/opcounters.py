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

"""Counters collect the progress of the Worker for reporting to the service."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import math
import random
from builtins import hex
from builtins import object
from typing import TYPE_CHECKING
from typing import Optional

from apache_beam.internal import pickler
from apache_beam.typehints.typecheck import TypeCheckWrapperDoFn
from apache_beam.utils import counters
from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterName

if TYPE_CHECKING:
  from apache_beam.utils import windowed_value
  from apache_beam.runners.worker.statesampler import StateSampler

# This module is experimental. No backwards-compatibility guarantees.


class TransformIOCounter(object):
  """Class to track time and bytes consumed while reading from IO.

  Subclasses should be able to track consumption of IO across steps
  in the same stage - for instance, if a Shuffle or Side Input iterable
  is passed down to a next step.

  Some examples of IO can be side inputs, shuffle, or streaming state.
  """
  def __init__(self, counter_factory, state_sampler):
    """Create a new IO read counter.

    Args:
      counter_factory: A counters.CounterFactory to create byte counters.
      state_sampler: A statesampler.StateSampler to transition into read states.
    """
    self._counter_factory = counter_factory
    self._state_sampler = state_sampler
    self._latest_step = None
    self.bytes_read_counter = None
    self.scoped_state = None

  def update_current_step(self):
    """Update the current running step.

    Due to the fusion optimization, user code may choose to emit the data
    structure that holds side inputs (Iterable, Dict, or others). This call
    updates the current step, to attribute the data consumption to the step
    that is responsible for actual consumption.

    CounterName uses the io_target field for information pertinent to the
    consumption of IO.
    """
    current_state = self._state_sampler.current_state()
    current_step_name = current_state.name.step_name
    if current_step_name != self._latest_step:
      self._latest_step = current_step_name
      self._update_counters_for_requesting_step(current_step_name)

  def _update_counters_for_requesting_step(self, step_name):
    pass

  def add_bytes_read(self, count):
    if count > 0 and self.bytes_read_counter:
      self.bytes_read_counter.update(count)

  def __enter__(self):
    self.scoped_state.__enter__()

  def __exit__(self, exception_type, exception_value, traceback):
    self.scoped_state.__exit__(exception_type, exception_value, traceback)


class NoOpTransformIOCounter(TransformIOCounter):
  """All operations for IO tracking are no-ops."""
  def __init__(self):
    super(NoOpTransformIOCounter, self).__init__(None, None)

  def update_current_step(self):
    pass

  def __enter__(self):
    pass

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def add_bytes_read(self, count):
    pass


class SideInputReadCounter(TransformIOCounter):
  """Tracks time and bytes consumed while reading from side inputs.

  This class is designed to track consumption of side inputs across fused steps.
  We represent a side input as a declaring step, and an input index.

  The declaring step is the step that originally receives the side input for
  consumption, and the input index in which the declaring step receives the side
  input that we want to identify.

  Note that the declaring step originally receives the side input, but it may
  not be the only step that spends time reading from this side input.
  """

  def __init__(self,
               counter_factory,
               state_sampler,  # type: StateSampler
               declaring_step,
               input_index
              ):
    """Create a side input read counter.

    Args:
      counter_factory: A counters.CounterFactory to create byte counters.
      state_sampler: A statesampler.StateSampler to transition into read states.
      declaring_step: A string with the step name of the step that directly
        receives the side input initially.
      input_index: The index of the side input in the list of inputs of the
        declaring step.

    The side input is uniquely identified by (declaring_step, input_index);
    where declaring_step is the step that receives the PCollectionView as a
    side input, and input_index is the index of the PCollectionView within
    the list of inputs.
    """
    super(SideInputReadCounter, self).__init__(counter_factory, state_sampler)
    self.declaring_step = declaring_step
    self.input_index = input_index

    # Side inputs are set up within the start state of the first receiving
    # step. We check the current state to create the internal counters.
    self.update_current_step()

  def _update_counters_for_requesting_step(self, step_name):
    side_input_id = counters.side_input_id(step_name, self.input_index)
    self.scoped_state = self._state_sampler.scoped_state(
        self.declaring_step, 'read-sideinput', io_target=side_input_id)
    self.bytes_read_counter = self._counter_factory.get_counter(
        CounterName(
            'read-sideinput-byte-count',
            step_name=self.declaring_step,
            io_target=side_input_id),
        Counter.SUM)


class SumAccumulator(object):
  """Accumulator for collecting byte counts."""
  def __init__(self):
    self._value = 0

  def update(self, value):
    self._value += value

  def value(self):
    return self._value


class OperationCounters(object):
  """The set of basic counters to attach to an Operation."""
  def __init__(
      self,
      counter_factory,
      step_name,  # type: str
      coder,
      index,
      suffix='out',
      consumers=None,
      producer=None):
    self._counter_factory = counter_factory
    self.element_counter = counter_factory.get_counter(
        '%s-%s%s-ElementCount' % (step_name, suffix, index), Counter.SUM)
    self.mean_byte_counter = counter_factory.get_counter(
        '%s-%s%s-MeanByteCount' % (step_name, suffix, index),
        Counter.BEAM_DISTRIBUTION)
    self.coder_impl = coder.get_impl() if coder else None
    self.active_accumulator = None  # type: Optional[SumAccumulator]
    self.current_size = None  # type: Optional[int]
    self._sample_counter = 0
    self._next_sample = 0

    self.producer_type_hints = None
    if producer and hasattr(producer, 'spec') and hasattr(producer.spec,
                                                          'serialized_fn'):
      fns = pickler.loads(producer.spec.serialized_fn)
      if fns:
        if hasattr(fns[0], '_runtime_type_hints'):
          self.producer_type_hints = fns[0]._runtime_type_hints

    self.consumers_type_hints = []

    if consumers:
      for consumer in consumers:
        if hasattr(consumer, 'spec') and hasattr(consumer.spec,
                                                 'serialized_fn'):
          fns = pickler.loads(consumer.spec.serialized_fn)
          if fns:
            if hasattr(fns[0], '_runtime_type_hints'):
              self.consumers_type_hints.append(fns[0]._runtime_type_hints)

  def update_from(self, windowed_value):
    # type: (windowed_value.WindowedValue) -> None

    """Add one value to this counter."""
    if self._should_sample():
      self.do_sample(windowed_value)

  def _observable_callback(self, inner_coder_impl, accumulator):
    def _observable_callback_inner(value, is_encoded=False):
      # TODO(ccy): If this stream is large, sample it as well.
      # To do this, we'll need to compute the average size of elements
      # in this stream to add the *total* size of this stream to accumulator.
      # We'll also want make sure we sample at least some of this stream
      # (as self.should_sample() may be sampling very sparsely by now).
      if is_encoded:
        size = len(value)
        accumulator.update(size)
      else:
        accumulator.update(inner_coder_impl.estimate_size(value))

    return _observable_callback_inner

  def type_check(self, value):
    # type: (any, bool) -> None

    if self.producer_type_hints and hasattr(self.producer_type_hints,
                                            'output_types'):
      output_constraint = self.producer_type_hints.output_types

      while True:
        try:
          output_constraint = next(iter(output_constraint))
        except TypeError:
          break
      if output_constraint:
        TypeCheckWrapperDoFn.type_check(output_constraint, value, False)

    for consumer_type_hints in self.consumers_type_hints:
      if consumer_type_hints and hasattr(consumer_type_hints, 'input_types'):
        input_constraint = consumer_type_hints.input_types
        while True:
          try:
            input_constraint = next(iter(input_constraint))
          except (TypeError, StopIteration):
            break
        if input_constraint:
          TypeCheckWrapperDoFn.type_check(input_constraint, value, False)

  def do_sample(self, windowed_value):
    # type: (windowed_value.WindowedValue) -> None
    self.type_check(windowed_value.value)

    size, observables = (
        self.coder_impl.get_estimated_size_and_observables(windowed_value))
    if not observables:
      self.current_size = size
    else:
      self.active_accumulator = SumAccumulator()
      self.active_accumulator.update(size)
      for observable, inner_coder_impl in observables:
        observable.register_observer(
            self._observable_callback(
                inner_coder_impl, self.active_accumulator))

  def update_collect(self):
    """Collects the accumulated size estimates.

    Now that the element has been processed, we ask our accumulator
    for the total and store the result in a counter.
    """
    self.element_counter.update(1)
    if self.current_size is not None:
      self.mean_byte_counter.update(self.current_size)
      self.current_size = None
    elif self.active_accumulator is not None:
      self.mean_byte_counter.update(self.active_accumulator.value())
      self.active_accumulator = None

  def _compute_next_sample(self, i):
    # https://en.wikipedia.org/wiki/Reservoir_sampling#Fast_Approximation
    gap = math.log(1.0 - random.random()) / math.log(1.0 - (10.0 / i))
    return i + math.floor(gap)

  def _should_sample(self):
    """Determines whether to sample the next element.

    Size calculation can be expensive, so we don't do it for each element.
    Because we need only an estimate of average size, we sample.

    We always sample the first 10 elements, then the sampling rate
    is approximately 10/N.  After reading N elements, of the next N,
    we will sample approximately 10*ln(2) (about 7) elements.

    This algorithm samples at the same rate as Reservoir Sampling, but
    it never throws away early results.  (Because we keep only a
    running accumulation, storage is not a problem, so there is no
    need to discard earlier calculations.)

    Because we accumulate and do not replace, our statistics are
    biased toward early data.  If the data are distributed uniformly,
    this is not a problem.  If the data change over time (i.e., the
    element size tends to grow or shrink over time), our estimate will
    show the bias.  We could correct this by giving weight N to each
    sample, since each sample is a stand-in for the N/(10*ln(2))
    samples around it, which is proportional to N.  Since we do not
    expect biased data, for efficiency we omit the extra multiplication.
    We could reduce the early-data bias by putting a lower bound on
    the sampling rate.

    Computing random.randint(1, self._sample_counter) for each element
    is too slow, so when the sample size is big enough (we estimate 30
    is big enough), we estimate the size of the gap after each sample.
    This estimation allows us to call random much less often.

    Returns:
      True if it is time to compute another element's size.
    """
    if self.coder_impl is None:
      return False
    self._sample_counter += 1
    if self._next_sample == 0:
      if random.randint(1, self._sample_counter) <= 10:
        if self._sample_counter > 30:
          self._next_sample = self._compute_next_sample(self._sample_counter)
        return True
      return False
    elif self._sample_counter >= self._next_sample:
      self._next_sample = self._compute_next_sample(self._sample_counter)
      return True
    return False

  def should_sample(self):
    # We create this separate method because the above "_should_sample()" method
    # is marked as inline in Cython and thus can't be exposed to Python code.
    return self._should_sample()

  def restart_sampling(self):
    self._sample_counter = 0

  def __str__(self):
    return '<%s [%s]>' % (
        self.__class__.__name__, ', '.join([str(x) for x in self.__iter__()]))

  def __repr__(self):
    return '<%s %s at %s>' % (
        self.__class__.__name__, [x for x in self.__iter__()], hex(id(self)))
