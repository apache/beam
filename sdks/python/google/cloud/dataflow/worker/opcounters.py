# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Counters collect the progress of the Worker for reporting to the service."""

from __future__ import absolute_import
import math
import random

from google.cloud.dataflow.coders import WindowedValueCoder
from google.cloud.dataflow.transforms.window import WindowedValue
from google.cloud.dataflow.utils.counters import Counter


class OperationCounters(object):
  """The set of basic counters to attach to an Operation."""

  def __init__(self, counter_factory, step_name, coder, output_index):
    self._counter_factory = counter_factory
    self.element_counter = counter_factory.get_counter(
        '%s-out%d-ElementCount' % (step_name, output_index), Counter.SUM)
    self.mean_byte_counter = counter_factory.get_counter(
        '%s-out%d-MeanByteCount' % (step_name, output_index), Counter.MEAN)
    self.coder = coder
    self._active_accumulators = []
    self._sample_counter = 0
    self._next_sample = 0

  def update_from(self, windowed_value, coder=None):
    """Add one value to this counter."""
    self.element_counter.update(1)
    if self.should_sample():
      byte_size_accumulator = self._counter_factory.get_counter(
          '%s-temp%d' % (self.mean_byte_counter.name, self._sample_counter),
          Counter.SUM)
      self._active_accumulators.append(byte_size_accumulator)
      # Shuffle operations may pass in their own coder
      if coder is None:
        coder = self.coder
      # Some Readers and Writers return windowed values even
      # though their output encoding does not claim to be windowed.
      # TODO(ccy): fix output encodings to be consistent here
      if (isinstance(windowed_value, WindowedValue)
          and not isinstance(coder, WindowedValueCoder)):
        coder = WindowedValueCoder(coder)
      # TODO(gildea):
      # Actually compute the encoded size of this value:
      #     coder.store_estimated_size(windowed_value, byte_size_accumulator)

  def update_collect(self):
    """Collects the accumulated size estimates.

    Now that the element has been processed, we ask our accumulator
    for the total and store the result in a counter.
    """
    for pending in self._active_accumulators:
      self.mean_byte_counter.update(pending.value())
    self._active_accumulators = []

  def should_sample(self):
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
    def compute_next_sample(i):
      # https://en.wikipedia.org/wiki/Reservoir_sampling#Fast_Approximation
      gap = math.log(1.0 - random.random()) / math.log(1.0 - 10.0/i)
      return i + math.floor(gap)

    self._sample_counter += 1
    if self._next_sample == 0:
      if random.randint(1, self._sample_counter) <= 10:
        if self._sample_counter > 30:
          self._next_sample = compute_next_sample(self._sample_counter)
        return True
      return False
    elif self._sample_counter >= self._next_sample:
      self._next_sample = compute_next_sample(self._sample_counter)
      return True
    return False

  def __str__(self):
    return '<%s [%s]>' % (self.__class__.__name__,
                          ', '.join([str(x) for x in self.__iter__()]))

  def __repr__(self):
    return '<%s %s at %s>' % (self.__class__.__name__,
                              [x for x in self.__iter__()], hex(id(self)))
