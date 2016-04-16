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

from google.cloud.dataflow.utils.counters import Accumulator
from google.cloud.dataflow.utils.counters import Counter


class OperationCounters(object):
  """The set of basic counters to attach to an Operation."""

  def __init__(self, counter_factory, step_name, coder, output_index):
    self.element_counter = counter_factory.get_counter(
        '%s-out%d-ElementCount' % (step_name, output_index), Counter.SUM)
    self.mean_byte_counter = counter_factory.get_counter(
        '%s-out%d-MeanByteCount' % (step_name, output_index), Counter.MEAN)
    self.coder = coder
    self._active_accumulators = []

  def update_from(self, windowed_value, coder=None):
    """Add one value to this counter."""
    self.element_counter.update(1)
    byte_size_accumulator = Accumulator(self.mean_byte_counter.name)
    self._active_accumulators.append(byte_size_accumulator)
    # TODO(gildea):
    # Actually compute the encoded size of this value.
    # In spirit, something like this:
    #     if coder is None:
    #       coder = self.coder
    #     coder.store_estimated_size(windowed_value, byte_size_accumulator)
    # but will need to do sampling.

  def update_collect(self):
    """Collects the accumulated size estimates.

    Now that the element has been processed, we ask our accumulator
    for the total and store the result in a counter.
    """
    for pending in self._active_accumulators:
      self.mean_byte_counter.update(pending.total)
    self._active_accumulators = []

  def __str__(self):
    return '<%s [%s]>' % (self.__class__.__name__,
                          ', '.join([str(x) for x in self.__iter__()]))

  def __repr__(self):
    return '<%s %s at %s>' % (self.__class__.__name__,
                              [x for x in self.__iter__()], hex(id(self)))
