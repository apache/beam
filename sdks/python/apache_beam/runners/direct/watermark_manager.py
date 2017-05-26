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

"""Manages watermarks of PCollections and AppliedPTransforms."""

from __future__ import absolute_import

import threading

from apache_beam import pipeline
from apache_beam import pvalue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP


class WatermarkManager(object):
  """For internal use only; no backwards-compatibility guarantees.

  Tracks and updates watermarks for all AppliedPTransforms."""

  WATERMARK_POS_INF = MAX_TIMESTAMP
  WATERMARK_NEG_INF = MIN_TIMESTAMP

  def __init__(self, clock, root_transforms, value_to_consumers, transform_keyed_states):
    self._clock = clock  # processing time clock
    self._value_to_consumers = value_to_consumers
    self._root_transforms = root_transforms
    self._transform_keyed_states = transform_keyed_states
    # AppliedPTransform -> TransformWatermarks
    self._transform_to_watermarks = {}

    for root_transform in root_transforms:
      keyed_states = self._transform_keyed_states[root_transform]
      self._transform_to_watermarks[root_transform] = _TransformWatermarks(
          self._clock, keyed_states, label=str(root_transform))

    for consumers in value_to_consumers.values():
      for consumer in consumers:
        keyed_states = self._transform_keyed_states[consumer]
        self._transform_to_watermarks[consumer] = _TransformWatermarks(
            self._clock, keyed_states, label=str(consumer))

    for consumers in value_to_consumers.values():
      for consumer in consumers:
        self._update_input_transform_watermarks(consumer)

  def _update_input_transform_watermarks(self, applied_ptransform):
    assert isinstance(applied_ptransform, pipeline.AppliedPTransform)
    input_transform_watermarks = []
    for input_pvalue in applied_ptransform.inputs:
      assert input_pvalue.producer or isinstance(input_pvalue, pvalue.PBegin)
      if input_pvalue.producer:
        input_transform_watermarks.append(
            self.get_watermarks(input_pvalue.producer))
    self._transform_to_watermarks[
        applied_ptransform].update_input_transform_watermarks(
            input_transform_watermarks)

  def get_watermarks(self, applied_ptransform):
    """Gets the input and output watermarks for an AppliedPTransform.

    If the applied_ptransform has not processed any elements, return a
    watermark with minimum value.

    Args:
      applied_ptransform: AppliedPTransform to get the watermarks for.

    Returns:
      A snapshot (TransformWatermarks) of the input watermark and output
      watermark for the provided transform.
    """

    # TODO(altay): Composite transforms should have a composite watermark. Until
    # then they are represented by their last transform.
    while applied_ptransform.parts:
      applied_ptransform = applied_ptransform.parts[-1]

    return self._transform_to_watermarks[applied_ptransform]

  def update_watermarks(self, completed_committed_bundle, unprocessed_bundle,
                        applied_ptransform,
                        timer_update, outputs, earliest_hold):
    assert isinstance(applied_ptransform, pipeline.AppliedPTransform)
    self._update_pending(
        completed_committed_bundle, unprocessed_bundle, applied_ptransform,
        timer_update, outputs)
    tw = self.get_watermarks(applied_ptransform)
    tw.hold(earliest_hold)
    self._refresh_watermarks(applied_ptransform)

  def _update_pending(self, input_committed_bundle, unprocessed_bundle,
                      applied_ptransform,
                      timer_update, output_committed_bundles):
    """Updated list of pending bundles for the given AppliedPTransform."""

    # Update pending elements. Filter out empty bundles. They do not impact
    # watermarks and should not trigger downstream execution.
    for output in output_committed_bundles:
      if output.has_elements():
        if output.pcollection in self._value_to_consumers:
          consumers = self._value_to_consumers[output.pcollection]
          for consumer in consumers:
            consumer_tw = self._transform_to_watermarks[consumer]
            consumer_tw.add_pending(output)

    completed_tw = self._transform_to_watermarks[applied_ptransform]
    completed_tw.update_timers(timer_update)

    if unprocessed_bundle:
      completed_tw.add_pending(unprocessed_bundle)

    assert input_committed_bundle or applied_ptransform in self._root_transforms
    if input_committed_bundle and input_committed_bundle.has_elements():
      completed_tw.remove_pending(input_committed_bundle)

  def _refresh_watermarks(self, applied_ptransform):
    assert isinstance(applied_ptransform, pipeline.AppliedPTransform)
    tw = self.get_watermarks(applied_ptransform)
    if tw.refresh():
      for pval in applied_ptransform.outputs.values():
        if isinstance(pval, pvalue.DoOutputsTuple):
          pvals = (v for v in pval)
        else:
          pvals = (pval,)
        for v in pvals:
          if v in self._value_to_consumers:  # If there are downstream consumers
            consumers = self._value_to_consumers[v]
            for consumer in consumers:
              self._refresh_watermarks(consumer)

  def extract_fired_timers(self):
    all_timers = []
    for applied_ptransform, tw in self._transform_to_watermarks.iteritems():
      fired_timers = tw.extract_fired_timers()
      if fired_timers:
        all_timers.append((applied_ptransform, fired_timers))
    return all_timers


class _TransformWatermarks(object):
  """Tracks input and output watermarks for aan AppliedPTransform."""

  def __init__(self, clock, keyed_states, label=None):
    self._clock = clock
    self._keyed_states = keyed_states
    self._input_transform_watermarks = []
    self._input_watermark = WatermarkManager.WATERMARK_NEG_INF
    self._output_watermark = WatermarkManager.WATERMARK_NEG_INF
    self._earliest_hold = WatermarkManager.WATERMARK_POS_INF
    self._pending = set()  # Scheduled bundles targeted for this transform.
    self._fired_timers = False
    self._lock = threading.Lock()

    # TODO(ccy): remove debug label
    self._label = label

  def update_input_transform_watermarks(self, input_transform_watermarks):
    with self._lock:
      self._input_transform_watermarks = input_transform_watermarks

  def update_timers(self, timer_update):
    with self._lock:
      if timer_update:
        assert self._fired_timers
        self._fired_timers = False

  @property
  def input_watermark(self):
    with self._lock:
      return self._input_watermark

  @property
  def output_watermark(self):
    with self._lock:
      return self._output_watermark

  def hold(self, value):
    with self._lock:
      if value is None:
        value = WatermarkManager.WATERMARK_POS_INF
      self._earliest_hold = value

  def add_pending(self, pending):
    with self._lock:
      self._pending.add(pending)

  def remove_pending(self, completed):
    with self._lock:
      # Ignore repeated removes. This will happen if a transform has a repeated
      # input.
      if completed in self._pending:
        self._pending.remove(completed)

  def refresh(self):
    # TODO: remove this and the below assert
    from apache_beam.runners.direct.evaluation_context import DirectUnmergedState
    with self._lock:
      pending_holder = WatermarkManager.WATERMARK_POS_INF
      for input_bundle in self._pending:
        # TODO: Perhaps we can have the Bundle class keep track of the minimum
        # timestamp so we don't have to do an iteration here.
        bundle_min_timestamp = min(wv.timestamp for wv in input_bundle.get_elements_iterable())
        if bundle_min_timestamp < pending_holder:
          pending_holder = bundle_min_timestamp

      earliest_watermark_hold = WatermarkManager.WATERMARK_POS_INF
      for unused_key, state in self._keyed_states.iteritems():
        assert isinstance(state, DirectUnmergedState), state
        print '~~~~~~~~WHSTATE [key=', unused_key, ']', state
        earliest_watermark_hold = state.get_earliest_hold()
        print 'holds [current watermark =', self._input_watermark, ']:', state.get_earliest_hold()


      input_watermarks = [
          tw.output_watermark for tw in self._input_transform_watermarks]
      input_watermarks.append(WatermarkManager.WATERMARK_POS_INF)
      producer_watermark = min(input_watermarks)

      self._input_watermark = max(self._input_watermark,
                                  min(pending_holder, producer_watermark, earliest_watermark_hold))
      new_output_watermark = min(self._input_watermark, self._earliest_hold)

      advanced = new_output_watermark > self._output_watermark
      if advanced:
        print '[!] Watermark for', self._label, 'advanced', new_output_watermark, 'from', self._output_watermark, '(pending_holder:', pending_holder
      self._output_watermark = new_output_watermark
      return advanced

  @property
  def synchronized_processing_output_time(self):
    return self._clock.time()

  def extract_fired_timers(self):
    # TODO: remove this and the below assert
    from apache_beam.runners.direct.evaluation_context import DirectUnmergedState
    with self._lock:
      if self._fired_timers:
        print 'FIRED_TIMERS? FALSE', self
        # Wait until fired timers have been processed.
        # return False

      fired_timers = []

      for key, state in self._keyed_states.iteritems():
        assert isinstance(state, DirectUnmergedState), state
        print '~~~~~~~~STATE [key=', key, ']', state
        timers = state.get_timers(watermark=self._input_watermark)
        print 'timers:', timers
        for expired in timers:
          _, (name, time_domain, timestamp) = expired
          fired_timers.append(TimerFiring(key, time_domain, timestamp))


      # TODO: check if this hack is still necessary.
      should_fire = (
          self._earliest_hold < WatermarkManager.WATERMARK_POS_INF and
          self._input_watermark == WatermarkManager.WATERMARK_POS_INF)
      if should_fire:
        fired_timers.append(None)  # Sentinel representing legacy timer firing.
      self._fired_timers = fired_timers
      return fired_timers

class TimerFiring(object):
  def __init__(self, key, time_domain, timestamp):
    # TODO: add time domain.
    self.key = key
    self.time_domain = time_domain
    self.timestamp = timestamp

