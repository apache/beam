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
from apache_beam.transforms.timeutil import MAX_TIMESTAMP
from apache_beam.transforms.timeutil import MIN_TIMESTAMP


class InProcessWatermarkManager(object):
  """Tracks and updates watermarks for all AppliedPTransforms."""

  WATERMARK_POS_INF = MAX_TIMESTAMP
  WATERMARK_NEG_INF = MIN_TIMESTAMP

  def __init__(self, clock, root_transforms, value_to_consumers):
    self._clock = clock  # processing time clock
    self._value_to_consumers = value_to_consumers
    self._root_transforms = root_transforms
    # AppliedPTransform -> TransformWatermarks
    self._transform_to_watermarks = {}

    for root_transform in root_transforms:
      self._transform_to_watermarks[root_transform] = TransformWatermarks(
          self._clock)

    for consumers in value_to_consumers.values():
      for consumer in consumers:
        self._transform_to_watermarks[consumer] = TransformWatermarks(
            self._clock)

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

  def update_watermarks(self, completed_committed_bundle, applied_ptransform,
                        timer_update, outputs, earliest_hold):
    assert isinstance(applied_ptransform, pipeline.AppliedPTransform)
    self._update_pending(
        completed_committed_bundle, applied_ptransform, timer_update, outputs)
    tw = self.get_watermarks(applied_ptransform)
    tw.hold(earliest_hold)
    self._refresh_watermarks(applied_ptransform)

  def _update_pending(self, input_committed_bundle, applied_ptransform,
                      timer_update, output_committed_bundles):
    """Updated list of pending bundles for the given AppliedPTransform."""

    # Update pending elements. Filter out empty bundles. They do not impact
    # watermarks and should not trigger downstream execution.
    for output in output_committed_bundles:
      if output.elements:
        if output.pcollection in self._value_to_consumers:
          consumers = self._value_to_consumers[output.pcollection]
          for consumer in consumers:
            consumer_tw = self._transform_to_watermarks[consumer]
            consumer_tw.add_pending(output)

    completed_tw = self._transform_to_watermarks[applied_ptransform]
    completed_tw.update_timers(timer_update)

    assert input_committed_bundle or applied_ptransform in self._root_transforms
    if input_committed_bundle and input_committed_bundle.elements:
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
      if tw.extract_fired_timers():
        all_timers.append(applied_ptransform)
    return all_timers


class TransformWatermarks(object):
  """Tracks input and output watermarks for aan AppliedPTransform."""

  def __init__(self, clock):
    self._clock = clock
    self._input_transform_watermarks = []
    self._input_watermark = InProcessWatermarkManager.WATERMARK_NEG_INF
    self._output_watermark = InProcessWatermarkManager.WATERMARK_NEG_INF
    self._earliest_hold = InProcessWatermarkManager.WATERMARK_POS_INF
    self._pending = set()  # Scheduled bundles targeted for this transform.
    self._fired_timers = False
    self._lock = threading.Lock()

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
        value = InProcessWatermarkManager.WATERMARK_POS_INF
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
    with self._lock:
      pending_holder = (InProcessWatermarkManager.WATERMARK_NEG_INF
                        if self._pending else
                        InProcessWatermarkManager.WATERMARK_POS_INF)

      input_watermarks = [
          tw.output_watermark for tw in self._input_transform_watermarks]
      input_watermarks.append(InProcessWatermarkManager.WATERMARK_POS_INF)
      producer_watermark = min(input_watermarks)

      self._input_watermark = max(self._input_watermark,
                                  min(pending_holder, producer_watermark))
      new_output_watermark = min(self._input_watermark, self._earliest_hold)

      advanced = new_output_watermark > self._output_watermark
      self._output_watermark = new_output_watermark
      return advanced

  @property
  def synchronized_processing_output_time(self):
    return self._clock.now

  def extract_fired_timers(self):
    with self._lock:
      if self._fired_timers:
        return  False

      should_fire = (
          self._earliest_hold < InProcessWatermarkManager.WATERMARK_POS_INF and
          self._input_watermark == InProcessWatermarkManager.WATERMARK_POS_INF)
      self._fired_timers = should_fire
      return should_fire
