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

# pytype: skip-file

"""a collection of ptransforms for deduplicating elements."""

import typing

from apache_beam import typehints
from apache_beam.coders.coders import BooleanCoder
from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.transforms import userstate
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils import timestamp

__all__ = [
    'Deduplicate',
    'DeduplicatePerKey',
]

K = typing.TypeVar('K')
V = typing.TypeVar('V')


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, V])
class DeduplicatePerKey(ptransform.PTransform):
  """ A PTransform which deduplicates <key, value> pair over a time domain and
  threshold. Values in different windows will NOT be considered duplicates of
  each other. Deduplication is guaranteed with respect of time domain and
  duration.

  Time durations are required so as to avoid unbounded memory and/or storage
  requirements within a runner and care might need to be used to ensure that the
  deduplication time limit is long enough to remove duplicates but short enough
  to not cause performance problems within a runner. Each runner may provide an
  optimized implementation of their choice using the deduplication time domain
  and threshold specified.

  Does not preserve any order the input PCollection might have had.
  """
  def __init__(self, processing_time_duration=None, event_time_duration=None):
    if processing_time_duration is None and event_time_duration is None:
      raise ValueError(
          'DeduplicatePerKey requires at lease provide either'
          'processing_time_duration or event_time_duration.')
    self.processing_time_duration = processing_time_duration
    self.event_time_duration = event_time_duration

  def _create_deduplicate_fn(self):
    processing_timer_spec = userstate.TimerSpec(
        'processing_timer', TimeDomain.REAL_TIME)
    event_timer_spec = userstate.TimerSpec('event_timer', TimeDomain.WATERMARK)
    state_spec = userstate.BagStateSpec('seen', BooleanCoder())
    processing_time_duration = self.processing_time_duration
    event_time_duration = self.event_time_duration

    class DeduplicationFn(core.DoFn):
      def process(
          self,
          kv,
          ts=core.DoFn.TimestampParam,
          seen_state=core.DoFn.StateParam(state_spec),
          processing_timer=core.DoFn.TimerParam(processing_timer_spec),
          event_timer=core.DoFn.TimerParam(event_timer_spec)):
        if True in seen_state.read():
          return

        if processing_time_duration is not None:
          processing_timer.set(
              timestamp.Timestamp.now() + processing_time_duration)
        if event_time_duration is not None:
          event_timer.set(ts + event_time_duration)
        seen_state.add(True)
        yield kv

      @userstate.on_timer(processing_timer_spec)
      def process_processing_timer(
          self, seen_state=core.DoFn.StateParam(state_spec)):
        seen_state.clear()

      @userstate.on_timer(event_timer_spec)
      def process_event_timer(
          self, seen_state=core.DoFn.StateParam(state_spec)):
        seen_state.clear()

    return DeduplicationFn()

  def expand(self, pcoll):
    return (
        pcoll
        | 'DeduplicateFn' >> core.ParDo(self._create_deduplicate_fn()))


class Deduplicate(ptransform.PTransform):
  """Similar to DeduplicatePerKey, the Deduplicate transform takes any arbitray
  value as input and uses value as key to deduplicate among certain amount of
  time duration.
  """
  def __init__(self, processing_time_duration=None, event_time_duration=None):
    if processing_time_duration is None and event_time_duration is None:
      raise ValueError(
          'Deduplicate requires at lease provide either'
          'processing_time_duration or event_time_duration.')
    self.processing_time_duration = processing_time_duration
    self.event_time_duration = event_time_duration

  def expand(self, pcoll):
    return (
        pcoll
        | 'Use Value as Key' >> core.Map(lambda x: (x, None))
        | 'DeduplicatePerKey' >> DeduplicatePerKey(
            processing_time_duration=self.processing_time_duration,
            event_time_duration=self.event_time_duration)
        | 'Output Value' >> core.Map(lambda kv: kv[0]))
