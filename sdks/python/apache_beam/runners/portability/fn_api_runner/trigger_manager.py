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
import logging

import typing
from collections import defaultdict

from apache_beam import typehints
from apache_beam.coders import PickleCoder
from apache_beam.coders import TupleCoder
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders.coders import IntervalWindowCoder
from apache_beam.transforms import DoFn
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import TriggerContext
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import RuntimeState
from apache_beam.transforms.userstate import RuntimeTimer
from apache_beam.transforms.userstate import SetStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import BoundedWindow
from apache_beam.transforms.window import GlobalWindow
from apache_beam.typehints import TypeCheckError
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MIN_TIMESTAMP

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

K = typing.TypeVar('K')
V = typing.TypeVar('V')


class ReifyWindows(DoFn):
  """Receives KV pairs, and wraps the values into WindowedValues."""
  def process(
      self, element, window=DoFn.WindowParam, timestamp=DoFn.TimestampParam):
    try:
      k, v = element
    except TypeError:
      raise TypeCheckError(
          'Input to GroupByKey must be a PCollection with '
          'elements compatible with KV[A, B]')

    yield (k, windowed_value.WindowedValue(v, timestamp, [window]))


class _GroupBundlesByKey(DoFn):
  def start_bundle(self):
    self.keys = defaultdict(list)

  def process(self, element):
    key, windowed_value = element
    self.keys[key].append(windowed_value)

  def finish_bundle(self):
    for k, vals in self.keys.items():
      yield windowed_value.WindowedValue((k, vals),
                                         MIN_TIMESTAMP, [GlobalWindow()])


@typehints.with_input_types(
    typing.Tuple[K, typing.Iterable[windowed_value.WindowedValue]])
@typehints.with_output_types(
    typing.Tuple[K, typing.Iterable[windowed_value.WindowedValue]])
class GeneralTriggerManagerDoFn(DoFn):
  """Logic for triggering."""

  KNOWN_WINDOWS = SetStateSpec('windows', IntervalWindowCoder())
  LAST_KNOWN_TIME = CombiningValueStateSpec('last_known_time', combine_fn=max)
  LAST_KNOWN_WATERMARK = CombiningValueStateSpec(
      'last_known_watermark', combine_fn=max)

  # TODO(pabloem) WHat's the coder for the elements/keys here?
  WINDOW_ELEMENT_PAIRS = BagStateSpec(
      'element_bag',
      TupleCoder([IntervalWindowCoder(), WindowedValueCoder(PickleCoder())]))

  PROCESSING_TIME_TIMER = TimerSpec(
      'processing_time_timer', TimeDomain.REAL_TIME)
  WATERMARK_TIMER = TimerSpec('watermark_timer', TimeDomain.WATERMARK)

  def __init__(self, windowing: Windowing):
    self.windowing = windowing

  def process(
      self,
      element: typing.Tuple[K, typing.Iterable[windowed_value.WindowedValue]],
      element_bag=DoFn.StateParam(WINDOW_ELEMENT_PAIRS),
      time_state=DoFn.StateParam(LAST_KNOWN_TIME),
      watermark_state=DoFn.StateParam(LAST_KNOWN_WATERMARK),
      processing_time_timer=DoFn.TimerParam(PROCESSING_TIME_TIMER),
      watermark_timer=DoFn.TimerParam(WATERMARK_TIMER)):
    context = FnRunnerTriggerContext(
        processing_time_timer=processing_time_timer,
        watermark_timer=watermark_timer,
        current_time_state=time_state,
        watermark_state=watermark_state,
        elements_bag_state=element_bag)
    _, windowed_values = element

    for wv in windowed_values:
      for w in wv.windows:
        _LOGGER.debug(wv)
        element_bag.add((w, wv))
        self.windowing.triggerfn.on_element(windowed_values, w, context)

  def _trigger_fire(
      self, key: K, element_bag, time_domain, timestamp, timer_tag, context):
    windows_to_elements: typing.Dict[
        BoundedWindow, typing.
        List[windowed_value.WindowedValue]] = self._build_windows_to_elements(
            element_bag)
    element_bag.clear()

    fired_windows = set()
    _LOGGER.debug(
        '%s - tag %s - timestamp %s', time_domain, timer_tag, timestamp)
    for w, elems in windows_to_elements.items():
      if self.windowing.triggerfn.should_fire(time_domain,
                                              timestamp,
                                              w,
                                              context):
        self.windowing.triggerfn.on_fire(timestamp, w, context)
        fired_windows.add(w)
        # TODO(pabloem): Format the output: e.g. pane info
        yield (key, elems)

    # Add elements that were not fired back into state.
    for w, elems in windows_to_elements.items():
      for e in elems:
        if (w in fired_windows and
            self.windowing.accumulation_mode == AccumulationMode.DISCARDING):
          continue
        element_bag.add((w, e))

  def _build_windows_to_elements(
      self, element_bag_state
  ) -> typing.Dict[BoundedWindow, typing.List[windowed_value.WindowedValue]]:
    window_element_pairs: typing.Iterable[
        typing.Tuple[BoundedWindow,
                     windowed_value.WindowedValue]] = element_bag_state.read()
    result = {}
    for w, e in window_element_pairs:
      if w not in result:
        result[w] = []
      result[w].append(e)
    return result

  @on_timer(PROCESSING_TIME_TIMER)
  def processing_time_trigger(
      self,
      key=DoFn.KeyParam,
      timer_tag=DoFn.DynamicTimerTagParam,
      timestamp=DoFn.TimestampParam,
      element_bag=DoFn.StateParam(WINDOW_ELEMENT_PAIRS),
      processing_time_timer=DoFn.TimerParam(PROCESSING_TIME_TIMER),
      watermark_timer=DoFn.TimerParam(WATERMARK_TIMER)):
    context = FnRunnerTriggerContext(
        processing_time_timer=processing_time_timer,
        watermark_timer=watermark_timer,
        current_time_state=None,
        watermark_state=None,
        elements_bag_state=element_bag)
    return self._trigger_fire(
        key, element_bag, TimeDomain.REAL_TIME, timestamp, timer_tag, context)

  @on_timer(WATERMARK_TIMER)
  def watermark_trigger(
      self,
      key=DoFn.KeyParam,
      timer_tag=DoFn.DynamicTimerTagParam,
      timestamp=DoFn.TimestampParam,
      element_bag=DoFn.StateParam(WINDOW_ELEMENT_PAIRS),
      processing_time_timer=DoFn.TimerParam(PROCESSING_TIME_TIMER),
      watermark_timer=DoFn.TimerParam(WATERMARK_TIMER)):
    context = FnRunnerTriggerContext(
        processing_time_timer=processing_time_timer,
        watermark_timer=watermark_timer,
        current_time_state=None,
        watermark_state=None,
        elements_bag_state=element_bag)
    return self._trigger_fire(
        key, element_bag, TimeDomain.WATERMARK, timestamp, timer_tag, context)


class FnRunnerTriggerContext(TriggerContext):
  def __init__(
      self,
      processing_time_timer: RuntimeTimer,
      watermark_timer: RuntimeTimer,
      current_time_state: RuntimeState,
      watermark_state: RuntimeState,
      elements_bag_state: RuntimeState):
    self.timers = {
        TimeDomain.REAL_TIME: processing_time_timer,
        TimeDomain.WATERMARK: watermark_timer
    }
    self.current_times = {
        TimeDomain.REAL_TIME: current_time_state,
        TimeDomain.WATERMARK: watermark_state
    }

  def get_current_time(self):
    return self.current_times[TimeDomain.REAL_TIME].read()

  def set_timer(self, name, time_domain, timestamp):
    _LOGGER.debug('Setting timer (%s, %s) at %s', time_domain, name, timestamp)
    self.timers[time_domain].set(timestamp, dynamic_timer_tag=name)

  def clear_timer(self, name, time_domain):
    _LOGGER.debug('Clearing timer (%s, %s)', time_domain, name)
    self.timers[time_domain].clear(dynamic_timer_tag=name)

  def add_state(self, tag, value):
    # TODO
    raise NotImplementedError('unimplemented')

  def get_state(self, tag):
    # TODO
    raise NotImplementedError('unimplemented')

  def clear_state(self, tag):
    # TODO
    raise NotImplementedError('unimplemented')
