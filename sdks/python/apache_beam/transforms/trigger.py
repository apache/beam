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

"""Support for Apache Beam triggers.

Triggers control when in processing time windows get emitted.
"""

# pytype: skip-file

import collections
import copy
import logging
import numbers
from abc import ABCMeta
from abc import abstractmethod
from collections import abc as collections_abc  # ambiguty with direct abc
from enum import Flag
from enum import auto
from itertools import zip_longest

from apache_beam.coders import coder_impl
from apache_beam.coders import observable
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import TIME_GRANULARITY

# AfterCount is experimental. No backwards compatibility guarantees.

__all__ = [
    'AccumulationMode',
    'TriggerFn',
    'DefaultTrigger',
    'AfterWatermark',
    'AfterProcessingTime',
    'AfterCount',
    'Repeatedly',
    'AfterAny',
    'AfterAll',
    'AfterEach',
    'OrFinally',
]

_LOGGER = logging.getLogger(__name__)


class AccumulationMode(object):
  """Controls what to do with data when a trigger fires multiple times."""
  DISCARDING = beam_runner_api_pb2.AccumulationMode.DISCARDING
  ACCUMULATING = beam_runner_api_pb2.AccumulationMode.ACCUMULATING
  # TODO(robertwb): Provide retractions of previous outputs.
  # RETRACTING = 3


class _StateTag(metaclass=ABCMeta):
  """An identifier used to store and retrieve typed, combinable state.

  The given tag must be unique for this step."""
  def __init__(self, tag):
    self.tag = tag


class _ReadModifyWriteStateTag(_StateTag):
  """StateTag pointing to an element."""
  def __repr__(self):
    return 'ValueStateTag(%s)' % (self.tag)

  def with_prefix(self, prefix):
    return _ReadModifyWriteStateTag(prefix + self.tag)


class _SetStateTag(_StateTag):
  """StateTag pointing to an element."""
  def __repr__(self):
    return 'SetStateTag({tag})'.format(tag=self.tag)

  def with_prefix(self, prefix):
    return _SetStateTag(prefix + self.tag)


class _CombiningValueStateTag(_StateTag):
  """StateTag pointing to an element, accumulated with a combiner.

  The given tag must be unique for this step. The given CombineFn will be
  applied (possibly incrementally and eagerly) when adding elements."""

  # TODO(robertwb): Also store the coder (perhaps extracted from the combine_fn)
  def __init__(self, tag, combine_fn):
    super().__init__(tag)
    if not combine_fn:
      raise ValueError('combine_fn must be specified.')
    if not isinstance(combine_fn, core.CombineFn):
      combine_fn = core.CombineFn.from_callable(combine_fn)
    self.combine_fn = combine_fn

  def __repr__(self):
    return 'CombiningValueStateTag(%s, %s)' % (self.tag, self.combine_fn)

  def with_prefix(self, prefix):
    return _CombiningValueStateTag(prefix + self.tag, self.combine_fn)

  def without_extraction(self):
    class NoExtractionCombineFn(core.CombineFn):
      setup = self.combine_fn.setup
      create_accumulator = self.combine_fn.create_accumulator
      add_input = self.combine_fn.add_input
      merge_accumulators = self.combine_fn.merge_accumulators
      compact = self.combine_fn.compact
      extract_output = staticmethod(lambda x: x)
      teardown = self.combine_fn.teardown

    return _CombiningValueStateTag(self.tag, NoExtractionCombineFn())


class _ListStateTag(_StateTag):
  """StateTag pointing to a list of elements."""
  def __repr__(self):
    return 'ListStateTag(%s)' % self.tag

  def with_prefix(self, prefix):
    return _ListStateTag(prefix + self.tag)


class _WatermarkHoldStateTag(_StateTag):
  def __init__(self, tag, timestamp_combiner_impl):
    super().__init__(tag)
    self.timestamp_combiner_impl = timestamp_combiner_impl

  def __repr__(self):
    return 'WatermarkHoldStateTag(%s, %s)' % (
        self.tag, self.timestamp_combiner_impl)

  def with_prefix(self, prefix):
    return _WatermarkHoldStateTag(
        prefix + self.tag, self.timestamp_combiner_impl)


class DataLossReason(Flag):
  """Enum defining potential reasons that a trigger may cause data loss.

  These flags should only cover when the trigger is the cause, though windowing
  can be taken into account. For instance, AfterWatermark may not flag itself
  as finishing if the windowing doesn't allow lateness.
  """

  # Trigger will never be the source of data loss.
  NO_POTENTIAL_LOSS = 0

  # Trigger may finish. In this case, data that comes in after the trigger may
  # be lost. Example: AfterCount(1) will stop firing after the first element.
  MAY_FINISH = auto()

  # Deprecated: Beam will emit buffered data at GC time. Any other behavior
  # should be treated as a bug with the runner used.
  CONDITION_NOT_GUARANTEED = auto()


# Convenience functions for checking if a flag is included. Each is equivalent
# to `reason & flag == flag`


def _IncludesMayFinish(reason):
  # type: (DataLossReason) -> bool
  return reason & DataLossReason.MAY_FINISH == DataLossReason.MAY_FINISH


# pylint: disable=unused-argument
# TODO(robertwb): Provisional API, Java likely to change as well.
class TriggerFn(metaclass=ABCMeta):
  """A TriggerFn determines when window (panes) are emitted.

  See https://beam.apache.org/documentation/programming-guide/#triggers
  """
  @abstractmethod
  def on_element(self, element, window, context):
    """Called when a new element arrives in a window.

    Args:
      element: the element being added
      window: the window to which the element is being added
      context: a context (e.g. a TriggerContext instance) for managing state
          and setting timers
    """
    pass

  @abstractmethod
  def on_merge(self, to_be_merged, merge_result, context):
    """Called when multiple windows are merged.

    Args:
      to_be_merged: the set of windows to be merged
      merge_result: the window into which the windows are being merged
      context: a context (e.g. a TriggerContext instance) for managing state
          and setting timers
    """
    pass

  @abstractmethod
  def should_fire(self, time_domain, timestamp, window, context):
    """Whether this trigger should cause the window to fire.

    Args:
      time_domain: WATERMARK for event-time timers and REAL_TIME for
          processing-time timers.
      timestamp: for time_domain WATERMARK, it represents the
          watermark: (a lower bound on) the watermark of the system
          and for time_domain REAL_TIME, it represents the
          trigger: timestamp of the processing-time timer.
      window: the window whose trigger is being considered
      context: a context (e.g. a TriggerContext instance) for managing state
          and setting timers

    Returns:
      whether this trigger should cause a firing
    """
    pass

  @abstractmethod
  def has_ontime_pane(self):
    """Whether this trigger creates an empty pane even if there are no elements.

    Returns:
      True if this trigger guarantees that there will always be an ON_TIME pane
      even if there are no elements in that pane.
    """
    pass

  @abstractmethod
  def on_fire(self, watermark, window, context):
    """Called when a trigger actually fires.

    Args:
      watermark: (a lower bound on) the watermark of the system
      window: the window whose trigger is being fired
      context: a context (e.g. a TriggerContext instance) for managing state
          and setting timers

    Returns:
      whether this trigger is finished
    """
    pass

  @abstractmethod
  def reset(self, window, context):
    """Clear any state and timers used by this TriggerFn."""
    pass

  def may_lose_data(self, unused_windowing):
    # type: (core.Windowing) -> DataLossReason

    """Returns whether or not this trigger could cause data loss.

    A trigger can cause data loss in the following scenarios:

        * The trigger has a chance to finish. For instance, AfterWatermark()
          without a late trigger would cause all late data to be lost. This
          scenario is only accounted for if the windowing strategy allows
          late data. Otherwise, the trigger is not responsible for the data
          loss.

    Note that this only returns the potential for loss. It does not mean that
    there will be data loss. It also only accounts for loss related to the
    trigger, not other potential causes.

    Args:
      windowing: The Windowing that this trigger belongs to. It does not need
        to be the top-level trigger.

    Returns:
      The DataLossReason. If there is no potential loss,
        DataLossReason.NO_POTENTIAL_LOSS is returned. Otherwise, all the
        potential reasons are returned as a single value.
    """
    # For backwards compatibility's sake, we're assuming the trigger is safe.
    return DataLossReason.NO_POTENTIAL_LOSS


# pylint: enable=unused-argument

  @staticmethod
  def from_runner_api(proto, context):
    return {
        'after_all': AfterAll,
        'after_any': AfterAny,
        'after_each': AfterEach,
        'after_end_of_window': AfterWatermark,
        'after_processing_time': AfterProcessingTime,
        # after_processing_time, after_synchronized_processing_time
        'always': Always,
        'default': DefaultTrigger,
        'element_count': AfterCount,
        'never': _Never,
        'or_finally': OrFinally,
        'repeat': Repeatedly,
    }[proto.WhichOneof('trigger')].from_runner_api(proto, context)

  @abstractmethod
  def to_runner_api(self, unused_context):
    pass


class DefaultTrigger(TriggerFn):
  """Semantically Repeatedly(AfterWatermark()), but more optimized."""
  def __init__(self):
    pass

  def __repr__(self):
    return 'DefaultTrigger()'

  def on_element(self, element, window, context):
    context.set_timer(str(window), TimeDomain.WATERMARK, window.end)

  def on_merge(self, to_be_merged, merge_result, context):
    for window in to_be_merged:
      context.clear_timer(str(window), TimeDomain.WATERMARK)

  def should_fire(self, time_domain, watermark, window, context):
    if watermark >= window.end:
      # Explicitly clear the timer so that late elements are not emitted again
      # when the timer is fired.
      context.clear_timer(str(window), TimeDomain.WATERMARK)
    return watermark >= window.end

  def on_fire(self, watermark, window, context):
    return False

  def reset(self, window, context):
    context.clear_timer(str(window), TimeDomain.WATERMARK)

  def may_lose_data(self, unused_windowing):
    return DataLossReason.NO_POTENTIAL_LOSS

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))

  @staticmethod
  def from_runner_api(proto, context):
    return DefaultTrigger()

  def to_runner_api(self, unused_context):
    return beam_runner_api_pb2.Trigger(
        default=beam_runner_api_pb2.Trigger.Default())

  def has_ontime_pane(self):
    return True


class AfterProcessingTime(TriggerFn):
  """Fire exactly once after a specified delay from processing time.

  AfterProcessingTime is experimental. No backwards compatibility guarantees.
  """
  def __init__(self, delay=0):
    """Initialize a processing time trigger with a delay in seconds."""
    self.delay = delay

  def __repr__(self):
    return 'AfterProcessingTime(delay=%d)' % self.delay

  def on_element(self, element, window, context):
    context.set_timer(
        '', TimeDomain.REAL_TIME, context.get_current_time() + self.delay)

  def on_merge(self, to_be_merged, merge_result, context):
    # timers will be kept through merging
    pass

  def should_fire(self, time_domain, timestamp, window, context):
    if time_domain == TimeDomain.REAL_TIME:
      return True

  def on_fire(self, timestamp, window, context):
    return True

  def reset(self, window, context):
    pass

  def may_lose_data(self, unused_windowing):
    """AfterProcessingTime may finish."""
    return DataLossReason.MAY_FINISH

  @staticmethod
  def from_runner_api(proto, context):
    return AfterProcessingTime(
        delay=(
            proto.after_processing_time.timestamp_transforms[0].delay.
            delay_millis) // 1000)

  def to_runner_api(self, context):
    delay_proto = beam_runner_api_pb2.TimestampTransform(
        delay=beam_runner_api_pb2.TimestampTransform.Delay(
            delay_millis=self.delay * 1000))
    return beam_runner_api_pb2.Trigger(
        after_processing_time=beam_runner_api_pb2.Trigger.AfterProcessingTime(
            timestamp_transforms=[delay_proto]))

  def has_ontime_pane(self):
    return False


class Always(TriggerFn):
  """Repeatedly invoke the given trigger, never finishing."""
  def __init__(self):
    pass

  def __repr__(self):
    return 'Always'

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return 1

  def on_element(self, element, window, context):
    pass

  def on_merge(self, to_be_merged, merge_result, context):
    pass

  def has_ontime_pane(self):
    return False

  def reset(self, window, context):
    pass

  def should_fire(self, time_domain, watermark, window, context):
    return True

  def on_fire(self, watermark, window, context):
    return False

  def may_lose_data(self, unused_windowing):
    """No potential loss, since the trigger always fires."""
    return DataLossReason.NO_POTENTIAL_LOSS

  @staticmethod
  def from_runner_api(proto, context):
    return Always()

  def to_runner_api(self, context):
    return beam_runner_api_pb2.Trigger(
        always=beam_runner_api_pb2.Trigger.Always())


class _Never(TriggerFn):
  """A trigger that never fires.

  Data may still be released at window closing.
  """
  def __init__(self):
    pass

  def __repr__(self):
    return 'Never'

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))

  def on_element(self, element, window, context):
    pass

  def on_merge(self, to_be_merged, merge_result, context):
    pass

  def has_ontime_pane(self):
    False

  def reset(self, window, context):
    pass

  def should_fire(self, time_domain, watermark, window, context):
    return False

  def on_fire(self, watermark, window, context):
    return True

  def may_lose_data(self, unused_windowing):
    """No potential data loss.

    Though Never doesn't explicitly trigger, it still collects data on
    windowing closing.
    """
    return DataLossReason.NO_POTENTIAL_LOSS

  @staticmethod
  def from_runner_api(proto, context):
    return _Never()

  def to_runner_api(self, context):
    return beam_runner_api_pb2.Trigger(
        never=beam_runner_api_pb2.Trigger.Never())


class AfterWatermark(TriggerFn):
  """Fire exactly once when the watermark passes the end of the window.

  Args:
      early: if not None, a speculative trigger to repeatedly evaluate before
        the watermark passes the end of the window
      late: if not None, a speculative trigger to repeatedly evaluate after
        the watermark passes the end of the window
  """
  LATE_TAG = _CombiningValueStateTag('is_late', any)

  def __init__(self, early=None, late=None):
    # TODO(zhoufek): Maybe don't wrap early/late if they are already Repeatedly
    self.early = Repeatedly(early) if early else None
    self.late = Repeatedly(late) if late else None

  def __repr__(self):
    qualifiers = []
    if self.early:
      qualifiers.append('early=%s' % self.early.underlying)
    if self.late:
      qualifiers.append('late=%s' % self.late.underlying)
    return 'AfterWatermark(%s)' % ', '.join(qualifiers)

  def is_late(self, context):
    return self.late and context.get_state(self.LATE_TAG)

  def on_element(self, element, window, context):
    if self.is_late(context):
      self.late.on_element(element, window, NestedContext(context, 'late'))
    else:
      context.set_timer('', TimeDomain.WATERMARK, window.end)
      if self.early:
        self.early.on_element(element, window, NestedContext(context, 'early'))

  def on_merge(self, to_be_merged, merge_result, context):
    # TODO(robertwb): Figure out whether the 'rewind' semantics could be used
    # here.
    if self.is_late(context):
      self.late.on_merge(
          to_be_merged, merge_result, NestedContext(context, 'late'))
    else:
      # Note: Timer clearing solely an optimization.
      for window in to_be_merged:
        if window.end != merge_result.end:
          context.clear_timer('', TimeDomain.WATERMARK)
      if self.early:
        self.early.on_merge(
            to_be_merged, merge_result, NestedContext(context, 'early'))

  def should_fire(self, time_domain, watermark, window, context):
    if self.is_late(context):
      return self.late.should_fire(
          time_domain, watermark, window, NestedContext(context, 'late'))
    elif watermark >= window.end:
      # Explicitly clear the timer so that late elements are not emitted again
      # when the timer is fired.
      context.clear_timer('', TimeDomain.WATERMARK)
      return True
    elif self.early:
      return self.early.should_fire(
          time_domain, watermark, window, NestedContext(context, 'early'))
    return False

  def on_fire(self, watermark, window, context):
    if self.is_late(context):
      return self.late.on_fire(
          watermark, window, NestedContext(context, 'late'))
    elif watermark >= window.end:
      context.add_state(self.LATE_TAG, True)
      return not self.late
    elif self.early:
      self.early.on_fire(watermark, window, NestedContext(context, 'early'))
      return False

  def reset(self, window, context):
    if self.late:
      context.clear_state(self.LATE_TAG)
    if self.early:
      self.early.reset(window, NestedContext(context, 'early'))
    if self.late:
      self.late.reset(window, NestedContext(context, 'late'))

  def may_lose_data(self, windowing):
    """May cause data loss if lateness allowed and no late trigger set."""
    if windowing.allowed_lateness == 0:
      return DataLossReason.NO_POTENTIAL_LOSS
    if self.late is None:
      return DataLossReason.MAY_FINISH
    return self.late.may_lose_data(windowing)

  def __eq__(self, other):
    return (
        type(self) == type(other) and self.early == other.early and
        self.late == other.late)

  def __hash__(self):
    return hash((type(self), self.early, self.late))

  @staticmethod
  def from_runner_api(proto, context):
    return AfterWatermark(
        early=TriggerFn.from_runner_api(
            proto.after_end_of_window.early_firings, context)
        if proto.after_end_of_window.HasField('early_firings') else None,
        late=TriggerFn.from_runner_api(
            proto.after_end_of_window.late_firings, context)
        if proto.after_end_of_window.HasField('late_firings') else None)

  def to_runner_api(self, context):
    early_proto = self.early.underlying.to_runner_api(
        context) if self.early else None
    late_proto = self.late.underlying.to_runner_api(
        context) if self.late else None
    return beam_runner_api_pb2.Trigger(
        after_end_of_window=beam_runner_api_pb2.Trigger.AfterEndOfWindow(
            early_firings=early_proto, late_firings=late_proto))

  def has_ontime_pane(self):
    return True


class AfterCount(TriggerFn):
  """Fire when there are at least count elements in this window pane.

  AfterCount is experimental. No backwards compatibility guarantees.
  """

  COUNT_TAG = _CombiningValueStateTag('count', combiners.CountCombineFn())

  def __init__(self, count):
    if not isinstance(count, numbers.Integral) or count < 1:
      raise ValueError("count (%d) must be a positive integer." % count)
    self.count = count

  def __repr__(self):
    return 'AfterCount(%s)' % self.count

  def __eq__(self, other):
    return type(self) == type(other) and self.count == other.count

  def __hash__(self):
    return hash(self.count)

  def on_element(self, element, window, context):
    context.add_state(self.COUNT_TAG, 1)

  def on_merge(self, to_be_merged, merge_result, context):
    # states automatically merged
    pass

  def should_fire(self, time_domain, watermark, window, context):
    return context.get_state(self.COUNT_TAG) >= self.count

  def on_fire(self, watermark, window, context):
    return True

  def reset(self, window, context):
    context.clear_state(self.COUNT_TAG)

  def may_lose_data(self, unused_windowing):
    """AfterCount may finish."""
    return DataLossReason.MAY_FINISH

  @staticmethod
  def from_runner_api(proto, unused_context):
    return AfterCount(proto.element_count.element_count)

  def to_runner_api(self, unused_context):
    return beam_runner_api_pb2.Trigger(
        element_count=beam_runner_api_pb2.Trigger.ElementCount(
            element_count=self.count))

  def has_ontime_pane(self):
    return False


class Repeatedly(TriggerFn):
  """Repeatedly invoke the given trigger, never finishing."""
  def __init__(self, underlying):
    self.underlying = underlying

  def __repr__(self):
    return 'Repeatedly(%s)' % self.underlying

  def __eq__(self, other):
    return type(self) == type(other) and self.underlying == other.underlying

  def __hash__(self):
    return hash(self.underlying)

  def on_element(self, element, window, context):
    self.underlying.on_element(element, window, context)

  def on_merge(self, to_be_merged, merge_result, context):
    self.underlying.on_merge(to_be_merged, merge_result, context)

  def should_fire(self, time_domain, watermark, window, context):
    return self.underlying.should_fire(time_domain, watermark, window, context)

  def on_fire(self, watermark, window, context):
    if self.underlying.on_fire(watermark, window, context):
      self.underlying.reset(window, context)
    return False

  def reset(self, window, context):
    self.underlying.reset(window, context)

  def may_lose_data(self, windowing):
    """Repeatedly will run in a loop and pick up whatever is left at GC."""
    return DataLossReason.NO_POTENTIAL_LOSS

  @staticmethod
  def from_runner_api(proto, context):
    return Repeatedly(
        TriggerFn.from_runner_api(proto.repeat.subtrigger, context))

  def to_runner_api(self, context):
    return beam_runner_api_pb2.Trigger(
        repeat=beam_runner_api_pb2.Trigger.Repeat(
            subtrigger=self.underlying.to_runner_api(context)))

  def has_ontime_pane(self):
    return self.underlying.has_ontime_pane()


class _ParallelTriggerFn(TriggerFn, metaclass=ABCMeta):
  def __init__(self, *triggers):
    self.triggers = triggers

  def __repr__(self):
    return '%s(%s)' % (
        self.__class__.__name__, ', '.join(str(t) for t in self.triggers))

  def __eq__(self, other):
    return type(self) == type(other) and self.triggers == other.triggers

  def __hash__(self):
    return hash(self.triggers)

  @abstractmethod
  def combine_op(self, trigger_results):
    pass

  def on_element(self, element, window, context):
    for ix, trigger in enumerate(self.triggers):
      trigger.on_element(element, window, self._sub_context(context, ix))

  def on_merge(self, to_be_merged, merge_result, context):
    for ix, trigger in enumerate(self.triggers):
      trigger.on_merge(
          to_be_merged, merge_result, self._sub_context(context, ix))

  def should_fire(self, time_domain, watermark, window, context):
    self._time_domain = time_domain
    return self.combine_op(
        trigger.should_fire(
            time_domain, watermark, window, self._sub_context(context, ix))
        for ix,
        trigger in enumerate(self.triggers))

  def on_fire(self, watermark, window, context):
    finished = []
    for ix, trigger in enumerate(self.triggers):
      nested_context = self._sub_context(context, ix)
      if trigger.should_fire(TimeDomain.WATERMARK,
                             watermark,
                             window,
                             nested_context):
        finished.append(trigger.on_fire(watermark, window, nested_context))
    return self.combine_op(finished)

  def may_lose_data(self, windowing):
    may_finish = self.combine_op(
        _IncludesMayFinish(t.may_lose_data(windowing)) for t in self.triggers)
    return (
        DataLossReason.MAY_FINISH
        if may_finish else DataLossReason.NO_POTENTIAL_LOSS)

  def reset(self, window, context):
    for ix, trigger in enumerate(self.triggers):
      trigger.reset(window, self._sub_context(context, ix))

  @staticmethod
  def _sub_context(context, index):
    return NestedContext(context, '%d/' % index)

  @staticmethod
  def from_runner_api(proto, context):
    subtriggers = [
        TriggerFn.from_runner_api(subtrigger, context) for subtrigger in
        proto.after_all.subtriggers or proto.after_any.subtriggers
    ]
    if proto.after_all.subtriggers:
      return AfterAll(*subtriggers)
    else:
      return AfterAny(*subtriggers)

  def to_runner_api(self, context):
    subtriggers = [
        subtrigger.to_runner_api(context) for subtrigger in self.triggers
    ]
    if self.combine_op == all:
      return beam_runner_api_pb2.Trigger(
          after_all=beam_runner_api_pb2.Trigger.AfterAll(
              subtriggers=subtriggers))
    elif self.combine_op == any:
      return beam_runner_api_pb2.Trigger(
          after_any=beam_runner_api_pb2.Trigger.AfterAny(
              subtriggers=subtriggers))
    else:
      raise NotImplementedError(self)

  def has_ontime_pane(self):
    return any(t.has_ontime_pane() for t in self.triggers)


class AfterAny(_ParallelTriggerFn):
  """Fires when any subtrigger fires.

  Also finishes when any subtrigger finishes.
  """
  combine_op = any


class AfterAll(_ParallelTriggerFn):
  """Fires when all subtriggers have fired.

  Also finishes when all subtriggers have finished.
  """
  combine_op = all


class AfterEach(TriggerFn):

  INDEX_TAG = _CombiningValueStateTag(
      'index', (lambda indices: 0 if not indices else max(indices)))

  def __init__(self, *triggers):
    self.triggers = triggers

  def __repr__(self):
    return '%s(%s)' % (
        self.__class__.__name__, ', '.join(str(t) for t in self.triggers))

  def __eq__(self, other):
    return type(self) == type(other) and self.triggers == other.triggers

  def __hash__(self):
    return hash(self.triggers)

  def on_element(self, element, window, context):
    ix = context.get_state(self.INDEX_TAG)
    if ix < len(self.triggers):
      self.triggers[ix].on_element(
          element, window, self._sub_context(context, ix))

  def on_merge(self, to_be_merged, merge_result, context):
    # This takes the furthest window on merging.
    # TODO(robertwb): Revisit this when merging windows logic is settled for
    # all possible merging situations.
    ix = context.get_state(self.INDEX_TAG)
    if ix < len(self.triggers):
      self.triggers[ix].on_merge(
          to_be_merged, merge_result, self._sub_context(context, ix))

  def should_fire(self, time_domain, watermark, window, context):
    ix = context.get_state(self.INDEX_TAG)
    if ix < len(self.triggers):
      return self.triggers[ix].should_fire(
          time_domain, watermark, window, self._sub_context(context, ix))

  def on_fire(self, watermark, window, context):
    ix = context.get_state(self.INDEX_TAG)
    if ix < len(self.triggers):
      if self.triggers[ix].on_fire(watermark,
                                   window,
                                   self._sub_context(context, ix)):
        ix += 1
        context.add_state(self.INDEX_TAG, ix)
      return ix == len(self.triggers)

  def reset(self, window, context):
    context.clear_state(self.INDEX_TAG)
    for ix, trigger in enumerate(self.triggers):
      trigger.reset(window, self._sub_context(context, ix))

  def may_lose_data(self, windowing):
    """If all sub-triggers may finish, this may finish."""
    may_finish = all(
        _IncludesMayFinish(t.may_lose_data(windowing)) for t in self.triggers)
    return (
        DataLossReason.MAY_FINISH
        if may_finish else DataLossReason.NO_POTENTIAL_LOSS)

  @staticmethod
  def _sub_context(context, index):
    return NestedContext(context, '%d/' % index)

  @staticmethod
  def from_runner_api(proto, context):
    return AfterEach(
        *[
            TriggerFn.from_runner_api(subtrigger, context)
            for subtrigger in proto.after_each.subtriggers
        ])

  def to_runner_api(self, context):
    return beam_runner_api_pb2.Trigger(
        after_each=beam_runner_api_pb2.Trigger.AfterEach(
            subtriggers=[
                subtrigger.to_runner_api(context)
                for subtrigger in self.triggers
            ]))

  def has_ontime_pane(self):
    return any(t.has_ontime_pane() for t in self.triggers)


class OrFinally(AfterAny):
  @staticmethod
  def from_runner_api(proto, context):
    return OrFinally(
        TriggerFn.from_runner_api(proto.or_finally.main, context),
        # getattr is used as finally is a keyword in Python
        TriggerFn.from_runner_api(
            getattr(proto.or_finally, 'finally'), context))

  def to_runner_api(self, context):
    return beam_runner_api_pb2.Trigger(
        or_finally=beam_runner_api_pb2.Trigger.OrFinally(
            main=self.triggers[0].to_runner_api(context),
            # dict keyword argument is used as finally is a keyword in Python
            **{'finally': self.triggers[1].to_runner_api(context)}))


class TriggerContext(object):
  def __init__(self, outer, window, clock):
    self._outer = outer
    self._window = window
    self._clock = clock

  def get_current_time(self):
    return self._clock.time()

  def set_timer(self, name, time_domain, timestamp):
    self._outer.set_timer(self._window, name, time_domain, timestamp)

  def clear_timer(self, name, time_domain):
    self._outer.clear_timer(self._window, name, time_domain)

  def add_state(self, tag, value):
    self._outer.add_state(self._window, tag, value)

  def get_state(self, tag):
    return self._outer.get_state(self._window, tag)

  def clear_state(self, tag):
    return self._outer.clear_state(self._window, tag)


class NestedContext(object):
  """Namespaced context useful for defining composite triggers."""
  def __init__(self, outer, prefix):
    self._outer = outer
    self._prefix = prefix

  def get_current_time(self):
    return self._outer.get_current_time()

  def set_timer(self, name, time_domain, timestamp):
    self._outer.set_timer(self._prefix + name, time_domain, timestamp)

  def clear_timer(self, name, time_domain):
    self._outer.clear_timer(self._prefix + name, time_domain)

  def add_state(self, tag, value):
    self._outer.add_state(tag.with_prefix(self._prefix), value)

  def get_state(self, tag):
    return self._outer.get_state(tag.with_prefix(self._prefix))

  def clear_state(self, tag):
    self._outer.clear_state(tag.with_prefix(self._prefix))


# pylint: disable=unused-argument
class SimpleState(metaclass=ABCMeta):
  """Basic state storage interface used for triggering.

  Only timers must hold the watermark (by their timestamp).
  """
  @abstractmethod
  def set_timer(
      self, window, name, time_domain, timestamp, dynamic_timer_tag=''):
    pass

  @abstractmethod
  def get_window(self, window_id):
    pass

  @abstractmethod
  def clear_timer(self, window, name, time_domain, dynamic_timer_tag=''):
    pass

  @abstractmethod
  def add_state(self, window, tag, value):
    pass

  @abstractmethod
  def get_state(self, window, tag):
    pass

  @abstractmethod
  def clear_state(self, window, tag):
    pass

  def at(self, window, clock):
    return NestedContext(TriggerContext(self, window, clock), 'trigger')


class UnmergedState(SimpleState):
  """State suitable for use in TriggerDriver.

  This class must be implemented by each backend.
  """
  @abstractmethod
  def set_global_state(self, tag, value):
    pass

  @abstractmethod
  def get_global_state(self, tag, default=None):
    pass


# pylint: enable=unused-argument


class MergeableStateAdapter(SimpleState):
  """Wraps an UnmergedState, tracking merged windows."""
  # TODO(robertwb): A similar indirection could be used for sliding windows
  # or other window_fns when a single element typically belongs to many windows.

  WINDOW_IDS = _ReadModifyWriteStateTag('window_ids')

  def __init__(self, raw_state):
    self.raw_state = raw_state
    self.window_ids = self.raw_state.get_global_state(self.WINDOW_IDS, {})
    self.counter = None

  def set_timer(
      self, window, name, time_domain, timestamp, dynamic_timer_tag=''):
    self.raw_state.set_timer(
        self._get_id(window),
        name,
        time_domain,
        timestamp,
        dynamic_timer_tag=dynamic_timer_tag)

  def clear_timer(self, window, name, time_domain, dynamic_timer_tag=''):
    for window_id in self._get_ids(window):
      self.raw_state.clear_timer(
          window_id, name, time_domain, dynamic_timer_tag=dynamic_timer_tag)

  def add_state(self, window, tag, value):
    if isinstance(tag, _ReadModifyWriteStateTag):
      raise ValueError(
          'Merging requested for non-mergeable state tag: %r.' % tag)
    elif isinstance(tag, _CombiningValueStateTag):
      tag = tag.without_extraction()
    self.raw_state.add_state(self._get_id(window), tag, value)

  def get_state(self, window, tag):
    if isinstance(tag, _CombiningValueStateTag):
      original_tag, tag = tag, tag.without_extraction()
    values = [
        self.raw_state.get_state(window_id, tag)
        for window_id in self._get_ids(window)
    ]
    if isinstance(tag, _ReadModifyWriteStateTag):
      raise ValueError(
          'Merging requested for non-mergeable state tag: %r.' % tag)
    elif isinstance(tag, _CombiningValueStateTag):
      return original_tag.combine_fn.extract_output(
          original_tag.combine_fn.merge_accumulators(values))
    elif isinstance(tag, _ListStateTag):
      return [v for vs in values for v in vs]
    elif isinstance(tag, _SetStateTag):
      return {v for vs in values for v in vs}
    elif isinstance(tag, _WatermarkHoldStateTag):
      return tag.timestamp_combiner_impl.combine_all(values)
    else:
      raise ValueError('Invalid tag.', tag)

  def clear_state(self, window, tag):
    for window_id in self._get_ids(window):
      self.raw_state.clear_state(window_id, tag)
    if tag is None:
      del self.window_ids[window]
      self._persist_window_ids()

  def merge(self, to_be_merged, merge_result):
    for window in to_be_merged:
      if window != merge_result:
        if window in self.window_ids:
          if merge_result in self.window_ids:
            merge_window_ids = self.window_ids[merge_result]
          else:
            merge_window_ids = self.window_ids[merge_result] = []
          merge_window_ids.extend(self.window_ids.pop(window))
          self._persist_window_ids()

  def known_windows(self):
    return list(self.window_ids)

  def get_window(self, window_id):
    for window, ids in self.window_ids.items():
      if window_id in ids:
        return window
    raise ValueError('No window for %s' % window_id)

  def _get_id(self, window):
    if window in self.window_ids:
      return self.window_ids[window][0]

    window_id = self._get_next_counter()
    self.window_ids[window] = [window_id]
    self._persist_window_ids()
    return window_id

  def _get_ids(self, window):
    return self.window_ids.get(window, [])

  def _get_next_counter(self):
    if not self.window_ids:
      self.counter = 0
    elif self.counter is None:
      self.counter = max(k for ids in self.window_ids.values() for k in ids)
    self.counter += 1
    return self.counter

  def _persist_window_ids(self):
    self.raw_state.set_global_state(self.WINDOW_IDS, self.window_ids)

  def __repr__(self):
    return '\n\t'.join([repr(self.window_ids)] +
                       repr(self.raw_state).split('\n'))


def create_trigger_driver(
    windowing, is_batch=False, phased_combine_fn=None, clock=None):
  """Create the TriggerDriver for the given windowing and options."""

  # TODO(BEAM-10149): Respect closing and on-time behaviors.
  # For batch, we should always fire once, no matter what.
  if is_batch and windowing.triggerfn == _Never():
    windowing = copy.copy(windowing)
    windowing.triggerfn = Always()

  # TODO(robertwb): We can do more if we know elements are in timestamp
  # sorted order.
  if windowing.is_default() and is_batch:
    driver = BatchGlobalTriggerDriver()
  elif (windowing.windowfn == GlobalWindows() and
        (windowing.triggerfn in [AfterCount(1), Always()]) and is_batch):
    # Here we also just pass through all the values exactly once.
    driver = BatchGlobalTriggerDriver()
  else:
    driver = GeneralTriggerDriver(windowing, clock)

  if phased_combine_fn:
    # TODO(ccy): Refactor GeneralTriggerDriver to combine values eagerly using
    # the known phased_combine_fn here.
    driver = CombiningTriggerDriver(phased_combine_fn, driver)
  return driver


class TriggerDriver(metaclass=ABCMeta):
  """Breaks a series of bundle and timer firings into window (pane)s."""
  @abstractmethod
  def process_elements(
      self,
      state,
      windowed_values,
      output_watermark,
      input_watermark=MIN_TIMESTAMP):
    pass

  @abstractmethod
  def process_timer(
      self,
      window_id,
      name,
      time_domain,
      timestamp,
      state,
      input_watermark=None):
    pass

  def process_entire_key(self, key, windowed_values):
    # This state holds per-key, multi-window state.
    state = InMemoryUnmergedState()
    for wvalue in self.process_elements(state,
                                        windowed_values,
                                        MIN_TIMESTAMP,
                                        MIN_TIMESTAMP):
      yield wvalue.with_value((key, wvalue.value))
    while state.timers:
      fired = state.get_and_clear_timers()
      for timer_window, (name, time_domain, fire_time, _) in fired:
        for wvalue in self.process_timer(timer_window,
                                         name,
                                         time_domain,
                                         fire_time,
                                         state):
          yield wvalue.with_value((key, wvalue.value))


class _UnwindowedValues(observable.ObservableMixin):
  """Exposes iterable of windowed values as iterable of unwindowed values."""
  def __init__(self, windowed_values):
    super().__init__()
    self._windowed_values = windowed_values

  def __iter__(self):
    for wv in self._windowed_values:
      unwindowed_value = wv.value
      self.notify_observers(unwindowed_value)
      yield unwindowed_value

  def __repr__(self):
    return '<_UnwindowedValues of %s>' % self._windowed_values

  def __reduce__(self):
    return list, (list(self), )

  def __eq__(self, other):
    if isinstance(other, collections_abc.Iterable):
      return all(
          a == b for a, b in zip_longest(self, other, fillvalue=object()))
    else:
      return NotImplemented

  def __hash__(self):
    return hash(tuple(self))


coder_impl.FastPrimitivesCoderImpl.register_iterable_like_type(
    _UnwindowedValues)


class BatchGlobalTriggerDriver(TriggerDriver):
  """Groups all received values together.
  """
  GLOBAL_WINDOW_TUPLE = (GlobalWindow(), )
  ONLY_FIRING = windowed_value.PaneInfo(
      is_first=True,
      is_last=True,
      timing=windowed_value.PaneInfoTiming.ON_TIME,
      index=0,
      nonspeculative_index=0)

  def process_elements(
      self,
      state,
      windowed_values,
      unused_output_watermark,
      unused_input_watermark=MIN_TIMESTAMP):
    yield WindowedValue(
        _UnwindowedValues(windowed_values),
        MIN_TIMESTAMP,
        self.GLOBAL_WINDOW_TUPLE,
        self.ONLY_FIRING)

  def process_timer(
      self,
      window_id,
      name,
      time_domain,
      timestamp,
      state,
      input_watermark=None):
    raise TypeError('Triggers never set or called for batch default windowing.')


class CombiningTriggerDriver(TriggerDriver):
  """Uses a phased_combine_fn to process output of wrapped TriggerDriver."""
  def __init__(self, phased_combine_fn, underlying):
    self.phased_combine_fn = phased_combine_fn
    self.underlying = underlying

  def process_elements(
      self,
      state,
      windowed_values,
      output_watermark,
      input_watermark=MIN_TIMESTAMP):
    uncombined = self.underlying.process_elements(
        state, windowed_values, output_watermark, input_watermark)
    for output in uncombined:
      yield output.with_value(self.phased_combine_fn.apply(output.value))

  def process_timer(
      self,
      window_id,
      name,
      time_domain,
      timestamp,
      state,
      input_watermark=None):
    uncombined = self.underlying.process_timer(
        window_id, name, time_domain, timestamp, state, input_watermark)
    for output in uncombined:
      yield output.with_value(self.phased_combine_fn.apply(output.value))


class GeneralTriggerDriver(TriggerDriver):
  """Breaks a series of bundle and timer firings into window (pane)s.

  Suitable for all variants of Windowing.
  """
  ELEMENTS = _ListStateTag('elements')
  TOMBSTONE = _CombiningValueStateTag('tombstone', combiners.CountCombineFn())
  INDEX = _CombiningValueStateTag('index', combiners.CountCombineFn())
  NONSPECULATIVE_INDEX = _CombiningValueStateTag(
      'nonspeculative_index', combiners.CountCombineFn())

  def __init__(self, windowing, clock):
    self.clock = clock
    self.allowed_lateness = windowing.allowed_lateness
    self.window_fn = windowing.windowfn
    self.timestamp_combiner_impl = TimestampCombiner.get_impl(
        windowing.timestamp_combiner, self.window_fn)
    # pylint: disable=invalid-name
    self.WATERMARK_HOLD = _WatermarkHoldStateTag(
        'watermark', self.timestamp_combiner_impl)
    # pylint: enable=invalid-name
    self.trigger_fn = windowing.triggerfn
    self.accumulation_mode = windowing.accumulation_mode
    self.is_merging = True

  def process_elements(
      self,
      state,
      windowed_values,
      output_watermark,
      input_watermark=MIN_TIMESTAMP):
    if self.is_merging:
      state = MergeableStateAdapter(state)

    windows_to_elements = collections.defaultdict(list)
    for wv in windowed_values:
      for window in wv.windows:
        # ignore expired windows
        if input_watermark > window.end + self.allowed_lateness:
          continue
        windows_to_elements[window].append((wv.value, wv.timestamp))

    # First handle merging.
    if self.is_merging:
      old_windows = set(state.known_windows())
      all_windows = old_windows.union(list(windows_to_elements))

      if all_windows != old_windows:
        merged_away = {}

        class TriggerMergeContext(WindowFn.MergeContext):
          def merge(_, to_be_merged, merge_result):  # pylint: disable=no-self-argument
            for window in to_be_merged:
              if window != merge_result:
                merged_away[window] = merge_result
                # Clear state associated with PaneInfo since it is
                # not preserved across merges.
                state.clear_state(window, self.INDEX)
                state.clear_state(window, self.NONSPECULATIVE_INDEX)
            state.merge(to_be_merged, merge_result)
            # using the outer self argument.
            self.trigger_fn.on_merge(
                to_be_merged, merge_result, state.at(merge_result, self.clock))

        self.window_fn.merge(TriggerMergeContext(all_windows))

        merged_windows_to_elements = collections.defaultdict(list)
        for window, values in windows_to_elements.items():
          while window in merged_away:
            window = merged_away[window]
          merged_windows_to_elements[window].extend(values)
        windows_to_elements = merged_windows_to_elements

        for window in merged_away:
          state.clear_state(window, self.WATERMARK_HOLD)

    # Next handle element adding.
    for window, elements in windows_to_elements.items():
      if state.get_state(window, self.TOMBSTONE):
        continue
      # Add watermark hold.
      # TODO(ccy): Add late data and garbage-collection hold support.
      output_time = self.timestamp_combiner_impl.merge(
          window,
          (
              element_output_time for element_output_time in (
                  self.timestamp_combiner_impl.assign_output_time(
                      window, timestamp) for unused_value,
                  timestamp in elements)
              if element_output_time >= output_watermark))
      if output_time is not None:
        state.add_state(window, self.WATERMARK_HOLD, output_time)

      context = state.at(window, self.clock)
      for value, unused_timestamp in elements:
        state.add_state(window, self.ELEMENTS, value)
        self.trigger_fn.on_element(value, window, context)

      # Maybe fire this window.
      if self.trigger_fn.should_fire(TimeDomain.WATERMARK,
                                     input_watermark,
                                     window,
                                     context):
        finished = self.trigger_fn.on_fire(input_watermark, window, context)
        yield self._output(window, finished, state, output_watermark, False)

  def process_timer(
      self,
      window_id,
      unused_name,
      time_domain,
      timestamp,
      state,
      input_watermark=None):
    if input_watermark is None:
      input_watermark = timestamp

    if self.is_merging:
      state = MergeableStateAdapter(state)
    window = state.get_window(window_id)
    if state.get_state(window, self.TOMBSTONE):
      return

    if time_domain in (TimeDomain.WATERMARK, TimeDomain.REAL_TIME):
      if not self.is_merging or window in state.known_windows():
        context = state.at(window, self.clock)
        if self.trigger_fn.should_fire(time_domain, timestamp, window, context):
          finished = self.trigger_fn.on_fire(timestamp, window, context)
          yield self._output(
              window,
              finished,
              state,
              timestamp,
              time_domain == TimeDomain.WATERMARK)
    else:
      raise Exception('Unexpected time domain: %s' % time_domain)

  def _output(self, window, finished, state, output_watermark, maybe_ontime):
    """Output window and clean up if appropriate."""
    index = state.get_state(window, self.INDEX)
    state.add_state(window, self.INDEX, 1)
    if output_watermark <= window.max_timestamp():
      nonspeculative_index = -1
      timing = windowed_value.PaneInfoTiming.EARLY
      if state.get_state(window, self.NONSPECULATIVE_INDEX):
        nonspeculative_index = state.get_state(
            window, self.NONSPECULATIVE_INDEX)
        state.add_state(window, self.NONSPECULATIVE_INDEX, 1)
        _LOGGER.warning(
            'Watermark moved backwards in time '
            'or late data moved window end forward.')
    else:
      nonspeculative_index = state.get_state(window, self.NONSPECULATIVE_INDEX)
      state.add_state(window, self.NONSPECULATIVE_INDEX, 1)
      timing = (
          windowed_value.PaneInfoTiming.ON_TIME if maybe_ontime and
          nonspeculative_index == 0 else windowed_value.PaneInfoTiming.LATE)
    pane_info = windowed_value.PaneInfo(
        index == 0, finished, timing, index, nonspeculative_index)

    values = state.get_state(window, self.ELEMENTS)
    if finished:
      # TODO(robertwb): allowed lateness
      state.clear_state(window, self.ELEMENTS)
      state.add_state(window, self.TOMBSTONE, 1)
    elif self.accumulation_mode == AccumulationMode.DISCARDING:
      state.clear_state(window, self.ELEMENTS)

    timestamp = state.get_state(window, self.WATERMARK_HOLD)
    if timestamp is None:
      # If no watermark hold was set, output at end of window.
      timestamp = window.max_timestamp()
    elif output_watermark < window.end and self.trigger_fn.has_ontime_pane():
      # Hold the watermark in case there is an empty pane that needs to be fired
      # at the end of the window.
      pass
    else:
      state.clear_state(window, self.WATERMARK_HOLD)

    return WindowedValue(values, timestamp, (window, ), pane_info)


class InMemoryUnmergedState(UnmergedState):
  """In-memory implementation of UnmergedState.

  Used for batch and testing.
  """
  def __init__(self, defensive_copy=False):
    # TODO(robertwb): Clean defensive_copy. It is too expensive in production.
    self.timers = collections.defaultdict(dict)
    self.state = collections.defaultdict(lambda: collections.defaultdict(list))
    self.global_state = {}
    self.defensive_copy = defensive_copy

  def copy(self):
    cloned_object = InMemoryUnmergedState(defensive_copy=self.defensive_copy)
    cloned_object.timers = copy.deepcopy(self.timers)
    cloned_object.global_state = copy.deepcopy(self.global_state)
    for window in self.state:
      for tag in self.state[window]:
        cloned_object.state[window][tag] = copy.copy(self.state[window][tag])
    return cloned_object

  def set_global_state(self, tag, value):
    assert isinstance(tag, _ReadModifyWriteStateTag)
    if self.defensive_copy:
      value = copy.deepcopy(value)
    self.global_state[tag.tag] = value

  def get_global_state(self, tag, default=None):
    return self.global_state.get(tag.tag, default)

  def set_timer(
      self, window, name, time_domain, timestamp, dynamic_timer_tag=''):
    self.timers[window][(name, time_domain, dynamic_timer_tag)] = timestamp

  def clear_timer(self, window, name, time_domain, dynamic_timer_tag=''):
    self.timers[window].pop((name, time_domain, dynamic_timer_tag), None)
    if not self.timers[window]:
      del self.timers[window]

  def get_window(self, window_id):
    return window_id

  def add_state(self, window, tag, value):
    if self.defensive_copy:
      value = copy.deepcopy(value)
    if isinstance(tag, _ReadModifyWriteStateTag):
      self.state[window][tag.tag] = value
    elif isinstance(tag, _CombiningValueStateTag):
      # TODO(robertwb): Store merged accumulators.
      self.state[window][tag.tag].append(value)
    elif isinstance(tag, _ListStateTag):
      self.state[window][tag.tag].append(value)
    elif isinstance(tag, _SetStateTag):
      self.state[window][tag.tag].append(value)
    elif isinstance(tag, _WatermarkHoldStateTag):
      self.state[window][tag.tag].append(value)
    else:
      raise ValueError('Invalid tag.', tag)

  def get_state(self, window, tag):
    values = self.state[window][tag.tag]
    if isinstance(tag, _ReadModifyWriteStateTag):
      return values
    elif isinstance(tag, _CombiningValueStateTag):
      return tag.combine_fn.apply(values)
    elif isinstance(tag, _ListStateTag):
      return values
    elif isinstance(tag, _SetStateTag):
      return values
    elif isinstance(tag, _WatermarkHoldStateTag):
      return tag.timestamp_combiner_impl.combine_all(values)
    else:
      raise ValueError('Invalid tag.', tag)

  def clear_state(self, window, tag):
    self.state[window].pop(tag.tag, None)
    if not self.state[window]:
      self.state.pop(window, None)

  def get_timers(
      self, clear=False, watermark=MAX_TIMESTAMP, processing_time=None):
    """Gets expired timers and reports if there
    are any realtime timers set per state.

    Expiration is measured against the watermark for event-time timers,
    and against a wall clock for processing-time timers.
    """
    expired = []
    has_realtime_timer = False
    for window, timers in list(self.timers.items()):
      for (name, time_domain, dynamic_timer_tag), timestamp in list(
          timers.items()):
        if time_domain == TimeDomain.REAL_TIME:
          time_marker = processing_time
          has_realtime_timer = True
        elif time_domain == TimeDomain.WATERMARK:
          time_marker = watermark
        else:
          _LOGGER.error(
              'TimeDomain error: No timers defined for time domain %s.',
              time_domain)
        if timestamp <= time_marker:
          expired.append(
              (window, (name, time_domain, timestamp, dynamic_timer_tag)))
          if clear:
            del timers[(name, time_domain, dynamic_timer_tag)]
      if not timers and clear:
        del self.timers[window]
    return expired, has_realtime_timer

  def get_and_clear_timers(self, watermark=MAX_TIMESTAMP):
    return self.get_timers(clear=True, watermark=watermark)[0]

  def get_earliest_hold(self):
    earliest_hold = MAX_TIMESTAMP
    for unused_window, tagged_states in self.state.items():
      # TODO(BEAM-2519): currently, this assumes that the watermark hold tag is
      # named "watermark".  This is currently only true because the only place
      # watermark holds are set is in the GeneralTriggerDriver, where we use
      # this name.  We should fix this by allowing enumeration of the tag types
      # used in adding state.
      if 'watermark' in tagged_states and tagged_states['watermark']:
        hold = min(tagged_states['watermark']) - TIME_GRANULARITY
        earliest_hold = min(earliest_hold, hold)
    return earliest_hold

  def __repr__(self):
    state_str = '\n'.join(
        '%s: %s' % (key, dict(state)) for key, state in self.state.items())
    return 'timers: %s\nstate: %s' % (dict(self.timers), state_str)
