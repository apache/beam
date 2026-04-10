#
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

"""Reusable watermark-gated timestamp buffer DoFns with batched processing.

Uses four timers:

1. **MIN_WM** (event-time): set to the earliest unprocessed element.
   When it fires, safe_watermark is confirmed at that timestamp.
   This is the trigger that starts each batch cycle, it arms the
   BATCH_TIMER and the MID/MAX watermark probes.

2. **MID_WM** (event-time): set to midpoint between min and max
   element timestamps. Advances safe_watermark when it fires.

3. **MAX_WM** (event-time): set to max element timestamp seen.
   Advances safe_watermark to the upper bound when it fires.

4. **BATCH_TIMER** (processing-time): armed by MIN_WM callback.
   Fires ``batch_interval_sec`` after MIN_WM confirms the first
   element is safe. On firing, processes all elements up to
   safe_watermark. Then sets MIN_WM to the next unprocessed element
   to start the next cycle.

MID_WM and MAX_WM are only set when they differ from MIN_WM.

Two buffer backends are available:

- ``TimestampBufferDoFnBag``: uses ``BagState`` (works on all runners)
- ``TimestampBufferDoFnOLS``: uses ``OrderedListState`` (Dataflow v2)
"""

import abc
import dataclasses
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.coders import FastPrimitivesCoder
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import OrderedListStateSpec
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)

_EPOCH = Timestamp(0)
_ONE_MICRO = Duration(micros=1)


def _fmt(timestamp):
  """Format a Timestamp for logging, handling None."""
  if timestamp is None:
    return 'None'
  return timestamp.to_rfc3339()


# -- State dataclass --

@dataclasses.dataclass
class BufferState:
  """All mutable per-key state for the timestamp buffer.

  Stored as a single pickled object in ReadModifyWriteState.
  All timestamp fields use Beam ``Timestamp`` objects.
  """
  safe_watermark: Timestamp = _EPOCH
  last_processed: Timestamp = _EPOCH
  min_event_time: Optional[Timestamp] = None
  max_event_time: Optional[Timestamp] = None
  batch_pending: bool = False

  def update_min_max(self, event_time):
    """Update min/max event timestamps with a new element."""
    if self.min_event_time is None or event_time < self.min_event_time:
      self.min_event_time = event_time
    if self.max_event_time is None or event_time > self.max_event_time:
      self.max_event_time = event_time

  def advance_safe_watermark(self, timestamp):
    """Advance safe_watermark if timestamp is beyond current value."""
    if timestamp > self.safe_watermark:
      self.safe_watermark = timestamp

  def reset_cycle(self):
    """Clear all cycle state after buffer is fully drained."""
    self.min_event_time = None
    self.max_event_time = None
    self.batch_pending = False

  def start_next_cycle(self, next_timestamp, min_wm_timer):
    """Preserve max_event_time, arm MIN_WM for next unprocessed."""
    self.min_event_time = next_timestamp
    # max_event_time preserved, those elements are still in buffer.
    self.batch_pending = False
    min_wm_timer.set(next_timestamp)


# -- Buffer readers --

class BagBufferReader:
  """Buffer reader backed by a materialized BagState."""

  def __init__(self, bag_state):
    self._bag_state = bag_state
    self._cached = None

  def _ensure_cached(self):
    if self._cached is None:
      self._cached = sorted(self._bag_state.read(), key=lambda x: x[0])

  def read_range(self, start_timestamp, end_timestamp):
    self._ensure_cached()
    return [(timestamp, value) for timestamp, value in self._cached
            if start_timestamp <= timestamp < end_timestamp]

  def read_before(self, timestamp, duration):
    return self.read_range(timestamp - duration, timestamp)


class OLSBufferReader:
  """Buffer reader backed by OrderedListState."""

  def __init__(self, ols_state):
    self._ols_state = ols_state

  def read_range(self, start_timestamp, end_timestamp):
    return list(self._ols_state.read_range(start_timestamp, end_timestamp))

  def read_before(self, timestamp, duration):
    return self.read_range(timestamp - duration, timestamp)


# -- Base DoFn --

class TimestampBufferDoFn(beam.DoFn, abc.ABC):
  """Abstract base for watermark-gated timestamp-buffered DoFns.

  Constructor args:
      batch_interval_sec: Real-time seconds to wait before processing
          (default 5). After MIN_WM confirms the first element is
          safe, the BATCH_TIMER fires this many seconds later.

  Reserved state/timer names: ``'buffer'``, ``'tb_state'``,
  ``'extra_state'``, ``'min_wm'``, ``'mid_wm'``, ``'max_wm'``,
  ``'batch'``.
  """

  TB_STATE = ReadModifyWriteStateSpec('tb_state', FastPrimitivesCoder())

  def __init__(self, context_size, batch_interval_sec=5):
    self._context_size = context_size
    self._batch_interval_sec = batch_interval_sec

  # -- Extension points --

  def extract_kv(self, element):
    key, value = element
    return key, value

  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    raise NotImplementedError(
        'Subclasses must implement process_element()')


  # -- Abstract buffer operations --

  @abc.abstractmethod
  def _buffer_add(self, buffer_state, timestamp, value):
    ...

  @abc.abstractmethod
  def _read_unprocessed(self, buffer_state, last_processed_timestamp,
                        fire_timestamp):
    ...

  @abc.abstractmethod
  def _make_buffer_reader(self, buffer_state):
    ...

  @abc.abstractmethod
  def _buffer_trim(self, buffer_state, trim_timestamp,
                   previous_trim_timestamp):
    ...

  @abc.abstractmethod
  def _read_next_unprocessed(self, buffer_state, after_timestamp):
    ...

  # -- Core logic --

  def _do_process(self, element, buffer_state, tb_state_handle,
                  min_wm_timer, element_timestamp, **extra_state):
    """Buffer element, track min/max, set MIN_WM if needed."""
    _, value = self.extract_kv(element)
    self._buffer_add(buffer_state, element_timestamp, value)

    state = tb_state_handle.read() or BufferState()

    state.update_min_max(element_timestamp)

    if element_timestamp <= state.safe_watermark:
      _LOGGER.info(
          '[process] dropping late element at %s '
          '(safe_watermark=%s already advanced past it)',
          _fmt(element_timestamp), _fmt(state.safe_watermark))
      tb_state_handle.write(state)
      return []

    if state.min_event_time > state.safe_watermark:
      min_wm_timer.set(state.min_event_time)
      _LOGGER.info(
          '[process] MIN_WM set to %s (safe_watermark=%s)',
          _fmt(state.min_event_time), _fmt(state.safe_watermark))

    tb_state_handle.write(state)
    return []

  def _do_on_min_wm(self, tb_state_handle, mid_wm_timer,
                    max_wm_timer, batch_timer, fire_timestamp):
    """MIN_WM fired: confirm lower bound, arm batch + mid/max."""
    state = tb_state_handle.read() or BufferState()
    previous_watermark = state.safe_watermark
    state.advance_safe_watermark(fire_timestamp)

    _LOGGER.info(
        '[on_min_wm] fire_ts=%s  safe_watermark: %s -> %s',
        _fmt(fire_timestamp), _fmt(previous_watermark),
        _fmt(state.safe_watermark))

    # Arm BATCH_TIMER.
    if not state.batch_pending:
      batch_timer.set(
          Timestamp.now() + Duration.of(self._batch_interval_sec))
      state.batch_pending = True
      _LOGGER.info(
          '[on_min_wm] BATCH_TIMER armed at now+%ss',
          self._batch_interval_sec)

    # Arm MID_WM and MAX_WM probes if max is beyond safe.
    if (state.max_event_time is not None
        and state.max_event_time > state.safe_watermark):
      midpoint = Timestamp(micros=(
          state.safe_watermark.micros + state.max_event_time.micros) // 2)
      if state.safe_watermark < midpoint < state.max_event_time:
        mid_wm_timer.set(midpoint)
        _LOGGER.info(
            '[on_min_wm] MID_WM set to %s', _fmt(midpoint))
      max_wm_timer.set(state.max_event_time)
      _LOGGER.info(
          '[on_min_wm] MAX_WM set to %s', _fmt(state.max_event_time))

    tb_state_handle.write(state)

  def _do_on_wm_probe(self, tb_state_handle, fire_timestamp):
    """MID_WM or MAX_WM fired: advance safe_watermark."""
    state = tb_state_handle.read() or BufferState()
    previous_watermark = state.safe_watermark
    state.advance_safe_watermark(fire_timestamp)
    _LOGGER.info(
        '[on_wm_probe] fire_ts=%s  safe_watermark: %s -> %s',
        _fmt(fire_timestamp), _fmt(previous_watermark),
        _fmt(state.safe_watermark))
    tb_state_handle.write(state)

  def _process_batch(self, buffer_state, state, key, **extra_state):
    """Process buffered elements from last_processed to safe_watermark."""
    unprocessed = self._read_unprocessed(
        buffer_state, state.last_processed, state.safe_watermark)

    if not unprocessed:
      return

    _LOGGER.info(
        '[on_batch] key=%s processing %d elements '
        '(last_processed=%s .. safe_watermark=%s)',
        key, len(unprocessed),
        _fmt(state.last_processed), _fmt(state.safe_watermark))

    # Trim old processed elements, keeping context_size for lookback.
    if self._context_size > 0:
      reader = self._make_buffer_reader(buffer_state)
      prior_context = reader.read_range(
          _EPOCH, state.last_processed + _ONE_MICRO)
      if len(prior_context) > self._context_size:
        trim_before = prior_context[-(self._context_size + 1)][0]
        self._buffer_trim(buffer_state, trim_before, _EPOCH)
        prior_context = prior_context[-self._context_size:]
    else:
      self._buffer_trim(buffer_state, state.last_processed,
                        _EPOCH)
      prior_context = []

    for index, (element_timestamp, value) in enumerate(unprocessed):
      if index < self._context_size:
        combined = prior_context + unprocessed[:index]
        context = combined[-self._context_size:]
      else:
        context = unprocessed[index - self._context_size:index]
      yield from self.process_element(
          key, element_timestamp, value, context, **extra_state)

    state.last_processed = state.safe_watermark

  def _do_on_batch(self, buffer_state, tb_state_handle,
                   min_wm_timer, key, **extra_state):
    """BATCH_TIMER fired: process up to safe_watermark, start next cycle."""
    state = tb_state_handle.read() or BufferState()

    _LOGGER.info(
        '[on_batch] key=%s  safe_watermark=%s  last_processed=%s  '
        'min_event_time=%s  max_event_time=%s',
        key, _fmt(state.safe_watermark),
        _fmt(state.last_processed),
        _fmt(state.min_event_time),
        _fmt(state.max_event_time))

    if state.safe_watermark <= state.last_processed:
      _LOGGER.info(
          '[on_batch] key=%s nothing to process '
          '(safe_watermark=%s last_processed=%s)',
          key, _fmt(state.safe_watermark),
          _fmt(state.last_processed))
    else:
      yield from self._process_batch(
          buffer_state, state, key, **extra_state)

    # Check for remaining unprocessed elements.
    next_timestamp = self._read_next_unprocessed(
        buffer_state, state.last_processed)

    if next_timestamp is not None:
      state.start_next_cycle(next_timestamp, min_wm_timer)
      _LOGGER.info(
          '[on_batch] key=%s more remain (next=%s). '
          'MIN_WM set for next cycle. max_event_time=%s',
          key, _fmt(next_timestamp), _fmt(state.max_event_time))
    else:
      state.reset_cycle()
      _LOGGER.info(
          '[on_batch] key=%s buffer empty. Cycle done.', key)

    tb_state_handle.write(state)


# -- Concrete implementations --

class TimestampBufferDoFnBag(TimestampBufferDoFn):
  """Timestamp buffer using ``BagState``. Works on all runners."""

  BUFFER = BagStateSpec('buffer', FastPrimitivesCoder())
  EXTRA_STATE = ReadModifyWriteStateSpec(
      'extra_state', FastPrimitivesCoder())
  MIN_WM_TIMER = TimerSpec('min_wm', TimeDomain.WATERMARK)
  MID_WM_TIMER = TimerSpec('mid_wm', TimeDomain.WATERMARK)
  MAX_WM_TIMER = TimerSpec('max_wm', TimeDomain.WATERMARK)
  BATCH_TIMER = TimerSpec('batch', TimeDomain.REAL_TIME)

  def _buffer_add(self, buffer_state, timestamp, value):
    buffer_state.add((timestamp, value))

  def _read_unprocessed(self, buffer_state, last_processed_timestamp,
                        fire_timestamp):
    all_entries = sorted(buffer_state.read(), key=lambda x: x[0])
    return [(timestamp, value) for timestamp, value in all_entries
            if last_processed_timestamp < timestamp <= fire_timestamp]

  def _make_buffer_reader(self, buffer_state):
    return BagBufferReader(buffer_state)

  def _buffer_trim(self, buffer_state, trim_timestamp,
                   previous_trim_timestamp):
    all_entries = sorted(buffer_state.read(), key=lambda x: x[0])
    kept = [(timestamp, value) for timestamp, value in all_entries
            if timestamp > trim_timestamp]
    buffer_state.clear()
    for entry in kept:
      buffer_state.add(entry)

  def _read_next_unprocessed(self, buffer_state, after_timestamp):
    all_entries = sorted(buffer_state.read(), key=lambda x: x[0])
    for timestamp, _ in all_entries:
      if timestamp > after_timestamp:
        return timestamp
    return None

  def process(
      self,
      element,
      buffer=beam.DoFn.StateParam(BUFFER),
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      min_wm_timer=beam.DoFn.TimerParam(MIN_WM_TIMER),
      element_timestamp=beam.DoFn.TimestampParam,
      extra_state=beam.DoFn.StateParam(EXTRA_STATE)):
    return self._do_process(
        element, buffer, tb_state,
        min_wm_timer, element_timestamp,
        extra_state=extra_state)

  @on_timer(MIN_WM_TIMER)
  def on_min_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      mid_wm_timer=beam.DoFn.TimerParam(MID_WM_TIMER),
      max_wm_timer=beam.DoFn.TimerParam(MAX_WM_TIMER),
      batch_timer=beam.DoFn.TimerParam(BATCH_TIMER),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_min_wm(
        tb_state, mid_wm_timer, max_wm_timer,
        batch_timer, fire_timestamp)

  @on_timer(MID_WM_TIMER)
  def on_mid_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_wm_probe(tb_state, fire_timestamp)

  @on_timer(MAX_WM_TIMER)
  def on_max_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_wm_probe(tb_state, fire_timestamp)

  @on_timer(BATCH_TIMER)
  def on_batch(
      self,
      buffer=beam.DoFn.StateParam(BUFFER),
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      min_wm_timer=beam.DoFn.TimerParam(MIN_WM_TIMER),
      key=beam.DoFn.KeyParam,
      extra_state=beam.DoFn.StateParam(EXTRA_STATE)):
    yield from self._do_on_batch(
        buffer, tb_state, min_wm_timer, key,
        extra_state=extra_state)


class TimestampBufferDoFnOLS(TimestampBufferDoFn):
  """Timestamp buffer using ``OrderedListState``."""

  BUFFER = OrderedListStateSpec('buffer', FastPrimitivesCoder())
  EXTRA_STATE = ReadModifyWriteStateSpec(
      'extra_state', FastPrimitivesCoder())
  MIN_WM_TIMER = TimerSpec('min_wm', TimeDomain.WATERMARK)
  MID_WM_TIMER = TimerSpec('mid_wm', TimeDomain.WATERMARK)
  MAX_WM_TIMER = TimerSpec('max_wm', TimeDomain.WATERMARK)
  BATCH_TIMER = TimerSpec('batch', TimeDomain.REAL_TIME)

  def _buffer_add(self, buffer_state, timestamp, value):
    buffer_state.add((timestamp, value))

  def _read_unprocessed(self, buffer_state, last_processed_timestamp,
                        fire_timestamp):
    entries = buffer_state.read_range(
        last_processed_timestamp, fire_timestamp + _ONE_MICRO)
    return [(timestamp, value) for timestamp, value in entries
            if timestamp > last_processed_timestamp]

  def _make_buffer_reader(self, buffer_state):
    return OLSBufferReader(buffer_state)

  def _buffer_trim(self, buffer_state, trim_timestamp,
                   previous_trim_timestamp):
    lower_bound = (previous_trim_timestamp
                   if previous_trim_timestamp else _EPOCH)
    buffer_state.clear_range(lower_bound, trim_timestamp + _ONE_MICRO)

  def _read_next_unprocessed(self, buffer_state, after_timestamp):
    for timestamp, _ in buffer_state.read_range(
        after_timestamp, MAX_TIMESTAMP):
      if timestamp > after_timestamp:
        return timestamp
    return None

  def process(
      self,
      element,
      buffer=beam.DoFn.StateParam(BUFFER),
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      min_wm_timer=beam.DoFn.TimerParam(MIN_WM_TIMER),
      element_timestamp=beam.DoFn.TimestampParam,
      extra_state=beam.DoFn.StateParam(EXTRA_STATE)):
    return self._do_process(
        element, buffer, tb_state,
        min_wm_timer, element_timestamp,
        extra_state=extra_state)

  @on_timer(MIN_WM_TIMER)
  def on_min_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      mid_wm_timer=beam.DoFn.TimerParam(MID_WM_TIMER),
      max_wm_timer=beam.DoFn.TimerParam(MAX_WM_TIMER),
      batch_timer=beam.DoFn.TimerParam(BATCH_TIMER),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_min_wm(
        tb_state, mid_wm_timer, max_wm_timer,
        batch_timer, fire_timestamp)

  @on_timer(MID_WM_TIMER)
  def on_mid_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_wm_probe(tb_state, fire_timestamp)

  @on_timer(MAX_WM_TIMER)
  def on_max_wm(
      self,
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      fire_timestamp=beam.DoFn.TimestampParam):
    self._do_on_wm_probe(tb_state, fire_timestamp)

  @on_timer(BATCH_TIMER)
  def on_batch(
      self,
      buffer=beam.DoFn.StateParam(BUFFER),
      tb_state=beam.DoFn.StateParam(TimestampBufferDoFn.TB_STATE),
      min_wm_timer=beam.DoFn.TimerParam(MIN_WM_TIMER),
      key=beam.DoFn.KeyParam,
      extra_state=beam.DoFn.StateParam(EXTRA_STATE)):
    yield from self._do_on_batch(
        buffer, tb_state, min_wm_timer, key,
        extra_state=extra_state)