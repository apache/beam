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

"""Experimental ``Watch`` transform for the Python SDK.

``Watch`` continuously watches a growing set of outputs for each input element,
calling a user poll function on an interval until a per-input termination
condition fires. It is the engine behind periodic file-discovery and any
periodic polling source.

For every input element the transform runs an independent loop::

    poll -> keep never-seen-before outputs -> emit them (timestamped) ->
    update watermark -> check termination -> wait(poll_interval) -> poll -> ...

The output is an unbounded ``PCollection`` of ``(input, output)`` pairs. Each
output carries the event time the poll function first reported it. Dedup uses a
stable 128-bit hash of the encoded output, so the output coder must be
deterministic for dedup to hold across workers and restarts.

Example::

    from apache_beam.io.watch import Watch, PollResult, after_total_of
    from apache_beam.transforms.window import TimestampedValue
    from apache_beam.utils.timestamp import Duration, Timestamp

    def poll(prefix):
      now = Timestamp.now()
      outputs = [TimestampedValue(prefix + str(i), now) for i in range(3)]
      return PollResult.complete(outputs)

    watched = (inputs
               | Watch.growth_of(poll)
                      .with_poll_interval(Duration(seconds=5))
                      .with_termination_per_input(after_total_of(60)))

This API is experimental and may change in backwards-incompatible ways.
"""

import collections
import dataclasses
import hashlib
import logging
import time
from typing import Any
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from apache_beam import coders
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import NullableCoder
from apache_beam.coders.coders import TimestampCoder
from apache_beam.coders.coders import TupleCoder
from apache_beam.io import iobase
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'Watch',
    'PollResult',
    'PollFn',
    'TerminationCondition',
    'never',
    'after_total_of',
]

_LOGGER = logging.getLogger(__name__)

_HASH_DIGEST_SIZE = 16  # 128-bit digest width.

# ------------------------------------------------------------------------------
# Public API.
# ------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PollResult:
  """Outputs produced by one poll, plus an optional explicit watermark.

  ``watermark`` of ``None`` lets the transform infer the watermark from the
  earliest new output. A watermark of ``MAX_TIMESTAMP`` (set by
  :meth:`complete`) marks the input finished, so polling stops.
  """
  outputs: Tuple[TimestampedValue, ...]
  watermark: Optional[Timestamp] = None

  @property
  def is_complete(self) -> bool:
    return self.watermark == MAX_TIMESTAMP

  @staticmethod
  def _normalize(outputs, timestamp) -> Tuple[TimestampedValue, ...]:
    if timestamp is None:
      default_ts = Timestamp.now()
    else:
      default_ts = Timestamp.of(timestamp)
    normalized = []
    for output in outputs:
      if isinstance(output, TimestampedValue):
        normalized.append(output)
      else:
        normalized.append(TimestampedValue(output, default_ts))
    return tuple(normalized)

  @staticmethod
  def incomplete(outputs: Iterable, timestamp=None) -> 'PollResult':
    """Reports outputs and expects more; the transform infers the watermark.

    A raw (non-:class:`TimestampedValue`) output is stamped with ``timestamp``
    when given, else with the current processing time.
    """
    return PollResult(PollResult._normalize(outputs, timestamp), watermark=None)

  @staticmethod
  def complete(outputs: Iterable, timestamp=None) -> 'PollResult':
    """Reports the final outputs for an input, after which polling stops.

    A raw (non-:class:`TimestampedValue`) output is stamped with ``timestamp``
    when given, else with the current processing time.
    """
    return PollResult(
        PollResult._normalize(outputs, timestamp), watermark=MAX_TIMESTAMP)

  def with_watermark(self, watermark) -> 'PollResult':
    return dataclasses.replace(self, watermark=Timestamp.of(watermark))


class PollFn(object):
  """Optional base for a poll function ``input -> PollResult``.

  Any callable with that signature works; subclass only to attach an output
  coder hint via :meth:`default_output_coder`.
  """
  def __call__(self, element: Any) -> PollResult:
    raise NotImplementedError

  def default_output_coder(self) -> Optional[Coder]:
    return None


class TerminationCondition(object):
  """Per-input stop policy with immutable, encodable state.

  Hooks follow the lifecycle of one input's polling loop. ``state`` flows from
  :meth:`for_new_input` through the per-round hooks and is serialized with
  :meth:`state_coder`.
  """
  def for_new_input(self, now: Timestamp, element: Any) -> Any:
    raise NotImplementedError

  def on_seen_new_output(self, now: Timestamp, state: Any) -> Any:
    return state

  def on_poll_complete(self, state: Any) -> Any:
    return state

  def can_stop_polling(self, now: Timestamp, state: Any) -> bool:
    raise NotImplementedError

  def state_coder(self) -> Coder:
    raise NotImplementedError


class _Never(TerminationCondition):
  """Polls until the poll function returns :meth:`PollResult.complete`."""
  def for_new_input(self, now, element):
    return 0

  def can_stop_polling(self, now, state):
    return False

  def state_coder(self):
    return coders.VarIntCoder()


class _AfterTotalOf(TerminationCondition):
  """Stops once the wall-clock time since the input was first seen exceeds a
  fixed duration."""
  def __init__(self, duration: Duration):
    self._duration_micros = duration.micros

  def for_new_input(self, now, element):
    return (now, self._duration_micros)

  def can_stop_polling(self, now, state):
    start, duration_micros = state
    return (now - start).micros > duration_micros

  def state_coder(self):
    return TupleCoder([TimestampCoder(), coders.VarIntCoder()])


def never() -> TerminationCondition:
  """Polls until :meth:`PollResult.complete`."""
  return _Never()


def after_total_of(duration) -> TerminationCondition:
  """Stops polling an input after ``duration`` (a :class:`Duration` or seconds)
  has elapsed since it was first seen."""
  return _AfterTotalOf(_as_duration(duration))


# ------------------------------------------------------------------------------
# Restriction state.
# ------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class _PollingGrowthState:
  """Keep-polling state: emitted-output hashes, watermark, termination state.

  ``completed`` maps a 16-byte output hash to the event time it was first seen.
  It is insertion-ordered and treated as immutable; the tracker builds a new
  mapping for each residual.
  """
  completed: 'collections.OrderedDict[bytes, Timestamp]'
  poll_watermark: Optional[Timestamp]
  termination_state: Any


@dataclasses.dataclass(frozen=True)
class _NonPollingGrowthState:
  """Replay-then-stop state: the outputs already emitted this round.

  Produced as the checkpoint primary so a bundle retry re-emits exactly those
  outputs.
  """
  pending: PollResult


_GrowthState = Any  # Union[_PollingGrowthState, _NonPollingGrowthState]

# ------------------------------------------------------------------------------
# Coders.
# ------------------------------------------------------------------------------


class _HashCode128Coder(Coder):
  """Fixed-width coder for a 16-byte output hash.

  Encodes and decodes exactly 16 bytes and raises on any other length, so a
  corrupt restriction surfaces at decode time.
  """
  def encode(self, value: bytes) -> bytes:
    if len(value) != _HASH_DIGEST_SIZE:
      raise ValueError(
          'hash must be %d bytes, got %d' % (_HASH_DIGEST_SIZE, len(value)))
    return value

  def decode(self, encoded: bytes) -> bytes:
    if len(encoded) != _HASH_DIGEST_SIZE:
      raise ValueError(
          'hash must be %d bytes, got %d' % (_HASH_DIGEST_SIZE, len(encoded)))
    return encoded

  def is_deterministic(self) -> bool:
    return True


class _TimestampedValueCoder(Coder):
  """Coder for :class:`TimestampedValue`.

  The Python SDK ships no coder for ``TimestampedValue``, so this encodes the
  ``(value, timestamp)`` pair with a :class:`TupleCoder` and rebuilds the
  ``TimestampedValue`` on decode.
  """
  def __init__(self, value_coder: Coder):
    self._tuple_coder = TupleCoder([value_coder, TimestampCoder()])

  def encode(self, value: TimestampedValue) -> bytes:
    return self._tuple_coder.encode((value.value, value.timestamp))

  def decode(self, encoded: bytes) -> TimestampedValue:
    value, timestamp = self._tuple_coder.decode(encoded)
    return TimestampedValue(value, timestamp)

  def is_deterministic(self) -> bool:
    return self._tuple_coder.is_deterministic()


class _GrowthStateCoder(Coder):
  """Encodes a :class:`_PollingGrowthState` or :class:`_NonPollingGrowthState`.

  A ``(tag, payload)`` envelope selects the variant; the payload is a
  variant-specific :class:`TupleCoder`. ``completed`` is encoded as an ordered
  list of ``(hash, timestamp)`` pairs so insertion order survives a round trip.
  This format is internal to the Python SDK.
  """
  def __init__(self, output_coder: Coder, termination: TerminationCondition):
    nullable_ts = NullableCoder(TimestampCoder())
    self._envelope_coder = TupleCoder(
        [coders.VarIntCoder(), coders.BytesCoder()])
    self._polling_coder = TupleCoder([
        termination.state_coder(),
        nullable_ts,
        coders.ListCoder(TupleCoder([_HashCode128Coder(), TimestampCoder()])),
    ])
    self._non_polling_coder = TupleCoder([
        nullable_ts,
        coders.ListCoder(_TimestampedValueCoder(output_coder)),
    ])

  def encode(self, state: _GrowthState) -> bytes:
    if isinstance(state, _PollingGrowthState):
      payload = self._polling_coder.encode((
          state.termination_state,
          state.poll_watermark,
          list(state.completed.items())))
      return self._envelope_coder.encode((0, payload))
    payload = self._non_polling_coder.encode(
        (state.pending.watermark, list(state.pending.outputs)))
    return self._envelope_coder.encode((1, payload))

  def decode(self, encoded: bytes) -> _GrowthState:
    tag, payload = self._envelope_coder.decode(encoded)
    if tag == 0:
      termination_state, poll_watermark, items = self._polling_coder.decode(
          payload)
      return _PollingGrowthState(
          collections.OrderedDict(items), poll_watermark, termination_state)
    if tag == 1:
      watermark, outputs = self._non_polling_coder.decode(payload)
      return _NonPollingGrowthState(PollResult(tuple(outputs), watermark))
    raise ValueError('unknown Watch growth state tag: %r' % (tag, ))

  def is_deterministic(self) -> bool:
    return False


# ------------------------------------------------------------------------------
# Restriction tracker.
# ------------------------------------------------------------------------------


class _GrowthRestrictionTracker(iobase.RestrictionTracker):
  """Drives one input's polling loop.

  ``process()`` only sees a ``RestrictionTrackerView`` whose ``try_claim``
  returns a bool, so the poll happens inside ``try_claim`` and its result is
  returned through a two-slot holder list passed as the claim position:
  ``holder[0]`` carries the input element in, ``holder[1]`` carries the work
  out. At most one claim succeeds per ``process()``.

  The poll runs while the tracker lock is held, so a ``PollFn`` must be bounded
  or timeout-safe; a blocking poll delays runner-initiated checkpoints.
  """
  def __init__(
      self,
      restriction: _GrowthState,
      poll_fn: Callable[[Any], PollResult],
      key_coder: Coder,
      termination: TerminationCondition,
      now_fn: Callable[[], float]):
    self._restriction = restriction
    self._poll_fn = poll_fn
    self._key_coder = key_coder
    self._termination = termination
    self._now = now_fn
    self._should_stop = False
    self._primary = None  # type: Optional[_GrowthState]
    self._residual = None  # type: Optional[_GrowthState]

  def current_restriction(self) -> _GrowthState:
    return self._restriction

  def _hash_output(self, value: Any) -> bytes:
    return hashlib.blake2b(
        self._key_coder.encode(value), digest_size=_HASH_DIGEST_SIZE).digest()

  def try_claim(self, holder: list) -> bool:
    """Performs one poll round (or one replay) and reports it via ``holder``.

    Returns ``False`` only when a checkpoint already stopped this invocation,
    in which case ``process()`` must emit nothing.
    """
    if self._should_stop:
      return False
    restriction = self._restriction
    if isinstance(restriction, _NonPollingGrowthState):
      holder[1] = ('replay', restriction.pending)
      self._should_stop = True
      return True

    element = holder[0]
    now = Timestamp.of(self._now())
    result = self._poll_fn(element)

    new_outputs = []  # type: List[TimestampedValue]
    claimed = []  # type: List[Tuple[bytes, Timestamp]]
    seen_this_round = set()  # type: set
    for output in result.outputs:
      key_hash = self._hash_output(output.value)
      if key_hash in restriction.completed or key_hash in seen_this_round:
        continue
      seen_this_round.add(key_hash)
      new_outputs.append(output)
      claimed.append((key_hash, output.timestamp))
    new_outputs.sort(key=lambda output: output.timestamp)

    termination_state = restriction.termination_state
    if new_outputs:
      termination_state = self._termination.on_seen_new_output(
          now, termination_state)
    termination_state = self._termination.on_poll_complete(termination_state)

    if result.watermark is not None:
      watermark = result.watermark
    elif new_outputs:
      watermark = new_outputs[0].timestamp
    else:
      watermark = None

    # A watermark at MAX means no more output is possible, so polling stops.
    reached_max = watermark is not None and watermark >= MAX_TIMESTAMP
    stop = (
        result.is_complete or reached_max or
        self._termination.can_stop_polling(now, termination_state))

    self._primary = _NonPollingGrowthState(
        PollResult(tuple(new_outputs), watermark))
    if stop:
      # Terminal round: no polling work remains, so a checkpoint (runner-
      # initiated or via defer_remainder) resumes a state that emits nothing.
      self._residual = _NonPollingGrowthState(PollResult((), watermark))
    else:
      merged = collections.OrderedDict(restriction.completed)
      for key_hash, first_seen in claimed:
        merged[key_hash] = first_seen
      residual_watermark = self._max_watermark(
          restriction.poll_watermark, watermark)
      self._residual = _PollingGrowthState(
          merged, residual_watermark, termination_state)
    holder[1] = ('poll', new_outputs, watermark, stop)
    self._should_stop = True
    return True

  @staticmethod
  def _max_watermark(left: Optional[Timestamp],
                     right: Optional[Timestamp]) -> Optional[Timestamp]:
    if left is None:
      return right
    if right is None:
      return left
    return max(left, right)

  def try_split(self, fraction_of_remainder):
    # Only self-checkpoint (fraction 0) is supported; decline dynamic splits.
    if fraction_of_remainder != 0:
      return None
    if self._primary is None:
      # No claim happened this invocation: keep the whole state as the residual.
      primary = _NonPollingGrowthState(PollResult((), None))
      residual = self._restriction
      self._restriction = primary
      self._should_stop = True
      return primary, residual
    primary, residual = self._primary, self._residual
    self._restriction = primary
    self._should_stop = True
    return primary, residual

  def check_done(self) -> bool:
    # Called after every process(); the single claim or a split sets the flag.
    if self._should_stop:
      return True
    raise ValueError(
        'Watch restriction was neither claimed nor checkpointed: %r' %
        (self._restriction, ))

  def current_progress(self) -> 'iobase.RestrictionProgress':
    if self._should_stop:
      return iobase.RestrictionProgress(completed=1.0, remaining=0.0)
    return iobase.RestrictionProgress(completed=0.0, remaining=1.0)

  def is_bounded(self) -> bool:
    # A polling restriction is unbounded; a replay-then-stop one is bounded.
    return isinstance(self._restriction, _NonPollingGrowthState)


# ------------------------------------------------------------------------------
# Splittable DoFn (its own restriction provider).
# ------------------------------------------------------------------------------


class _WatchGrowthDoFn(core.DoFn, core.RestrictionProvider):
  """Polling SDF that emits ``(input, output)`` pairs.

  The DoFn is its own ``RestrictionProvider``: ``RestrictionParam()`` with no
  argument resolves the provider to the DoFn instance, so the provider methods
  read the transform-level spec (poll function, coders, termination) off
  ``self``. Provider methods run on a separately deserialized copy and before
  ``setup()``, so the spec is immutable state set in ``__init__``.
  """
  def __init__(
      self,
      poll_fn: Callable[[Any], PollResult],
      termination: TerminationCondition,
      poll_interval: Duration,
      output_coder: Coder,
      now_fn: Optional[Callable[[], float]] = None):
    self._poll_fn = poll_fn
    self._termination = termination
    self._poll_interval = poll_interval
    self._output_coder = output_coder
    self._key_coder = output_coder
    self._now = now_fn or time.time
    self._restriction_coder = _GrowthStateCoder(output_coder, termination)

  def initial_restriction(self, element) -> _PollingGrowthState:
    now = Timestamp.of(self._now())
    return _PollingGrowthState(
        collections.OrderedDict(),
        None,
        self._termination.for_new_input(now, element))

  def create_tracker(self, restriction) -> _GrowthRestrictionTracker:
    return _GrowthRestrictionTracker(
        restriction,
        self._poll_fn,
        self._key_coder,
        self._termination,
        self._now)

  def split(self, element, restriction):
    # Watch fans out by input element, so each restriction stays whole.
    yield restriction

  def restriction_coder(self) -> Coder:
    return self._restriction_coder

  def restriction_size(self, element, restriction) -> int:
    return 1

  def truncate(self, element, restriction):
    # On drain, replay a pending NonPolling state and stop further polling.
    if isinstance(restriction, _NonPollingGrowthState):
      return restriction
    return None

  @core.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      timestamp=core.DoFn.TimestampParam,
      tracker=core.DoFn.RestrictionParam(),
      watermark_estimator=core.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    assert isinstance(tracker, sdf_utils.RestrictionTrackerView)
    holder = [element, None]
    if not tracker.try_claim(holder):
      # A checkpoint already stopped this invocation; emit nothing.
      return
    # Seed the watermark hold from the input event time after the claim.
    _set_watermark_if_greater(watermark_estimator, timestamp)
    work = holder[1]
    if work[0] == 'replay':
      for output in work[1].outputs:
        yield TimestampedValue((element, output.value), output.timestamp)
      return
    new_outputs, watermark, stop = work[1], work[2], work[3]
    for output in new_outputs:
      yield TimestampedValue((element, output.value), output.timestamp)
    if stop:
      # The input is finished, so release the watermark hold to MAX.
      _set_watermark_if_greater(watermark_estimator, MAX_TIMESTAMP)
      return
    if watermark is not None:
      _set_watermark_if_greater(watermark_estimator, watermark)
    tracker.defer_remainder(self._poll_interval)


def _set_watermark_if_greater(watermark_estimator, new_watermark) -> None:
  # set_watermark raises on regression, so only ever advance the watermark.
  current = watermark_estimator.current_watermark()
  if current is None or new_watermark > current:
    watermark_estimator.set_watermark(new_watermark)


# ------------------------------------------------------------------------------
# Public PTransform.
# ------------------------------------------------------------------------------


class Watch(PTransform):
  """Watches a growing set of outputs per input via a periodic poll function.

  Build with :meth:`growth_of` and the ``with_*`` methods. The output is an
  unbounded ``PCollection`` of ``(input, output)`` pairs.
  """
  def __init__(
      self,
      poll_fn: Callable[[Any], PollResult],
      termination: Optional[TerminationCondition] = None,
      poll_interval: Optional[Duration] = None,
      output_coder: Optional[Coder] = None,
      now_fn: Optional[Callable[[], float]] = None):
    super().__init__()
    self._poll_fn = poll_fn
    self._termination = termination or never()
    self._poll_interval = poll_interval
    self._output_coder = output_coder
    self._now = now_fn

  @classmethod
  def growth_of(cls, poll_fn: Callable[[Any], PollResult]) -> 'Watch':
    return cls(poll_fn)

  def _replace(self, **changes) -> 'Watch':
    spec = dict(
        poll_fn=self._poll_fn,
        termination=self._termination,
        poll_interval=self._poll_interval,
        output_coder=self._output_coder,
        now_fn=self._now)
    spec.update(changes)
    return Watch(**spec)

  def with_poll_interval(self, poll_interval) -> 'Watch':
    return self._replace(poll_interval=_as_duration(poll_interval))

  def with_termination_per_input(
      self, termination: TerminationCondition) -> 'Watch':
    return self._replace(termination=termination)

  def with_output_coder(self, output_coder: Coder) -> 'Watch':
    return self._replace(output_coder=output_coder)

  def expand(self, pcoll):
    if self._poll_interval is None:
      raise ValueError('Watch requires with_poll_interval(...)')
    output_coder = self._output_coder
    if output_coder is None:
      hint = self._poll_fn.default_output_coder() if isinstance(
          self._poll_fn, PollFn) else None
      output_coder = hint or coders.PickleCoder()
    if not output_coder.is_deterministic():
      _LOGGER.warning(
          'Watch dedup uses a non-deterministic output coder (%s); equal '
          'outputs may be emitted more than once. Pass a deterministic coder '
          'via with_output_coder() for reliable dedup.',
          type(output_coder).__name__)
    return pcoll | core.ParDo(
        _WatchGrowthDoFn(
            self._poll_fn,
            self._termination,
            self._poll_interval,
            output_coder,
            self._now))


def _as_duration(value) -> Duration:
  return value if isinstance(value, Duration) else Duration(value)
