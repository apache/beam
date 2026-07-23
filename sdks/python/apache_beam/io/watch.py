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
output carries the event time the poll function first reported it. Dedup
hashes each output's key: the output itself by default, or
``output_key_fn(output)`` when one is given. The key coder is inferred when
not passed explicitly and converted to its deterministic form, so equal keys
hash equally across workers and restarts.

Example::

    from apache_beam.io.watch import Watch, PollResult, after_total_of
    from apache_beam.transforms.window import TimestampedValue
    from apache_beam.utils.timestamp import Duration, Timestamp

    def poll(prefix) -> PollResult[str]:
      now = Timestamp.now()
      outputs = [TimestampedValue(prefix + str(i), now) for i in range(3)]
      return PollResult.complete(outputs)

    watched = inputs | Watch(
        poll,
        poll_interval=Duration(seconds=5),
        termination=after_total_of(60))

This API is experimental and may change in backwards-incompatible ways.
"""

import collections
import dataclasses
import hashlib
import inspect
import time
import typing
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import TypeVar

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

_HASH_DIGEST_SIZE = 16  # 128-bit digest width.

OutputT = TypeVar('OutputT')

# ------------------------------------------------------------------------------
# Public API.
# ------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PollResult(Generic[OutputT]):
  """Outputs produced by one poll, plus an optional explicit watermark.

  ``watermark`` of ``None`` lets the transform infer the watermark from the
  earliest new output. A watermark of ``MAX_TIMESTAMP`` (set by
  :meth:`complete`) marks the input finished, so polling stops.

  The ``OutputT`` type parameter can annotate a poll function's return type,
  as in ``-> PollResult[str]``; the transform infers the output coder from it.
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
  coder hint via :meth:`default_output_coder`::

      from apache_beam import coders

      class ListFiles(PollFn):
        def __call__(self, prefix):
          return PollResult.incomplete(list_files(prefix))

        def default_output_coder(self):
          return coders.StrUtf8Coder()

  A plain function can instead annotate its return type as ``PollResult[V]``
  and have the output coder inferred from ``V``.
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


class _GrowthState:
  """Base for the two restriction variants a Watch input can hold."""


@dataclasses.dataclass(frozen=True)
class _PollingGrowthState(_GrowthState):
  """Keep-polling state: emitted-output hashes, watermark, termination state.

  ``completed`` maps a 16-byte output-key hash to the event time it was first
  seen. It is insertion-ordered and treated as immutable; a new mapping is
  built for each residual.
  """
  completed: 'collections.OrderedDict[bytes, Timestamp]'
  poll_watermark: Optional[Timestamp]
  termination_state: Any


@dataclasses.dataclass(frozen=True)
class _NonPollingGrowthState(_GrowthState):
  """Replay-then-stop state: the outputs already emitted this round.

  Produced as the checkpoint primary so a bundle retry re-emits exactly those
  outputs.
  """
  pending: PollResult


# Primary used when a checkpoint arrives before any claim; replays nothing.
_EMPTY_STATE = _NonPollingGrowthState(PollResult((), None))

# ------------------------------------------------------------------------------
# Coders.
# ------------------------------------------------------------------------------


class _TimestampedValueCoder(Coder):
  """Coder for :class:`TimestampedValue`.

  ``TimestampedValue`` is normally unwrapped into a ``WindowedValue`` on the
  wire, so the SDK ships no standalone coder for it. Watch keeps it inside the
  restriction state, so this encodes the ``(value, timestamp)`` pair with a
  :class:`TupleCoder` and rebuilds the ``TimestampedValue`` on decode.
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
        coders.ListCoder(TupleCoder([coders.BytesCoder(), TimestampCoder()])),
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


def _identity(value: Any) -> Any:
  return value


def _hash_output(key_coder: Coder, value: Any) -> bytes:
  return hashlib.blake2b(
      key_coder.encode(value), digest_size=_HASH_DIGEST_SIZE).digest()


def _max_watermark(left: Optional[Timestamp],
                   right: Optional[Timestamp]) -> Optional[Timestamp]:
  if left is None:
    return right
  if right is None:
    return left
  return max(left, right)


def _never_seen_before(
    restriction: _PollingGrowthState,
    result: PollResult,
    key_fn: Callable[[Any], Any],
    key_coder: Coder) -> PollResult:
  """Filters a poll result down to outputs whose key was never seen before.

  Dedup hashes ``key_fn(output.value)`` against the restriction's completed
  set, also dropping in-round duplicates. Outputs are sorted by timestamp so
  the earliest one can serve as the inferred watermark.
  """
  new_outputs = []
  seen_this_round = set()
  for output in result.outputs:
    key_hash = _hash_output(key_coder, key_fn(output.value))
    if key_hash in restriction.completed or key_hash in seen_this_round:
      continue
    seen_this_round.add(key_hash)
    new_outputs.append(output)
  new_outputs.sort(key=lambda output: output.timestamp)
  return dataclasses.replace(result, outputs=tuple(new_outputs))


class _GrowthRestrictionTracker(iobase.RestrictionTracker):
  """Tracks one input's polling restriction over claimed poll rounds.

  The claimed position is one poll round: a ``(PollResult, termination_state)``
  pair whose ``PollResult`` holds only never-seen-before outputs. ``process()``
  polls and dedups before claiming, so a slow poll never holds the tracker
  lock; the tracker validates each claim against the restriction and derives
  the checkpoint split from the claimed round in :meth:`try_split`.
  """
  def __init__(
      self,
      restriction: _GrowthState,
      key_fn: Callable[[Any], Any],
      key_coder: Coder):
    self._restriction = restriction
    self._key_fn = key_fn
    self._key_coder = key_coder
    self._claimed_result = None  # type: Optional[PollResult]
    self._claimed_termination_state = None  # type: Any
    self._claimed_hashes = None  # type: Optional[collections.OrderedDict]
    self._should_stop = False

  def _hash(self, value: Any) -> bytes:
    return _hash_output(self._key_coder, self._key_fn(value))

  def current_restriction(self) -> _GrowthState:
    return self._restriction

  def try_claim(self, position: Tuple[PollResult, Any]) -> bool:
    """Claims one poll round; at most one claim succeeds per ``process()``.

    The claim is rejected after a checkpoint already stopped this invocation,
    when a claimed output key was already completed, or when a replay does not
    match the pending outputs exactly.
    """
    if self._should_stop:
      return False
    result, termination_state = position
    claimed_hashes = collections.OrderedDict()
    for output in result.outputs:
      claimed_hashes[self._hash(output.value)] = output.timestamp
    if isinstance(self._restriction, _PollingGrowthState):
      if any(key_hash in self._restriction.completed
             for key_hash in claimed_hashes):
        return False
    else:
      expected = set(
          self._hash(output.value)
          for output in self._restriction.pending.outputs)
      if expected != set(claimed_hashes):
        return False
    self._should_stop = True
    self._claimed_result = result
    self._claimed_termination_state = termination_state
    self._claimed_hashes = claimed_hashes
    return True

  def try_split(self, fraction_of_remainder):
    # Every split checkpoints at the claimed poll round; splitting a round
    # further is not supported.
    if self._claimed_result is None:
      # No claim happened this invocation: the residual is all the work and
      # the primary replays nothing.
      residual = self._restriction
      self._restriction = _EMPTY_STATE
    elif isinstance(self._restriction, _NonPollingGrowthState):
      # The claimed replay was the entire restriction, so nothing remains.
      residual = _EMPTY_STATE
    else:
      # The primary becomes a replay of the claimed round; the residual
      # resumes polling with the claimed keys marked completed.
      merged = collections.OrderedDict(self._restriction.completed)
      merged.update(self._claimed_hashes)
      residual = _PollingGrowthState(
          merged,
          _max_watermark(
              self._restriction.poll_watermark, self._claimed_result.watermark),
          self._claimed_termination_state)
      self._restriction = _NonPollingGrowthState(self._claimed_result)
    self._should_stop = True
    return self._restriction, residual

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
      key_fn: Callable[[Any], Any],
      key_coder: Coder,
      now_fn: Optional[Callable[[], float]] = None):
    self._poll_fn = poll_fn
    self._termination = termination
    self._poll_interval = poll_interval
    self._output_coder = output_coder
    self._key_fn = key_fn
    self._key_coder = key_coder
    self._now = now_fn or time.time
    self._restriction_coder = _GrowthStateCoder(output_coder, termination)

  def initial_restriction(self, element) -> _PollingGrowthState:
    now = Timestamp.of(self._now())
    return _PollingGrowthState(
        collections.OrderedDict(),
        None,
        self._termination.for_new_input(now, element))

  def create_tracker(self, restriction) -> _GrowthRestrictionTracker:
    return _GrowthRestrictionTracker(restriction, self._key_fn, self._key_coder)

  def restriction_coder(self) -> Coder:
    return self._restriction_coder

  def restriction_size(self, element, restriction) -> int:
    return 1

  @core.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      timestamp=core.DoFn.TimestampParam,
      tracker=core.DoFn.RestrictionParam(),
      watermark_estimator=core.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    assert isinstance(tracker, sdf_utils.RestrictionTrackerView)
    # Java seeds the manual estimator with the element timestamp; the Python
    # default provider starts at None, which a runner reads as MIN_TIMESTAMP
    # and would pin the stage's output watermark until the first output.
    if watermark_estimator.current_watermark() is None:
      watermark_estimator.set_watermark(timestamp)
    restriction = tracker.current_restriction()
    if isinstance(restriction, _NonPollingGrowthState):
      # Replay the outputs already emitted this round, then stop. No poll.
      if not tracker.try_claim((restriction.pending, None)):
        return
      for output in restriction.pending.outputs:
        yield TimestampedValue((element, output.value), output.timestamp)
      return
    # Poll before claiming so a slow poll never holds the tracker lock, which
    # would block runner progress checks and checkpoints.
    result = self._poll_fn(element)
    # Read the clock after the poll so a slow poll counts against termination.
    now = Timestamp.of(self._now())
    new_results = _never_seen_before(
        restriction, result, self._key_fn, self._key_coder)
    termination_state = restriction.termination_state
    if new_results.outputs:
      termination_state = self._termination.on_seen_new_output(
          now, termination_state)
    termination_state = self._termination.on_poll_complete(termination_state)
    if not tracker.try_claim((new_results, termination_state)):
      # A checkpoint already stopped this invocation; emit nothing.
      return
    for output in new_results.outputs:
      yield TimestampedValue((element, output.value), output.timestamp)
    if new_results.watermark is not None:
      watermark = new_results.watermark
    elif new_results.outputs:
      # Outputs are timestamp-sorted, so the first one is the earliest.
      watermark = new_results.outputs[0].timestamp
    else:
      watermark = None
    if self._termination.can_stop_polling(now, termination_state):
      return
    if watermark is not None and watermark >= MAX_TIMESTAMP:
      # No more output is possible (PollResult.complete), so polling stops.
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


def _return_type(fn) -> Any:
  """The return type annotation of ``fn`` or its ``__call__``, else ``Any``."""
  target = fn if inspect.isroutine(fn) else getattr(type(fn), '__call__', None)
  if target is None:
    return Any
  try:
    hints = typing.get_type_hints(target)
  except (NameError, TypeError):
    return Any
  return hints.get('return', Any)


def _poll_output_type(poll_fn) -> Any:
  """The ``V`` of a ``PollResult[V]`` return annotation on ``poll_fn``.

  This mirrors the Java SDK, which infers the output coder from the
  ``PollFn``'s ``OutputT`` type parameter. Returns ``Any`` when ``poll_fn``
  carries no such annotation.
  """
  hint = _return_type(poll_fn)
  if typing.get_origin(hint) is PollResult:
    args = typing.get_args(hint)
    if len(args) == 1:
      return args[0]
  return Any


class Watch(PTransform):
  """Watches a growing set of outputs per input via a periodic poll function.

  The output is an unbounded ``PCollection`` of ``(input, output)`` pairs.

  Args:
    poll_fn: callable ``input -> PollResult``, invoked once per poll round.
    poll_interval: delay between two poll rounds for one input, as a
      :class:`Duration` or in seconds.
    termination: per-input stop policy; defaults to :func:`never`.
    output_coder: coder for the poll outputs, used to keep them in the
      restriction state. Inferred when omitted: from a :class:`PollFn`'s
      :meth:`~PollFn.default_output_coder`, else from the registered coder for
      the ``V`` of a ``PollResult[V]`` return annotation on ``poll_fn``.
    output_key_fn: derives the dedup key from an output; an output is emitted
      only when its key was never seen before. Defaults to the output itself.
    output_key_coder: coder whose encoding of the key is hashed for dedup;
      inferred like ``output_coder`` when omitted. It is converted with
      ``as_deterministic_coder`` so equal keys always hash equally; a coder
      with no deterministic form is rejected.
    now_fn: clock used for termination decisions; tests can inject one.
  """
  def __init__(
      self,
      poll_fn: Callable[[Any], PollResult],
      poll_interval,
      termination: Optional[TerminationCondition] = None,
      output_coder: Optional[Coder] = None,
      output_key_fn: Optional[Callable[[Any], Any]] = None,
      output_key_coder: Optional[Coder] = None,
      now_fn: Optional[Callable[[], float]] = None):
    super().__init__()
    if poll_interval is None:
      raise ValueError('Watch requires a poll_interval')
    self._poll_fn = poll_fn
    self._poll_interval = _as_duration(poll_interval)
    self._termination = termination or never()
    self._output_coder = output_coder
    self._output_key_fn = output_key_fn
    self._output_key_coder = output_key_coder
    self._now = now_fn

  def expand(self, pcoll):
    output_coder = self._output_coder
    if output_coder is None and isinstance(self._poll_fn, PollFn):
      output_coder = self._poll_fn.default_output_coder()
    if output_coder is None:
      output_coder = coders.registry.get_coder(_poll_output_type(self._poll_fn))
    if self._output_key_fn is None:
      # The output is its own dedup key, so the key coder is the output coder.
      key_fn = _identity
      key_coder = self._output_key_coder or output_coder
    else:
      key_fn = self._output_key_fn
      key_coder = self._output_key_coder or coders.registry.get_coder(
          _return_type(self._output_key_fn))
    # Dedup hashes the encoded key, so equal keys must encode equally; use the
    # coder's deterministic form and reject coders that have none.
    key_coder = key_coder.as_deterministic_coder(
        self.label,
        'Watch dedups by hashing the encoded output key, so the key coder '
        'must be deterministic. %s has no deterministic form; pass a '
        'deterministic output_key_coder (or output_coder).' %
        type(key_coder).__name__)
    # Type the (input, output) pairs from the input type and the resolved
    # coder's type, so downstream transforms are typed and coder inference does
    # not fall back to pickling.
    input_type = pcoll.element_type or Any
    try:
      value_type = output_coder.to_type_hint()
    except NotImplementedError:
      value_type = Any
    return pcoll | core.ParDo(
        _WatchGrowthDoFn(
            self._poll_fn,
            self._termination,
            self._poll_interval,
            output_coder,
            key_fn,
            key_coder,
            self._now)).with_output_types(Tuple[input_type, value_type])


def _as_duration(value) -> Duration:
  return value if isinstance(value, Duration) else Duration(value)
