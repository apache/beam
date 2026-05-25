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

"""A minimal, self-contained ``UnboundedSource`` for the Python SDK.

This module is a Week-1 proof-of-concept for GSoC 2026 (issue #19137). It brings
the Java ``UnboundedSource`` abstractions to Python and makes them *runnable* on
the portable Fn API path (e.g. the default DirectRunner) by dispatching reads
through a Splittable ``DoFn`` -- mirroring Java's
``Read.UnboundedSourceAsSDFWrapperFn``.

Public API::

    from apache_beam.io.unbounded_source import (
        UnboundedSource, UnboundedReader, CheckpointMark, ReadFromUnboundedSource)

    class MySource(UnboundedSource):
      ...

    with beam.Pipeline() as p:
      p | ReadFromUnboundedSource(MySource()) | beam.Map(print)

Scope (deliberately minimal): read loop, event-time timestamps, monotonic
watermark reporting, checkpoint-based pause/resume (``defer_remainder``) and
bundle finalization. Out of scope for this PoC: record-id deduplication,
backlog-byte reporting, dynamic split fractions, and wiring into
``iobase.Read.expand()`` (callers use ``ReadFromUnboundedSource`` directly). The
design mirrors the in-tree streaming SDF template
``apache_beam.transforms.periodicsequence``.
"""

# pytype: skip-file

import dataclasses
import logging
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from apache_beam import coders
from apache_beam.coders.coders import BooleanCoder
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import NullableCoder
from apache_beam.coders.coders import TimestampCoder
from apache_beam.coders.coders import TupleCoder
from apache_beam.coders.coders import _MemoizingPickleCoder
from apache_beam.io import iobase
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.transforms import Impulse
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'CheckpointMark',
    'UnboundedReader',
    'UnboundedSource',
    'ReadFromUnboundedSource',
]

_LOGGER = logging.getLogger(__name__)

# Placed in the ``try_claim`` holder when the reader has no data *right now*.
# This is NOT end-of-stream -- an unbounded reader may produce more later.
_NO_DATA = object()

_DEFAULT_POLL_INTERVAL_SECONDS = 1.0

# ------------------------------------------------------------------------------
# Public abstract base classes (Java semantics, Python names). Following the
# existing iobase.py style, methods raise NotImplementedError rather than using
# a formal abc.ABC.
# ------------------------------------------------------------------------------


class CheckpointMark(object):
  """A durable, serializable position in an :class:`UnboundedSource`.

  Mirrors ``org.apache.beam.sdk.io.UnboundedSource.CheckpointMark``. Produced by
  :meth:`UnboundedReader.get_checkpoint_mark`, encoded with
  :meth:`UnboundedSource.get_checkpoint_mark_coder`, and used to resume a reader
  (see :meth:`UnboundedSource.create_reader`).
  """
  def finalize_checkpoint(self) -> None:
    """Called once the runner has durably committed work up to this mark.

    Override to acknowledge/commit upstream (e.g. ack Pub/Sub messages). The
    default is a no-op. Unlike Java, the Python bundle finalizer takes no
    deadline argument, so no mapping is required here.
    """
    pass


class UnboundedReader(object):
  """Reads records from an :class:`UnboundedSource`.

  Mirrors ``UnboundedSource.UnboundedReader``. Lifecycle: exactly one
  :meth:`start`, then any number of :meth:`advance` calls; whenever one returns
  ``True`` the current record is available via :meth:`get_current` /
  :meth:`get_current_timestamp`. A ``False`` return means "no data available
  right now", which is distinct from end-of-stream: a reader signals a permanent
  end by reporting a watermark of ``MAX_TIMESTAMP``.
  """
  def start(self) -> bool:
    """Positions at the first record; returns whether one is available."""
    raise NotImplementedError

  def advance(self) -> bool:
    """Advances to the next record. ``False`` == no data *now*, not EOF."""
    raise NotImplementedError

  def get_current(self) -> Any:
    """Returns the record claimed by the last successful start/advance."""
    raise NotImplementedError

  def get_current_timestamp(self) -> Timestamp:
    """Returns the event-time timestamp of the current record."""
    raise NotImplementedError

  def get_watermark(self) -> Timestamp:
    """A best-effort lower bound on timestamps of future records.

    Treated as monotonic by the wrapper. Return ``MAX_TIMESTAMP`` to signal that
    this reader has permanently finished.
    """
    raise NotImplementedError

  def get_checkpoint_mark(self) -> CheckpointMark:
    """Returns a durable mark to resume from. Call only at a bundle boundary."""
    raise NotImplementedError

  def close(self) -> None:
    """Releases reader resources. Default no-op."""
    pass


class UnboundedSource(iobase.SourceBase):
  """A source producing an unbounded stream of records with checkpointing.

  Mirrors ``org.apache.beam.sdk.io.UnboundedSource``. Read it in a pipeline with
  :class:`ReadFromUnboundedSource`::

      p | ReadFromUnboundedSource(MyUnboundedSource())
  """
  def split(self,
            desired_num_splits: int,
            options: Optional[Any] = None) -> Iterable['UnboundedSource']:
    """Splits into at most ``desired_num_splits`` independent sub-sources."""
    raise NotImplementedError

  def create_reader(
      self, options: Optional[Any],
      checkpoint_mark: Optional[CheckpointMark]) -> UnboundedReader:
    """Creates a reader, resuming from ``checkpoint_mark`` when it is not None."""
    raise NotImplementedError

  def get_checkpoint_mark_coder(self) -> Coder:
    """Returns the coder for this source's :class:`CheckpointMark` instances."""
    raise NotImplementedError

  def is_bounded(self) -> bool:
    # SourceBase.is_bounded raises; an unbounded source is, by definition, not.
    return False

  def default_output_coder(self) -> Coder:
    # Permissive default, matching BoundedSource (iobase.py). Override for a
    # tighter coder. Not wired into ReadFromUnboundedSource in this PoC.
    return coders.registry.get_coder(object)


# ------------------------------------------------------------------------------
# SDF wrapper internals (restriction, coder, tracker, provider). Names are
# underscore-prefixed: they are an implementation detail of
# ReadFromUnboundedSource, not public API.
# ------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class _UnboundedSourceRestriction(object):
  """Durable SDF restriction describing where a reader should (re)start.

  Holds only serializable state -- never a live reader. Mirrors Java's
  ``UnboundedSourceRestriction(source, checkpoint, watermark)`` plus an explicit
  ``is_done`` flag for the terminal (MAX-watermark) transition.
  """
  source: UnboundedSource
  checkpoint_mark: Optional[CheckpointMark] = None
  watermark: Timestamp = MIN_TIMESTAMP
  is_done: bool = False


class _UnboundedSourceRestrictionCoder(Coder):
  """Encodes :class:`_UnboundedSourceRestriction`.

  Shape mirrors Java's ``UnboundedSourceRestrictionCoder``: pickled source +
  nullable checkpoint (encoded with the source's own checkpoint coder) +
  watermark + done flag.
  """
  def __init__(self, checkpoint_mark_coder: Optional[Coder] = None):
    nullable_checkpoint = NullableCoder(
        checkpoint_mark_coder or _MemoizingPickleCoder())
    self._tuple_coder = TupleCoder((
        _MemoizingPickleCoder(),  # source
        nullable_checkpoint,  # checkpoint_mark (may be None)
        TimestampCoder(),  # watermark
        BooleanCoder()))  # is_done

  def encode(self, restriction: '_UnboundedSourceRestriction') -> bytes:
    return self._tuple_coder.encode((
        restriction.source,
        restriction.checkpoint_mark,
        restriction.watermark,
        restriction.is_done))

  def decode(self, encoded: bytes) -> '_UnboundedSourceRestriction':
    source, checkpoint_mark, watermark, is_done = self._tuple_coder.decode(
        encoded)
    return _UnboundedSourceRestriction(
        source, checkpoint_mark, watermark, is_done)

  def is_deterministic(self) -> bool:
    # The source and checkpoint are pickled, which is not guaranteed
    # deterministic; matches the bounded SDF restriction coder in iobase.py.
    return False


class _UnboundedSourceRestrictionTracker(iobase.RestrictionTracker):
  """Drives an :class:`UnboundedReader` for one SDF restriction.

  Owns the live reader (lazily created, never serialized): both runner-initiated
  ``try_split`` and the self-checkpoint ``try_split(0)`` raised by
  ``defer_remainder`` must checkpoint the *same* reader.

  ``process()`` only ever sees a ``RestrictionTrackerView`` -- which hides custom
  methods and whose ``try_claim`` returns just a bool -- so the freshly-read
  record is handed back through a one-element holder list passed as the
  ``try_claim`` *position* argument (mirrors Java's
  ``tryClaim(UnboundedSourceValue[] out)``).
  """
  def __init__(
      self,
      restriction: _UnboundedSourceRestriction,
      options: Optional[Any] = None):
    self._restriction = restriction
    self._options = options
    self._reader = None  # type: Optional[UnboundedReader]
    self._started = False
    # True once a checkpoint has been cut this bundle (EOF or self-checkpoint).
    self._checkpoint_taken = False

  def _ensure_reader(self) -> None:
    if self._reader is None:
      self._reader = self._restriction.source.create_reader(
          self._options, self._restriction.checkpoint_mark)

  def current_restriction(self) -> _UnboundedSourceRestriction:
    return self._restriction

  def try_claim(self, out: List[Any]) -> bool:
    # 'out' is a one-element holder -- the only channel back to process(). It
    # receives either (value, timestamp) or the _NO_DATA sentinel.
    if self._restriction.is_done:
      out[0] = _NO_DATA
      return False
    self._ensure_reader()
    if not self._started:
      has_data = self._reader.start()
    else:
      has_data = self._reader.advance()
    self._started = True
    if has_data:
      # A record is available: always emit it. Inspect has_data BEFORE the
      # watermark, because a reader may return its final record and report a
      # MAX_TIMESTAMP watermark on the *same* call (meaning "nothing after
      # this"). That EOF is realized on the next, data-less claim, so the record
      # we just read is never dropped. (We do not touch self._restriction on the
      # data path, so its identity is preserved for the finalization gate.)
      out[0] = (
          self._reader.get_current(), self._reader.get_current_timestamp())
      return True
    watermark = self._reader.get_watermark()
    if watermark >= MAX_TIMESTAMP:
      # No data and watermark at MAX: the reader permanently finished. Cut a
      # final checkpoint, close, and mark the restriction done.
      checkpoint = self._reader.get_checkpoint_mark()
      self._reader.close()
      self._reader = None
      self._restriction = dataclasses.replace(
          self._restriction,
          checkpoint_mark=checkpoint,
          watermark=MAX_TIMESTAMP,
          is_done=True)
      self._checkpoint_taken = True
      out[0] = _NO_DATA
      return False
    # No data right now (not EOF): refresh the watermark so process() can
    # advance it before deferring, then let process() self-checkpoint.
    self._restriction = dataclasses.replace(
        self._restriction, watermark=watermark)
    out[0] = _NO_DATA
    return True

  def try_split(
      self, fraction_of_remainder
  ) -> Optional[Tuple[_UnboundedSourceRestriction,
                      _UnboundedSourceRestriction]]:
    # fraction 0 is the self-checkpoint raised by defer_remainder(); any other
    # fraction cuts the same checkpoint. Returns (primary, residual) or None.
    if self._reader is None or not self._started or self._restriction.is_done:
      return None
    checkpoint = self._reader.get_checkpoint_mark()
    watermark = self._reader.get_watermark()
    # Primary is finished work; it carries the checkpoint only so the DoFn can
    # register finalization. The residual carries the resume state.
    primary = dataclasses.replace(
        self._restriction, checkpoint_mark=checkpoint, is_done=True)
    residual = _UnboundedSourceRestriction(
        source=self._restriction.source,
        checkpoint_mark=checkpoint,
        watermark=watermark,
        is_done=False)
    self._restriction = primary
    self._checkpoint_taken = True
    # The residual's reader is rebuilt from its checkpoint on resume; drop ours.
    self._reader = None
    return primary, residual

  def check_done(self) -> bool:
    # Called after every process(); must raise if work is left unaccounted for.
    if self._restriction.is_done or self._checkpoint_taken:
      return True
    raise ValueError(
        'UnboundedSource restriction was neither finished nor checkpointed; '
        'process() must self-checkpoint via defer_remainder() or run to EOF: '
        '%r' % (self._restriction, ))

  def current_progress(self) -> 'iobase.RestrictionProgress':
    # Backlog-based progress is out of scope; report a coarse done/not-done
    # fraction so the runner has a (recommended) signal.
    return iobase.RestrictionProgress(
        fraction=1.0 if self._restriction.is_done else 0.0)

  def is_bounded(self) -> bool:
    return False


class _UnboundedSourceRestrictionProvider(core.RestrictionProvider):
  """Wraps an :class:`UnboundedSource` element as an SDF restriction."""
  def __init__(
      self,
      checkpoint_mark_coder: Optional[Coder] = None,
      options: Optional[Any] = None):
    self._restriction_coder = _UnboundedSourceRestrictionCoder(
        checkpoint_mark_coder)
    self._options = options

  def initial_restriction(
      self, element: UnboundedSource) -> _UnboundedSourceRestriction:
    if not isinstance(element, UnboundedSource):
      raise TypeError(
          'ReadFromUnboundedSource expected an UnboundedSource element, got %r'
          % (element, ))
    return _UnboundedSourceRestriction(source=element)

  def create_tracker(
      self, restriction: _UnboundedSourceRestriction
  ) -> _UnboundedSourceRestrictionTracker:
    return _UnboundedSourceRestrictionTracker(
        restriction, options=self._options)

  def split(self, element,
            restriction) -> Iterable[_UnboundedSourceRestriction]:
    # Minimal PoC: no initial fan-out. Real splitting is future work.
    yield restriction

  def restriction_size(self, element, restriction) -> int:
    # Backlog estimation is out of scope; report a constant non-negative size.
    return 1

  def restriction_coder(self) -> Coder:
    return self._restriction_coder

  def truncate(self, element, restriction):
    # On drain, stop emitting new records (mirrors PeriodicSequence.truncate).
    return None


def _set_watermark_if_greater(watermark_estimator, new_watermark: Timestamp):
  # ManualWatermarkEstimator.set_watermark raises if the watermark regresses, so
  # only ever advance it (mirrors PeriodicSequence's monotonic guard).
  current = watermark_estimator.current_watermark()
  if current is None or new_watermark > current:
    watermark_estimator.set_watermark(new_watermark)


class ReadFromUnboundedSource(PTransform):
  """Reads an :class:`UnboundedSource` via a Splittable ``DoFn``.

  Dispatches through an SDF that mirrors Java's
  ``Read.UnboundedSourceAsSDFWrapperFn``: checkpoint-based pause/resume
  (``defer_remainder``), monotonic watermark reporting, and bundle finalization.
  Runs on the portable Fn API path -- the default DirectRunner routes an
  ``Impulse | Map | ParDo`` pipeline to Prism/FnApiRunner::

      p | ReadFromUnboundedSource(MyUnboundedSource())
  """
  def __init__(
      self,
      source: UnboundedSource,
      poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS):
    if not isinstance(source, UnboundedSource):
      raise TypeError('source must be an UnboundedSource, got %r' % (source, ))
    super().__init__()
    self._source = source
    self._poll_interval_seconds = poll_interval_seconds

  def expand(self, pbegin):
    source = self._source
    poll_interval_seconds = self._poll_interval_seconds
    provider = _UnboundedSourceRestrictionProvider(
        checkpoint_mark_coder=source.get_checkpoint_mark_coder())

    class _ReadFromUnboundedSourceDoFn(core.DoFn):
      @core.DoFn.unbounded_per_element()
      def process(
          self,
          unused_element,
          bundle_finalizer=core.DoFn.BundleFinalizerParam,
          tracker=core.DoFn.RestrictionParam(provider),
          watermark_estimator=core.DoFn.WatermarkEstimatorParam(
              ManualWatermarkEstimator.default_provider())):
        # Parameter order matters: positionally-injected params (the element and
        # the bundle finalizer) must precede the kwarg-injected ones (the
        # restriction tracker and watermark estimator), which the SDF invoker
        # passes by name (runners/common.py _get_arg_placeholders).
        assert isinstance(tracker, sdf_utils.RestrictionTrackerView)
        initial = tracker.current_restriction()
        try:
          while True:
            holder = [None]
            if not tracker.try_claim(holder):
              break  # restriction done (EOF) -> stop
            record = holder[0]
            if record is _NO_DATA:
              # No data right now: advance the watermark and self-checkpoint so
              # the runner reschedules us later. Resume via defer_remainder() +
              # break -- NOT yield ProcessContinuation (the portable SDF path).
              _set_watermark_if_greater(
                  watermark_estimator, tracker.current_restriction().watermark)
              tracker.defer_remainder(Duration(seconds=poll_interval_seconds))
              break
            value, record_timestamp = record
            _set_watermark_if_greater(watermark_estimator, record_timestamp)
            yield TimestampedValue(value, record_timestamp)
        finally:
          current = tracker.current_restriction()
          checkpoint = current.checkpoint_mark
          # Register finalization only when a real checkpoint was cut this
          # bundle. Restriction identity (`current is not initial`) mirrors
          # Java's reference-equality gate in Read.java.
          if current is not initial and checkpoint is not None:
            bundle_finalizer.register(checkpoint.finalize_checkpoint)

    return (
        pbegin
        | 'Impulse' >> Impulse()
        | 'EmitSource' >> core.Map(lambda _: source)
        | 'ReadUnbounded' >> core.ParDo(_ReadFromUnboundedSourceDoFn()))
