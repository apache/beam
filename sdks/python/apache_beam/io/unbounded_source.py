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
through a Splittable ``DoFn`` -- inspired by (not a literal port of) Java's
``Read.UnboundedSourceAsSDFWrapperFn``. The streaming-SDF template followed for
the process-loop / watermark / defer plumbing is
``apache_beam.transforms.periodicsequence``.

Public API::

    from apache_beam.io.unbounded_source import (
        UnboundedSource, UnboundedReader, CheckpointMark, ReadFromUnboundedSource)

    class MySource(UnboundedSource):
      ...

    with beam.Pipeline() as p:
      p | ReadFromUnboundedSource(MySource()) | beam.Map(print)
      # Equivalent (since iobase.Read.expand dispatches on source type):
      # p | beam.io.Read(MySource()) | beam.Map(print)

Scope (deliberately minimal): read loop, event-time timestamps, monotonic
watermark reporting (including the EOF advance to ``MAX_TIMESTAMP`` so downstream
windows can close), checkpoint-based pause/resume (``defer_remainder``),
deterministic reader close on EOF / split / exception, and bundle finalization.

Out of scope for this PoC (tracked under #19137):
  * Record-id-based deduplication (Java's ``ValueWithRecordId``).
  * Backlog-byte reporting (``restriction_size`` is a constant 1; per-restriction
    progress is binary 0.0 / 1.0).
  * Dynamic split fractions / runner-initiated work stealing.
  * Initial fan-out: ``RestrictionProvider.split`` ignores ``desired_num_splits``
    and yields a single restriction.
  * Source-specific checkpoint coders threaded through the SDF restriction coder
    (the restriction coder always pickles checkpoint marks via the source's
    ``get_checkpoint_mark_coder`` captured once at ``expand()`` time, but no
    per-tracker coder dispatch).
  * Reader caching across bundles (Java caches readers across split boundaries
    via a Guava cache; this PoC always rebuilds the reader from the checkpoint).
  * ``EmptyUnboundedSource`` terminal-state marker (this PoC uses an ``is_done``
    flag on the restriction instead).
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

    Implementations MUST be idempotent in two senses:
      * Repeated calls on the same :class:`CheckpointMark` instance must be
        safe (runner may retry callbacks).
      * Successive calls on *different* CheckpointMark instances must be
        consistent with monotonically progressing committed state (each
        bundle's defer/split produces a fresh CheckpointMark covering the
        records read so far; the typical Kafka-style implementation is
        ``ack(self.last_offset)`` which is naturally monotonic and idempotent
        on the broker side).

    The SDK's bundle finalizer (``_BundleFinalizerParam.finalize_bundle`` at
    ``transforms/core.py``) catches and logs any exception raised from this
    method but does NOT retry, so a transient failure is silently dropped.
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
    """Splits into at most ``desired_num_splits`` independent sub-sources.

    Each returned sub-source MUST be independent and MUST NOT share mutable
    state with siblings (the runner may execute them concurrently across
    workers). Mirrors Java's ``UnboundedSource.split``. Note that the current
    ``ReadFromUnboundedSource`` PoC ignores ``desired_num_splits`` -- this
    method is the public API but is dead code from the SDF wrapper's
    perspective until W2.
    """
    raise NotImplementedError

  def create_reader(
      self, options: Optional[Any],
      checkpoint_mark: Optional[CheckpointMark]) -> UnboundedReader:
    """Creates a reader, optionally resuming from ``checkpoint_mark``.

    Contract:
      * When ``checkpoint_mark`` is ``None``, the returned reader's ``start()``
        produces the very first record of the source (or returns ``False`` if
        none yet).
      * When ``checkpoint_mark`` is not ``None``, the returned reader's
        ``start()`` produces the FIRST record strictly AFTER the position
        encoded by ``checkpoint_mark``. The reader must NOT re-deliver records
        already covered by the prior bundle. Mirrors Java's
        ``UnboundedSource.createReader(options, checkpointMark)``.
    """
    raise NotImplementedError

  def get_checkpoint_mark_coder(self) -> Coder:
    """Returns the coder for this source's :class:`CheckpointMark` instances.

    Called once at pipeline construction (graph build), NOT per-bundle. Do not
    perform I/O here. Subclasses MUST override; the default raises with a
    helpful message naming the subclass.
    """
    raise NotImplementedError(
        '%s must override get_checkpoint_mark_coder() to return a Coder for '
        'its CheckpointMark subclass.' % type(self).__name__)

  def is_bounded(self) -> bool:
    # SourceBase.is_bounded raises; an unbounded source is, by definition, not.
    return False

  def default_output_coder(self) -> Coder:
    # Permissive default, matching BoundedSource (iobase.py). Override for a
    # tighter coder. Not wired into ReadFromUnboundedSource in this PoC --
    # this method is kept as a forward-compat hook so subclasses written
    # against the API today will Just Work when wiring lands in W2.
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
  ``is_done`` flag for the terminal (MAX-watermark) transition and a separate
  ``finalization_checkpoint_mark`` so a done primary can carry a commit-hook
  without polluting the RESUME-state ``checkpoint_mark`` (matches W1 design
  doc v5 finding: combining the two channels causes resume/commit semantic
  confusion).

  Field roles:
    * ``checkpoint_mark`` -- RESUME state. A reader rebuilt from this mark
      MUST produce the FIRST record strictly AFTER it (i.e. no re-delivery).
    * ``finalization_checkpoint_mark`` -- COMMIT hook. Only set on a done
      primary that was just cut this bundle. Registered with the runner's
      bundle finalizer to acknowledge upstream (e.g. ack Pub/Sub messages).
      Independent of ``checkpoint_mark`` so a residual's resume state can be
      ``None`` while still recording what should be acked.
  """
  source: UnboundedSource
  checkpoint_mark: Optional[CheckpointMark] = None
  watermark: Timestamp = MIN_TIMESTAMP
  is_done: bool = False
  finalization_checkpoint_mark: Optional[CheckpointMark] = None


class _UnboundedSourceRestrictionCoder(Coder):
  """Encodes :class:`_UnboundedSourceRestriction` as a fixed 5-tuple.

  Shape: pickled source + nullable resume checkpoint (encoded with the
  source's own checkpoint coder if provided, else pickle) + watermark +
  done flag + nullable finalization checkpoint (same coder as resume).
  """
  def __init__(self, checkpoint_mark_coder: Optional[Coder] = None):
    nullable_checkpoint = NullableCoder(
        checkpoint_mark_coder or _MemoizingPickleCoder())
    self._tuple_coder = TupleCoder((
        _MemoizingPickleCoder(),  # source
        nullable_checkpoint,  # checkpoint_mark (RESUME state, may be None)
        TimestampCoder(),  # watermark
        BooleanCoder(),  # is_done
        nullable_checkpoint))  # finalization_checkpoint_mark (commit hook)

  def encode(self, restriction: '_UnboundedSourceRestriction') -> bytes:
    return self._tuple_coder.encode((
        restriction.source,
        restriction.checkpoint_mark,
        restriction.watermark,
        restriction.is_done,
        restriction.finalization_checkpoint_mark))

  def decode(self, encoded: bytes) -> '_UnboundedSourceRestriction':
    (source, checkpoint_mark, watermark, is_done,
     finalization_checkpoint_mark) = self._tuple_coder.decode(encoded)
    return _UnboundedSourceRestriction(
        source=source,
        checkpoint_mark=checkpoint_mark,
        watermark=watermark,
        is_done=is_done,
        finalization_checkpoint_mark=finalization_checkpoint_mark)

  def is_deterministic(self) -> bool:
    # The source and checkpoint are pickled, which is not guaranteed
    # deterministic; matches the bounded SDF restriction coder in iobase.py.
    # NOTE on forward-compat: the wire format is a fixed 5-tuple. Adding a
    # 6th field in a future version would break decoding of in-flight blobs
    # from older workers. If/when another field is needed, switch this to a
    # length-prefixed or version-tagged encoding -- out of scope for W1.
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
    # Today this co-varies with `_restriction.is_done` (both set together at
    # EOF and at try_split); kept separate as a forward-compat hook so a
    # future refactor can checkpoint without finishing the restriction.
    self._checkpoint_taken = False

  def _ensure_reader(self) -> None:
    if self._reader is None:
      self._reader = self._restriction.source.create_reader(
          self._options, self._restriction.checkpoint_mark)

  def _close_reader_if_open(self) -> None:
    """Idempotent reader release. Called by the EOF and split paths, and by
    the DoFn's ``finally`` so an exception inside ``process()`` does not leak
    sockets / file descriptors held by the live :class:`UnboundedReader`.
    """
    if self._reader is None:
      return
    try:
      self._reader.close()
    except Exception:  # pylint: disable=broad-except
      _LOGGER.warning('Error closing UnboundedReader', exc_info=True)
    finally:
      self._reader = None

  def current_restriction(self) -> _UnboundedSourceRestriction:
    return self._restriction

  def try_claim(self, out: List[Any]) -> bool:
    """Advances the underlying reader by one record.

    Holder protocol: ``out[0]`` receives either
    ``(value, record_timestamp, source_watermark)`` on the has-data path, or
    the :data:`_NO_DATA` sentinel on no-data-now / EOF / already-done paths.

    The watermark in the has-data tuple is the SOURCE'S reported watermark
    (``reader.get_watermark()``), not the record's event time -- matching
    Java's ``UnboundedSourceValue.getWatermark()`` (Read.java:594). The
    DoFn uses ``source_watermark`` to advance the output PCollection's
    watermark and uses ``record_timestamp`` only to label the emitted
    ``TimestampedValue``. Conflating the two would freeze the PCollection
    watermark at the last record's event time, breaking out-of-order
    sources and starving downstream event-time windows.

    Contract drift note: the ``RestrictionTracker`` ABC defines
    ``try_claim(position)`` where ``position`` identifies a split point. We
    instead use the argument as a one-element output holder, like Java's
    ``tryClaim(UnboundedSourceValue[] out)``. The
    ``ThreadsafeRestrictionTracker`` / ``RestrictionTrackerView`` chain
    forwards the value opaquely (sdf_utils.py:75, sdf_utils.py:171), so the
    mutation is visible across the lock.

    Exception safety: any exception from ``reader.start()`` / ``advance()`` /
    ``get_watermark()`` etc. closes the reader before re-raising, so the
    DoFn's ``finally`` does not need to traverse the SDF wrapper chain on
    reader-method failures.
    """
    try:
      return self._try_claim_inner(out)
    except Exception:
      # Reader is in an indeterminate state; release its resources before
      # the exception bubbles to the DoFn (which can't trust ``self._reader``
      # anymore).
      self._close_reader_if_open()
      raise

  def _try_claim_inner(self, out: List[Any]) -> bool:
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
      # this"). That EOF is realized on the next, data-less claim, so the
      # record we just read is never dropped. Capture the source watermark
      # alongside the record (see method docstring for why).
      out[0] = (
          self._reader.get_current(),
          self._reader.get_current_timestamp(),
          self._reader.get_watermark())
      return True
    watermark = self._reader.get_watermark()
    if watermark >= MAX_TIMESTAMP:
      # No data and watermark at MAX: the reader permanently finished. Cut a
      # final checkpoint, close, and mark the restriction done.
      checkpoint = self._reader.get_checkpoint_mark()
      self._close_reader_if_open()
      self._restriction = dataclasses.replace(
          self._restriction,
          checkpoint_mark=None,  # nothing left to resume from
          watermark=MAX_TIMESTAMP,
          is_done=True,
          finalization_checkpoint_mark=checkpoint)
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
    """Cuts a checkpoint, returning (primary, residual) or None.

    The cut checkpoint goes into ``primary.finalization_checkpoint_mark`` so
    the DoFn can register a bundle-finalize callback for it. The same
    checkpoint object also goes into ``residual.checkpoint_mark`` so the
    resumed reader rebuilds at the correct position. The two fields are
    independent on purpose (see :class:`_UnboundedSourceRestriction`
    docstring): a runner that re-processes the primary alone must not see
    a stale resume state, and a residual must not register finalize again
    until ITS checkpoint is cut in a future bundle.
    """
    try:
      return self._try_split_inner(fraction_of_remainder)
    except Exception:
      self._close_reader_if_open()
      raise

  def _try_split_inner(self, fraction_of_remainder):
    # fraction 0 is the self-checkpoint raised by defer_remainder(); any other
    # fraction cuts the same checkpoint. Returns (primary, residual) or None.
    if self._reader is None or not self._started or self._restriction.is_done:
      return None
    checkpoint = self._reader.get_checkpoint_mark()
    watermark = self._reader.get_watermark()
    # Primary represents work just finished THIS bundle; it carries ONLY the
    # finalize hook. checkpoint_mark on primary is None to make it obvious
    # that the primary has no resume state of its own.
    primary = dataclasses.replace(
        self._restriction,
        checkpoint_mark=None,
        is_done=True,
        finalization_checkpoint_mark=checkpoint)
    # Residual represents future work; it carries ONLY the resume state.
    # finalization_checkpoint_mark is None so a future bundle does not
    # double-register finalize for the same checkpoint.
    residual = _UnboundedSourceRestriction(
        source=self._restriction.source,
        checkpoint_mark=checkpoint,
        watermark=watermark,
        is_done=False,
        finalization_checkpoint_mark=None)
    self._restriction = primary
    self._checkpoint_taken = True
    # The residual's reader is rebuilt from its checkpoint on resume; close
    # ours rather than dropping a live handle on the floor.
    self._close_reader_if_open()
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
    # Minimal PoC: no initial fan-out. ``desired_num_splits`` is *not* honored
    # and ``UnboundedSource.split(desired_num_splits, options)`` is currently
    # dead code from this provider's perspective. Real splitting (one
    # restriction per sub-source, e.g. one per Kafka partition) is W2 work.
    yield restriction

  def restriction_size(self, element, restriction) -> int:
    # Backlog estimation is out of scope; report a constant non-negative size.
    # This blinds Dataflow's auto-scaler and Flink's work-stealing to per-
    # restriction load -- documented gap for #19137.
    return 1

  def restriction_coder(self) -> Coder:
    return self._restriction_coder

  def truncate(self, element, restriction):
    # On drain, stop emitting new records (mirrors PeriodicSequence.truncate).
    return None


def _set_watermark_if_greater(
    watermark_estimator, new_watermark: Timestamp) -> None:
  # ManualWatermarkEstimator.set_watermark raises if the watermark regresses, so
  # only ever advance it (mirrors PeriodicSequence's monotonic guard). A
  # misbehaving reader that reports a regressing watermark is silently absorbed
  # here -- intentional, to keep the pipeline running through reader bugs.
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
    if poll_interval_seconds <= 0:
      # A zero / negative poll interval would either busy-spin on no-data
      # polls (poll_interval=0 -> defer_remainder(Duration(0)) -> immediate
      # re-schedule) or pass a negative ``Duration`` to the runner which is
      # not well-defined. Mirror Java's IllegalArgumentException posture.
      raise ValueError(
          'poll_interval_seconds must be > 0, got %r' %
          (poll_interval_seconds, ))
    super().__init__()
    self._source = source
    self._poll_interval_seconds = poll_interval_seconds

  def expand(self, pbegin):
    source = self._source
    poll_interval_seconds = self._poll_interval_seconds
    provider = _UnboundedSourceRestrictionProvider(
        checkpoint_mark_coder=source.get_checkpoint_mark_coder())

    # The DoFn is defined inside ``expand`` so it can close over the
    # source-specific ``provider`` (which holds the source's checkpoint coder)
    # and the user-tuned ``poll_interval_seconds``. Lifting it to module level
    # would require a stateless provider (losing per-source checkpoint coder
    # selection), so this is a deliberate trade-off. Cloudpickle, Beam's
    # default, handles closure-defined classes; stdlib ``pickle`` does not.
    class _ReadFromUnboundedSourceDoFn(core.DoFn):
      """SDF wrapper driving an :class:`UnboundedReader` for one restriction."""

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
              # EOF (restriction is_done==True, watermark already set to MAX in
              # the tracker). Mirrors Java Read.java:625 -- advance the
              # watermark estimator unconditionally on the terminal path so
              # downstream event-time windows can close, otherwise the
              # estimator would stay at the last reported watermark.
              _set_watermark_if_greater(
                  watermark_estimator, tracker.current_restriction().watermark)
              break
            record = holder[0]
            if record is _NO_DATA:
              # No data right now: advance the watermark and self-checkpoint so
              # the runner reschedules us later. Resume via defer_remainder() +
              # break -- NOT yield ProcessContinuation (the portable SDF path).
              _set_watermark_if_greater(
                  watermark_estimator, tracker.current_restriction().watermark)
              tracker.defer_remainder(Duration(seconds=poll_interval_seconds))
              break
            # Data path: advance the estimator with the SOURCE's reported
            # watermark (third tuple slot), NOT the record's event time.
            # Mirrors Java Read.java:594. The record's event time is used
            # only as the TimestampedValue label so the downstream sees the
            # real per-record timestamp.
            value, record_timestamp, source_watermark = record
            _set_watermark_if_greater(watermark_estimator, source_watermark)
            yield TimestampedValue(value, record_timestamp)
        finally:
          current = tracker.current_restriction()
          # Register finalization only when a real checkpoint was cut this
          # bundle. Restriction identity (`current is not initial`) mirrors
          # Java's reference-equality gate in Read.java. We read the explicit
          # finalization channel, NOT ``checkpoint_mark`` (which is the
          # RESUME state and may belong to the residual after a split).
          finalize_mark = current.finalization_checkpoint_mark
          if current is not initial and finalize_mark is not None:
            bundle_finalizer.register(finalize_mark.finalize_checkpoint)
          # Release the underlying reader on every exit path, including the
          # exception path where a downstream yield raised between two
          # try_claim calls (reader-method failures are already closed inside
          # the tracker). ``RestrictionTrackerView`` does not expose the inner
          # tracker, so traverse the (stable-but-private) wrapper chain. If
          # the chain changes in a future Beam version we log a warning and
          # let GC eventually close; never call ``close`` on an unrelated
          # tracker subclass.
          threadsafe = getattr(
              tracker, '_threadsafe_restriction_tracker', None)
          inner_tracker = getattr(
              threadsafe, '_restriction_tracker', None)
          if isinstance(inner_tracker, _UnboundedSourceRestrictionTracker):
            inner_tracker._close_reader_if_open()
          elif inner_tracker is not None or threadsafe is not None:
            _LOGGER.warning(
                'UnboundedSource DoFn could not reach the inner tracker via '
                '_threadsafe_restriction_tracker._restriction_tracker; reader '
                'close on exception path skipped, relying on GC. Beam SDF '
                'wrapper internals may have changed -- file an issue.')

    return (
        pbegin
        | 'Impulse' >> Impulse()
        | 'EmitSource' >> core.Map(lambda _: source)
        | 'ReadUnbounded' >> core.ParDo(_ReadFromUnboundedSourceDoFn()))
