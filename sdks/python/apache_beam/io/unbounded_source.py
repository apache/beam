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

"""Experimental ``UnboundedSource`` support for the Python SDK.

``UnboundedSource`` support is currently experimental in the Python SDK; the
API may change in backwards-incompatible ways.

An unbounded source reads an effectively infinite stream of records (message
queues, change-data-capture feeds, and similar) with checkpoint-based
pause/resume, watermark reporting, and bundle finalization.

To define a source, implement :class:`UnboundedSource`, an
:class:`UnboundedReader`, and (when the reader has a resumable position) a
:class:`CheckpointMark`::

    import apache_beam as beam
    from apache_beam.io.unbounded_source import (
        CheckpointMark, UnboundedReader, UnboundedSource)
    from apache_beam.utils.timestamp import MAX_TIMESTAMP

    class MyCheckpointMark(CheckpointMark):
      def __init__(self, position):
        self.position = position

      def finalize_checkpoint(self):
        # Commit/acknowledge records up to ``position`` upstream, e.g. ack the
        # consumed messages on a queue.
        ...

    class MyReader(UnboundedReader):
      def start(self):
        # Position at the first record; return whether one is available.
        ...

      def advance(self):
        # Move to the next record; ``False`` means no data is available now.
        ...

      def get_current(self):
        ...

      def get_current_timestamp(self):
        ...  # event time of the current record

      def get_watermark(self):
        # Lower bound on the timestamps of future records. Return
        # ``MAX_TIMESTAMP`` to signal the reader has permanently finished.
        ...

      def get_checkpoint_mark(self):
        return MyCheckpointMark(...)

    class MySource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        # Return independent sub-sources, or ``[self]`` when not splittable.
        return [self]

      def create_reader(self, options, checkpoint_mark):
        # Build a reader; resume after ``checkpoint_mark`` when it is not None.
        return MyReader(...)

      def get_checkpoint_mark_coder(self):
        return ...  # a Coder for MyCheckpointMark

Read the source in a pipeline with :class:`apache_beam.io.Read`::

    with beam.Pipeline() as p:
      p | beam.io.Read(MySource()) | beam.Map(print)
"""

import collections
import dataclasses
import logging
import threading
import time
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Optional

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
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'CheckpointMark',
    'UnboundedReader',
    'UnboundedSource',
    'ReadFromUnboundedSource',
]

_LOGGER = logging.getLogger(__name__)

# Sentinel used when a reader has no data available right now.
# This is distinct from end-of-stream.
_NO_DATA = object()

_DEFAULT_POLL_INTERVAL_SECONDS = 1.0
_DEFAULT_DESIRED_NUM_SPLITS = 20
_DEFAULT_MAX_RECORDS_PER_BUNDLE = 10000
_DEFAULT_MAX_READ_TIME_SECONDS = 10.0
# A reader parked by a residual that never resumes is closed once idle this
# long; the cache also caps its entry count as a memory backstop.
_DEFAULT_READER_CACHE_IDLE_SECONDS = 60.0
_DEFAULT_READER_CACHE_MAX_SIZE = 100

# Encodes a source to a structural cache key. Internally consistent across park
# and acquire; need not match the restriction wire coder.
_SOURCE_KEY_CODER = _MemoizingPickleCoder()

# ------------------------------------------------------------------------------
# Public abstract base classes.
# ------------------------------------------------------------------------------


class CheckpointMark(object):
  """A durable, serializable position in an :class:`UnboundedSource`.

  Produced by :meth:`UnboundedReader.get_checkpoint_mark`, encoded with
  :meth:`UnboundedSource.get_checkpoint_mark_coder`, and used to resume a reader
  (see :meth:`UnboundedSource.create_reader`).
  """
  def finalize_checkpoint(self) -> None:
    """Called once the runner has durably committed work up to this mark.

    Override to acknowledge/commit upstream (for example, ack the consumed
    messages on a queue). The default is a no-op.

    The runner calls this at most once for a committed checkpoint mark.
    Finalization is best effort; a mark may never be finalized. An exception
    raised here is logged. On bundle retry an uncommitted mark may be re-cut
    over an overlapping span, so this method must be idempotent (acknowledge by
    absolute position).
    """
    pass


class UnboundedReader(object):
  """Reads records from an :class:`UnboundedSource`.

  Lifecycle: exactly one :meth:`start`, then any number of :meth:`advance`
  calls; whenever one returns ``True`` the current record is available via
  :meth:`get_current` / :meth:`get_current_timestamp`. A ``False`` return means
  "no data available right now", which is distinct from end-of-stream: a reader
  signals a permanent end by reporting a watermark of ``MAX_TIMESTAMP``.
  """
  def start(self) -> bool:
    """Positions at the first record; returns whether one is available."""
    raise NotImplementedError

  def advance(self) -> bool:
    """Advances to the next record. ``False`` means no data is available now.

    Should not block. The wrapper enforces the per-bundle record and time caps
    only between records, so a blocking ``start``/``advance`` can overrun the
    time cap and stall the bundle. Return ``False`` when no data is currently
    available instead of waiting.
    """
    raise NotImplementedError

  def get_current(self) -> Any:
    """Returns the record claimed by the last successful start/advance."""
    raise NotImplementedError

  def get_current_timestamp(self) -> Timestamp:
    """Returns the event-time timestamp of the current record."""
    raise NotImplementedError

  def get_watermark(self) -> Timestamp:
    """An approximate lower bound on timestamps of future records.

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

  Read it in a pipeline with :class:`apache_beam.io.Read`::

      p | beam.io.Read(MyUnboundedSource())
  """
  def split(self,
            desired_num_splits: int,
            options: Optional[Any] = None) -> Iterable['UnboundedSource']:
    """Splits into at most ``desired_num_splits`` independent sub-sources.

    Each returned sub-source must be independent and must not share mutable
    state with siblings (the runner may execute them concurrently across
    workers). Return ``[self]`` if the source cannot be split. Splitting is
    performed once, before any checkpoint exists; once a reader has
    checkpointed, the restriction is kept intact.
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
        ``start()`` produces the first record strictly after the position
        encoded by ``checkpoint_mark``. The reader must not re-deliver records
        already covered by the prior bundle.
    """
    raise NotImplementedError

  def get_checkpoint_mark_coder(self) -> Coder:
    """Returns the coder for this source's :class:`CheckpointMark` instances.

    The SDK may call this while encoding or decoding source restrictions.
    Implementations should be deterministic, side-effect free, and should not
    perform I/O.
    """
    raise NotImplementedError(
        '%s must override get_checkpoint_mark_coder() to return a Coder for '
        'its CheckpointMark subclass.' % type(self).__name__)

  def is_bounded(self) -> bool:
    # SourceBase.is_bounded raises; an unbounded source is, by definition, not.
    return False

  def default_output_coder(self) -> Coder:
    # Permissive default; override for a tighter coder.
    return coders.registry.get_coder(object)


# ------------------------------------------------------------------------------
# SDF wrapper internals: a private implementation detail of
# ReadFromUnboundedSource.
# ------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class _UnboundedSourceRestriction(object):
  """Durable SDF restriction describing where a reader should (re)start.

  Holds only serializable state -- never a live reader. ``is_done`` marks the
  terminal (MAX-watermark) transition. ``finalization_checkpoint_mark`` is kept
  separate from ``checkpoint_mark`` so a done primary can carry a commit hook
  without polluting the resume state.

  Field roles:
    * ``checkpoint_mark`` -- RESUME state. A reader rebuilt from this mark
      must produce the first record strictly after it.
    * ``finalization_checkpoint_mark`` -- COMMIT hook. Only set on a done
      primary that was just cut this bundle. Registered with the runner's
      bundle finalizer to acknowledge upstream. Independent of
      ``checkpoint_mark`` so a residual's resume state can be ``None`` while
      still recording what should be committed.
  """
  source: UnboundedSource
  checkpoint_mark: Optional[CheckpointMark] = None
  watermark: Timestamp = MIN_TIMESTAMP
  is_done: bool = False
  finalization_checkpoint_mark: Optional[CheckpointMark] = None


class _UnboundedSourceRestrictionCoder(Coder):
  """Encodes :class:`_UnboundedSourceRestriction` as a fixed 5-tuple.

  Stateless: at encode time the source's own
  :meth:`UnboundedSource.get_checkpoint_mark_coder` is looked up from the
  restriction; at decode time the source is decoded first and its coder
  drives the checkpoint-mark decoding. This avoids passing source-specific
  coder state into the coder's constructor, which in turn lets
  :class:`_UnboundedSourceRestrictionProvider` and
  :class:`_ReadFromUnboundedSourceDoFn` be module-level classes.

  Wire shape: source_bytes / checkpoint_bytes / watermark / done /
  finalization_checkpoint_bytes -- the checkpoint and finalization bytes
  are independently encoded with the (source-declared) checkpoint coder
  wrapped in :class:`NullableCoder`.
  """
  def __init__(self):
    self._source_coder = _MemoizingPickleCoder()
    self._bytes_coder = coders.BytesCoder()
    self._tuple_coder = TupleCoder((
        self._bytes_coder,  # source (pickled bytes)
        self._bytes_coder,  # checkpoint_mark (nullable-encoded bytes)
        TimestampCoder(),  # watermark
        BooleanCoder(),  # is_done
        self._bytes_coder))  # finalization_checkpoint_mark (nullable-encoded)

  def _checkpoint_coder(self, source: UnboundedSource) -> Coder:
    return NullableCoder(source.get_checkpoint_mark_coder())

  def encode(self, restriction: '_UnboundedSourceRestriction') -> bytes:
    source_bytes = self._source_coder.encode(restriction.source)
    cp_coder = self._checkpoint_coder(restriction.source)
    return self._tuple_coder.encode((
        source_bytes,
        cp_coder.encode(restriction.checkpoint_mark),
        restriction.watermark,
        restriction.is_done,
        cp_coder.encode(restriction.finalization_checkpoint_mark)))

  def decode(self, encoded: bytes) -> '_UnboundedSourceRestriction':
    (source_bytes, checkpoint_bytes, watermark, is_done,
     finalization_bytes) = self._tuple_coder.decode(encoded)
    source = self._source_coder.decode(source_bytes)
    cp_coder = self._checkpoint_coder(source)
    return _UnboundedSourceRestriction(
        source=source,
        checkpoint_mark=cp_coder.decode(checkpoint_bytes),
        watermark=watermark,
        is_done=is_done,
        finalization_checkpoint_mark=cp_coder.decode(finalization_bytes))

  def is_deterministic(self) -> bool:
    # Pickled source and checkpoint are not guaranteed deterministic.
    return False


class _ReaderCache(object):
  """Holds live readers between an SDF self-checkpoint and its resume.

  A fresh tracker is built for every bundle, so a reader parked at a
  self-checkpoint would otherwise be closed and rebuilt from its checkpoint
  mark on the next bundle. Parking it here lets the resuming bundle reclaim the
  same started reader, keyed by the residual's structural ``(source, checkpoint
  mark)``. ``acquire`` removes the entry, so two trackers never drive one
  reader.

  A residual may be reassigned to another worker and never resume here; such a
  reader is released once it falls idle past ``idle_seconds`` or when the entry
  count exceeds ``max_size``, and the owning DoFn's teardown releases the rest.
  One DoFn instance drives several trackers across threads, so access is locked.
  """
  def __init__(
      self,
      idle_seconds: float = _DEFAULT_READER_CACHE_IDLE_SECONDS,
      max_size: int = _DEFAULT_READER_CACHE_MAX_SIZE,
      now: Optional[Callable[[], float]] = None):
    self._idle_seconds = idle_seconds
    self._max_size = max_size
    self._now = now or time.monotonic
    self._lock = threading.Lock()
    # key -> (reader, started, parked_at); ordered oldest-first for eviction.
    self._entries = collections.OrderedDict(
    )  # type: collections.OrderedDict[Any, tuple[UnboundedReader, bool, float]]

  def acquire(self, key: Any) -> Optional[tuple['UnboundedReader', bool]]:
    """Removes and returns ``(reader, started)`` for ``key``, or None."""
    with self._lock:
      entry = self._entries.pop(key, None)
      stale = self._evict_idle()
    self._close_readers(stale)
    if entry is None:
      return None
    return entry[0], entry[1]

  def park(self, key: Any, reader: 'UnboundedReader', started: bool) -> None:
    """Stores ``reader`` under ``key`` for a later bundle to reclaim. A reader
    already parked under ``key`` is closed."""
    with self._lock:
      replaced = self._entries.pop(key, None)
      self._entries[key] = (reader, started, self._now())
      stale = self._evict_idle()
      while len(self._entries) > self._max_size:
        _, oldest = self._entries.popitem(last=False)
        stale.append(oldest[0])
    if replaced is not None and replaced[0] is not reader:
      stale.append(replaced[0])
    self._close_readers(stale)

  def close_all(self) -> None:
    """Closes every parked reader. Called from the owning DoFn's teardown."""
    with self._lock:
      entries = list(self._entries.values())
      self._entries.clear()
    self._close_readers(entry[0] for entry in entries)

  def _evict_idle(self) -> list:
    # Caller holds the lock. Pops entries idle past the window (oldest first)
    # and returns their readers for the caller to close after unlocking.
    deadline = self._now() - self._idle_seconds
    stale = []
    while self._entries:
      key, entry = next(iter(self._entries.items()))
      if entry[2] > deadline:
        break
      del self._entries[key]
      stale.append(entry[0])
    return stale

  def _close_readers(self, readers) -> None:
    for reader in readers:
      try:
        reader.close()
      except Exception:  # pylint: disable=broad-except
        _LOGGER.warning('Error closing UnboundedReader', exc_info=True)


class _UnboundedSourceRestrictionTracker(iobase.RestrictionTracker):
  """Drives an :class:`UnboundedReader` for one SDF restriction.

  Owns the live reader (lazily created, never serialized): both runner-initiated
  ``defer_remainder`` self-checkpoints with ``try_split(0)``, which must
  checkpoint the live reader.

  A self-checkpoint parks the reader in the DoFn's :class:`_ReaderCache` for the
  next bundle to reclaim, keeping one started reader alive across bundles. The
  DoFn injects ``_reader_cache`` at the start of ``process()``; with no cache
  the tracker builds a fresh reader each bundle.

  ``process()`` only sees a ``RestrictionTrackerView``, which hides custom
  methods and whose ``try_claim`` returns just a bool, so the freshly-read
  record is handed back through a one-element holder list passed as the
  ``try_claim`` *position* argument.
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
    # Cross-bundle reader cache, injected by the DoFn; None disables caching.
    self._reader_cache = None  # type: Optional[_ReaderCache]

  def _ensure_reader(self) -> None:
    if self._reader is not None:
      return
    cached = self._acquire_cached_reader()
    if cached is not None:
      # A parked reader is already started and positioned past its checkpoint.
      self._reader, self._started = cached
      return
    self._reader = self._restriction.source.create_reader(
        self._options, self._restriction.checkpoint_mark)

  def _cache_key(self,
                 restriction: _UnboundedSourceRestriction) -> Optional[Any]:
    """Structural ``(source, checkpoint)`` key, or None when uncacheable.

    Built from the source pickle and the source's own checkpoint coder so a
    parked reader and its resuming restriction map to the same entry. A None
    key disables caching for that restriction; the resume then rebuilds from
    the checkpoint mark under the source's ``create_reader`` contract.
    """
    try:
      source_bytes = _SOURCE_KEY_CODER.encode(restriction.source)
      cp_coder = NullableCoder(restriction.source.get_checkpoint_mark_coder())
      return source_bytes, cp_coder.encode(restriction.checkpoint_mark)
    except Exception:  # pylint: disable=broad-except
      return None

  def _acquire_cached_reader(self) -> Optional[tuple['UnboundedReader', bool]]:
    if self._reader_cache is None:
      return None
    key = self._cache_key(self._restriction)
    if key is None:
      return None
    return self._reader_cache.acquire(key)

  def _park_or_close_reader(
      self, residual: _UnboundedSourceRestriction) -> None:
    """Hands the live reader to the cache for ``residual`` to reclaim, or
    closes it when no cache is available or the restriction is uncacheable."""
    if self._reader is None:
      return
    key = (
        self._cache_key(residual) if self._reader_cache is not None else None)
    if key is None:
      self._close_reader_if_open()
      return
    reader, self._reader = self._reader, None
    self._reader_cache.park(key, reader, self._started)

  def _clone_checkpoint(
      self, checkpoint: Optional[CheckpointMark]) -> Optional[CheckpointMark]:
    """Returns an independent copy of a mark via the source's checkpoint coder.

    Used to keep the primary's finalize hook and the residual's resume state
    from sharing one object, since a user ``finalize_checkpoint()`` may mutate
    the mark.
    """
    if checkpoint is None:
      return None
    coder = self._restriction.source.get_checkpoint_mark_coder()
    return coder.decode(coder.encode(checkpoint))

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

  def try_claim(self, out: list[Any]) -> bool:
    """Advances the reader by one record.

    ``out[0]`` receives ``(value, record_timestamp, source_watermark)`` on the
    has-data path, or the :data:`_NO_DATA` sentinel otherwise. The watermark is
    the source's reported watermark, not the record's event time: the DoFn
    advances the output watermark with the former and timestamps the record
    with the latter. The argument doubles as the output holder (the
    ``RestrictionTracker`` ABC treats it as a claim position), which the
    threadsafe-tracker chain forwards opaquely.
    """
    try:
      return self._try_claim_inner(out)
    except Exception:
      # Reader state is now indeterminate; release it before re-raising.
      self._close_reader_if_open()
      raise

  def _try_claim_inner(self, out: list[Any]) -> bool:
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
      # Emit an available record before checking the watermark: a reader may
      # report its last record and a MAX_TIMESTAMP watermark on the same call,
      # and EOF is realized on the next data-less claim.
      out[0] = (
          self._reader.get_current(),
          self._reader.get_current_timestamp(),
          self._reader.get_watermark())
      return True
    watermark = self._reader.get_watermark()
    if watermark >= MAX_TIMESTAMP:
      # No data and watermark at MAX: cut a final checkpoint, close, mark done.
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
    # No data is available now. Refresh the watermark before deferring.
    self._restriction = dataclasses.replace(
        self._restriction, watermark=watermark)
    out[0] = _NO_DATA
    return True

  def try_split(
      self, fraction_of_remainder
  ) -> Optional[tuple[_UnboundedSourceRestriction,
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
    # Only self-checkpoint (fraction 0) is supported; decline runner-initiated
    # dynamic splits.
    if fraction_of_remainder != 0:
      return None
    if self._reader is None or not self._started or self._restriction.is_done:
      return None
    checkpoint = self._reader.get_checkpoint_mark()
    # The residual watermark is advisory; the SDF watermark estimator state is
    # the authoritative cross-bundle watermark.
    watermark = self._reader.get_watermark()
    # Keep the two channels independent: the primary carries only the finalize
    # hook, the residual only the resume state. The residual gets its own clone
    # so a finalize_checkpoint() that mutates the primary's mark cannot corrupt
    # the residual's resume position before the runner encodes it.
    primary = dataclasses.replace(
        self._restriction,
        checkpoint_mark=None,
        is_done=True,
        finalization_checkpoint_mark=checkpoint)
    residual = _UnboundedSourceRestriction(
        source=self._restriction.source,
        checkpoint_mark=self._clone_checkpoint(checkpoint),
        watermark=watermark,
        is_done=False,
        finalization_checkpoint_mark=None)
    self._restriction = primary
    self._checkpoint_taken = True
    # Park the reader so the resuming bundle reclaims it; on a cache miss the
    # residual rebuilds one from its checkpoint mark.
    self._park_or_close_reader(residual)
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
    # Backlog-based progress is not implemented; report a coarse done/not-done
    # signal via ``completed`` / ``remaining``.
    if self._restriction.is_done:
      return iobase.RestrictionProgress(completed=1.0, remaining=0.0)
    return iobase.RestrictionProgress(completed=0.0, remaining=1.0)

  def is_bounded(self) -> bool:
    return False


class _UnboundedSourceRestrictionProvider(core.RestrictionProvider):
  """Wraps an :class:`UnboundedSource` element as an SDF restriction.

  Stateless module-level singleton (see :data:`_PROVIDER`): all
  source-specific state (e.g. the source's checkpoint coder) is derived
  per-call from the restriction's ``source`` field, which lets
  :class:`_ReadFromUnboundedSourceDoFn` live at module level too. The provider
  currently passes ``None`` for the ``options`` forwarded to
  :meth:`UnboundedSource.split`.
  """
  def __init__(self):
    self._restriction_coder = _UnboundedSourceRestrictionCoder()

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
    return _UnboundedSourceRestrictionTracker(restriction)

  def split(self, element,
            restriction) -> Iterable[_UnboundedSourceRestriction]:
    if restriction.is_done or restriction.checkpoint_mark is not None:
      yield restriction
      return

    # ``source.split`` is user code and may refuse to split; fall back to a
    # single restriction on error.
    try:
      split_sources = list(
          restriction.source.split(_DEFAULT_DESIRED_NUM_SPLITS, None))
    except Exception:  # pylint: disable=broad-except
      _LOGGER.warning(
          'Exception while splitting UnboundedSource. Source not split.',
          exc_info=True)
      yield restriction
      return

    if not split_sources:
      yield restriction
      return

    # A non-UnboundedSource split result is a contract violation, not a
    # refusal, so fail loudly (outside the try/except above).
    for split_source in split_sources:
      if not isinstance(split_source, UnboundedSource):
        raise TypeError(
            'UnboundedSource.split() produced %r, expected UnboundedSource' %
            (split_source, ))

    for split_source in split_sources:
      yield dataclasses.replace(
          restriction,
          source=split_source,
          checkpoint_mark=None,
          is_done=False,
          finalization_checkpoint_mark=None)

  def restriction_size(self, element, restriction) -> int:
    # TODO(https://github.com/apache/beam/issues/19137): implement backlog
    # estimation.
    return 1

  def restriction_coder(self) -> Coder:
    return self._restriction_coder

  def truncate(self, element, restriction):
    # On drain, stop emitting new records.
    return None


# Module-level stateless singleton, captured via ``RestrictionParam`` at the
# DoFn's class-definition time.
_PROVIDER = _UnboundedSourceRestrictionProvider()


class _FinalizeCheckpointOnce(object):
  def __init__(self, checkpoint_mark: CheckpointMark):
    self._checkpoint_mark = checkpoint_mark
    # The lock keeps finalization idempotent if a runner ever invokes the
    # callback more than once.
    self._lock = threading.Lock()
    self._finalized = False

  def __call__(self) -> None:
    with self._lock:
      if self._finalized:
        return
      self._finalized = True
    # Finalization is best effort: log and swallow so a failing user override
    # does not fail the bundle (matches CheckpointMark.finalize_checkpoint).
    try:
      self._checkpoint_mark.finalize_checkpoint()
    except Exception:  # pylint: disable=broad-except
      _LOGGER.warning(
          'Error finalizing UnboundedSource checkpoint mark.', exc_info=True)


class _ReadFromUnboundedSourceDoFn(core.DoFn):
  """SDF wrapper driving an :class:`UnboundedReader` for one restriction.

  Module-level so stdlib pickle and cloudpickle can serialise the DoFn. The
  restriction provider is the module-level :data:`_PROVIDER` singleton.
  """
  def __init__(
      self,
      poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
      max_records_per_bundle: int = _DEFAULT_MAX_RECORDS_PER_BUNDLE,
      max_read_time_seconds: float = _DEFAULT_MAX_READ_TIME_SECONDS,
      _now: Optional[Callable[[], float]] = None):
    self._poll_interval = poll_interval
    self._max_records_per_bundle = max_records_per_bundle
    self._max_read_time_seconds = max_read_time_seconds
    # Monotonic clock seam; tests inject a deterministic clock.
    self._now = _now
    # Per-worker reader cache; created in setup(), never serialized.
    self._reader_cache = None  # type: Optional[_ReaderCache]

  def setup(self):
    self._reader_cache = _ReaderCache()

  def teardown(self):
    if self._reader_cache is not None:
      self._reader_cache.close_all()
      self._reader_cache = None

  @core.DoFn.unbounded_per_element()
  def process(
      self,
      unused_element,
      bundle_finalizer=core.DoFn.BundleFinalizerParam,
      tracker=core.DoFn.RestrictionParam(_PROVIDER),
      watermark_estimator=core.DoFn.WatermarkEstimatorParam(
          ManualWatermarkEstimator.default_provider())):
    # Positional params (element, bundle finalizer) must precede the
    # kwarg-injected ones (tracker, watermark estimator).
    assert isinstance(tracker, sdf_utils.RestrictionTrackerView)
    inner_tracker = _unwrap_tracker(tracker)
    if inner_tracker is not None and self._reader_cache is not None:
      # Let this bundle reclaim a reader parked by the prior bundle and re-park
      # it on self-checkpoint. No cache means setup() was skipped.
      inner_tracker._reader_cache = self._reader_cache
    initial = tracker.current_restriction()
    now = self._now or time.monotonic
    records_emitted = 0
    # Armed on the first emitted record so reader startup is excluded.
    read_deadline = None  # type: Optional[float]
    try:
      while True:
        holder = [None]
        if not tracker.try_claim(holder):
          # EOF: advance the estimator to the tracker's MAX watermark so
          # downstream event-time windows can close.
          _set_watermark_if_greater(
              watermark_estimator, tracker.current_restriction().watermark)
          break
        record = holder[0]
        if record is _NO_DATA:
          # No data now: advance the watermark and self-checkpoint with the
          # poll delay so an idle source backs off before resuming.
          _set_watermark_if_greater(
              watermark_estimator, tracker.current_restriction().watermark)
          tracker.defer_remainder(Duration(seconds=self._poll_interval))
          break
        # The third tuple field is the source watermark. The record timestamp
        # remains the output event time. Emit the element before advancing the
        # estimator so a reader that reports MAX on the same claim as its final
        # record cannot push the output watermark past that record first.
        value, record_timestamp, source_watermark = record
        yield TimestampedValue(value, record_timestamp)
        _set_watermark_if_greater(watermark_estimator, source_watermark)
        records_emitted += 1
        if read_deadline is None:
          read_deadline = now() + self._max_read_time_seconds
        # A busy reader never hits the EOF or no-data branch. Bound the bundle
        # by record count and elapsed time so the runner commits the checkpoint
        # and runs finalization, then resume with no delay. The deadline is
        # checked between records; a reader that blocks inside advance() can
        # overrun it, so the record cap is the hard backstop.
        reached_record_cap = records_emitted >= self._max_records_per_bundle
        if reached_record_cap or now() >= read_deadline:
          tracker.defer_remainder()
          break
    finally:
      current = tracker.current_restriction()
      try:
        # Register finalization only when a checkpoint was cut this bundle.
        # The SDK bundle finalizer applies no deadline, so finalization is
        # unbounded best effort.
        finalize_mark = current.finalization_checkpoint_mark
        if current is not initial and finalize_mark is not None:
          bundle_finalizer.register(_FinalizeCheckpointOnce(finalize_mark))
      finally:
        # The EOF and self-checkpoint paths already closed or parked the
        # reader, so this is a no-op there. It closes a reader still held when
        # process() exits early, e.g. a downstream yield raised before any
        # checkpoint.
        if inner_tracker is not None:
          inner_tracker._close_reader_if_open()
        else:
          _LOGGER.warning(
              'UnboundedSource DoFn could not close a reader because the SDF '
              'tracker wrapper did not expose '
              '_UnboundedSourceRestrictionTracker (got %s). Reader resources '
              'may remain open until garbage collection.',
              type(tracker).__name__)


def _unwrap_tracker(
    tracker: Any) -> Optional['_UnboundedSourceRestrictionTracker']:
  """Returns the :class:`_UnboundedSourceRestrictionTracker` behind the SDF
  view and threadsafe wrappers, or None when the chain is unexpected."""
  inner = tracker
  if hasattr(inner, '_threadsafe_restriction_tracker'):
    inner = inner._threadsafe_restriction_tracker
  if hasattr(inner, '_restriction_tracker'):
    inner = inner._restriction_tracker
  if isinstance(inner, _UnboundedSourceRestrictionTracker):
    return inner
  return None


def _set_watermark_if_greater(
    watermark_estimator, new_watermark: Timestamp) -> None:
  # ManualWatermarkEstimator.set_watermark raises on regression, so only ever
  # advance it (a regressing reader watermark is absorbed here).
  current = watermark_estimator.current_watermark()
  if current is None or new_watermark > current:
    watermark_estimator.set_watermark(new_watermark)


class ReadFromUnboundedSource(PTransform):
  """Reads an :class:`UnboundedSource`.

  Most users should prefer :class:`apache_beam.io.Read`, which dispatches an
  ``UnboundedSource`` here automatically::

      p | beam.io.Read(MyUnboundedSource())

  Args:
    source: the :class:`UnboundedSource` to read.
    poll_interval: resume delay in seconds applied when the reader has no data,
      which bounds how often an idle source is polled. Must be >= 0.
    max_records_per_bundle: a busy reader self-checkpoints after emitting this
      many records in one bundle. Must be >= 1. Defaults to 10000.
    max_read_time_seconds: a busy reader self-checkpoints after this many
      seconds in one bundle. Must be > 0. Defaults to 10.0. The deadline is
      checked between records, so a reader that blocks inside ``advance()`` may
      overrun it; ``max_records_per_bundle`` is the hard backstop.

  The bundle self-checkpoints as soon as either cap is reached.
  """
  def __init__(
      self,
      source: UnboundedSource,
      poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
      max_records_per_bundle: int = _DEFAULT_MAX_RECORDS_PER_BUNDLE,
      max_read_time_seconds: float = _DEFAULT_MAX_READ_TIME_SECONDS):
    if not isinstance(source, UnboundedSource):
      raise TypeError('source must be an UnboundedSource, got %r' % (source, ))
    if max_records_per_bundle < 1:
      raise ValueError(
          'max_records_per_bundle must be >= 1, got %r' %
          (max_records_per_bundle, ))
    if max_read_time_seconds <= 0:
      raise ValueError(
          'max_read_time_seconds must be > 0, got %r' %
          (max_read_time_seconds, ))
    if poll_interval < 0:
      raise ValueError(
          'poll_interval must be >= 0, got %r' % (poll_interval, ))
    super().__init__()
    self._source = source
    self._poll_interval = poll_interval
    self._max_records_per_bundle = max_records_per_bundle
    self._max_read_time_seconds = max_read_time_seconds

  def expand(self, pbegin):
    source = self._source
    output_coder = source.default_output_coder()
    # The source is the SDF element used to derive the initial restriction.
    # process() reads from the restriction, so it does not use the element
    # directly.
    output = (
        pbegin
        | 'Create' >> core.Create([source])
        | 'ReadUnbounded' >> core.ParDo(
            _ReadFromUnboundedSourceDoFn(
                self._poll_interval,
                self._max_records_per_bundle,
                self._max_read_time_seconds)))
    # Surface an element type only when the global registry already maps it to
    # an equivalent coder. Avoid mutating ``coders.registry`` for a
    # parameterized coder whose instance state would be lost.
    try:
      type_hint = output_coder.to_type_hint()
    except NotImplementedError:
      type_hint = None
    if type_hint is not None:
      try:
        registered_coder = coders.registry.get_coder(type_hint)
      except Exception:  # pylint: disable=broad-except
        _LOGGER.warning(
            'Could not look up the registered coder for element type %s.',
            type_hint,
            exc_info=True)
      else:
        if registered_coder == output_coder:
          output.element_type = type_hint
    return output

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self._source.default_output_coder()
