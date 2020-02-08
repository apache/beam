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

"""Common utility class to help SDK harness to execute an SDF. """

from __future__ import absolute_import
from __future__ import division

import logging
import threading
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import NamedTuple
from typing import Optional
from typing import Tuple

from apache_beam.utils import timestamp
from apache_beam.utils.windowed_value import WindowedValue

if TYPE_CHECKING:
  from apache_beam.io.iobase import RestrictionTracker
  from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)

SplitResultPrimary = NamedTuple(
    'SplitResultPrimary', [('primary_value', WindowedValue)])

SplitResultResidual = NamedTuple(
    'SplitResultResidual',
    [('residual_value', WindowedValue),
     ('current_watermark', timestamp.Timestamp),
     ('deferred_timestamp', timestamp.Duration)])


class ThreadsafeRestrictionTracker(object):
  """A thread-safe wrapper which wraps a `RestritionTracker`.

  This wrapper guarantees synchronization of modifying restrictions across
  multi-thread.
  """
  def __init__(self, restriction_tracker):
    # type: (RestrictionTracker) -> None
    from apache_beam.io.iobase import RestrictionTracker
    if not isinstance(restriction_tracker, RestrictionTracker):
      raise ValueError(
          'Initialize ThreadsafeRestrictionTracker requires'
          'RestrictionTracker.')
    self._restriction_tracker = restriction_tracker
    # Records an absolute timestamp when defer_remainder is called.
    self._deferred_timestamp = None
    self._lock = threading.RLock()
    self._deferred_residual = None
    self._deferred_watermark = None

  def current_restriction(self):
    with self._lock:
      return self._restriction_tracker.current_restriction()

  def try_claim(self, position):
    with self._lock:
      return self._restriction_tracker.try_claim(position)

  def defer_remainder(self, deferred_time=None):
    """Performs self-checkpoint on current processing restriction with an
    expected resuming time.

    Self-checkpoint could happen during processing elements. When executing an
    DoFn.process(), you may want to stop processing an element and resuming
    later if current element has been processed quit a long time or you also
    want to have some outputs from other elements. ``defer_remainder()`` can be
    called on per element if needed.

    Args:
      deferred_time: A relative ``timestamp.Duration`` that indicates the ideal
        time gap between now and resuming, or an absolute
        ``timestamp.Timestamp`` for resuming execution time. If the time_delay
        is None, the deferred work will be executed as soon as possible.
    """

    # Record current time for calculating deferred_time later.
    self._deferred_timestamp = timestamp.Timestamp.now()
    if (deferred_time and not isinstance(deferred_time, timestamp.Duration) and
        not isinstance(deferred_time, timestamp.Timestamp)):
      raise ValueError(
          'The timestamp of deter_remainder() should be a '
          'Duration or a Timestamp, or None.')
    self._deferred_watermark = deferred_time
    checkpoint = self.try_split(0)
    if checkpoint:
      _, self._deferred_residual = checkpoint

  def check_done(self):
    with self._lock:
      return self._restriction_tracker.check_done()

  def current_progress(self):
    with self._lock:
      return self._restriction_tracker.current_progress()

  def try_split(self, fraction_of_remainder):
    with self._lock:
      return self._restriction_tracker.try_split(fraction_of_remainder)

  def deferred_status(self):
    # type: () -> Optional[Tuple[Any, Timestamp]]

    """Returns deferred work which is produced by ``defer_remainder()``.

    When there is a self-checkpoint performed, the system needs to fulfill the
    DelayedBundleApplication with deferred_work for a  ProcessBundleResponse.
    The system calls this API to get deferred_residual with watermark together
    to help the runner to schedule a future work.

    Returns: (deferred_residual, time_delay) if having any residual, else None.
    """
    if self._deferred_residual:
      # If _deferred_watermark is None, create Duration(0).
      if not self._deferred_watermark:
        self._deferred_watermark = timestamp.Duration()
      # If an absolute timestamp is provided, calculate the delta between
      # the absoluted time and the time deferred_status() is called.
      elif isinstance(self._deferred_watermark, timestamp.Timestamp):
        self._deferred_watermark = (
            self._deferred_watermark - timestamp.Timestamp.now())
      # If a Duration is provided, the deferred time should be:
      # provided duration - the spent time since the defer_remainder() is
      # called.
      elif isinstance(self._deferred_watermark, timestamp.Duration):
        self._deferred_watermark -= (
            timestamp.Timestamp.now() - self._deferred_timestamp)
      return self._deferred_residual, self._deferred_watermark
    return None


class RestrictionTrackerView(object):
  """A DoFn view of thread-safe RestrictionTracker.

  The RestrictionTrackerView wraps a ThreadsafeRestrictionTracker and only
  exposes APIs that will be called by a ``DoFn.process()``. During execution
  time, the RestrictionTrackerView will be fed into the ``DoFn.process`` as a
  restriction_tracker.
  """
  def __init__(self, threadsafe_restriction_tracker):
    if not isinstance(threadsafe_restriction_tracker,
                      ThreadsafeRestrictionTracker):
      raise ValueError(
          'Initialize RestrictionTrackerView requires '
          'ThreadsafeRestrictionTracker.')
    self._threadsafe_restriction_tracker = threadsafe_restriction_tracker

  def current_restriction(self):
    return self._threadsafe_restriction_tracker.current_restriction()

  def try_claim(self, position):
    return self._threadsafe_restriction_tracker.try_claim(position)

  def defer_remainder(self, deferred_time=None):
    self._threadsafe_restriction_tracker.defer_remainder(deferred_time)
