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

"""A native Python streaming IO based on UnboundedSource.

This module provides ``PeriodicImpulseSource``, an example of a native Python
streaming IO built on top of the ``UnboundedSource`` / ``UnboundedReader`` API.

It generates timestamp-based "impulses" at a regular interval, making it useful
as a streaming trigger or heartbeat source in Beam pipelines.

Example usage::

    import apache_beam as beam
    from apache_beam.io.iobase import Read
    from apache_beam.io.periodic_impulse_source import PeriodicImpulseSource

    with beam.Pipeline() as p:
      impulses = (
          p
          | Read(PeriodicImpulseSource(
              fire_interval=1.0,   # seconds between impulses
              max_elements=10))    # stop after 10 impulses
          | beam.Map(print))

This also serves as an example for how to implement your own custom
``UnboundedSource`` with checkpointing and watermark support.
"""

import time

from apache_beam.io.iobase import CheckpointMark
from apache_beam.io.iobase import UnboundedReader
from apache_beam.io.iobase import UnboundedSource
from apache_beam.utils.timestamp import Timestamp


class _PeriodicImpulseCheckpointMark(CheckpointMark):
  """Checkpoint that tracks how many impulses have been emitted."""
  def __init__(self, count=0):
    self.count = count

  def finalize_checkpoint(self):
    pass  # No external resources to finalize


class _PeriodicImpulseReader(UnboundedReader):
  """Reader that generates impulses at a regular interval.

  Each impulse is a ``Timestamp`` representing when the impulse fired.
  """
  def __init__(self, source, checkpoint_mark=None):
    self._source = source
    self._count = checkpoint_mark.count if checkpoint_mark else 0
    self._current = None
    self._current_ts = None

  def start(self):
    return self._try_produce()

  def advance(self):
    if self._source.fire_interval > 0:
      time.sleep(self._source.fire_interval)
    return self._try_produce()

  def _try_produce(self):
    if (self._source.max_elements is not None and
        self._count >= self._source.max_elements):
      return False
    now = Timestamp.now()
    self._current = self._count
    self._current_ts = now
    self._count += 1
    return True

  def get_current(self):
    if self._current is None:
      raise StopIteration('No current element.')
    return self._current

  def get_current_timestamp(self):
    return self._current_ts or Timestamp.now()

  def get_current_record_id(self):
    return str(self._current).encode('utf-8')

  def get_watermark(self):
    if (self._source.max_elements is not None and
        self._count >= self._source.max_elements):
      from apache_beam.utils.timestamp import MAX_TIMESTAMP
      return MAX_TIMESTAMP
    return Timestamp.now()

  def get_checkpoint_mark(self):
    return _PeriodicImpulseCheckpointMark(self._count)

  def get_current_source(self):
    return self._source

  def get_split_backlog_bytes(self):
    if self._source.max_elements is not None:
      return max(0, self._source.max_elements - self._count)
    return UnboundedReader.BACKLOG_UNKNOWN

  def close(self):
    pass


class PeriodicImpulseSource(UnboundedSource):
  """An ``UnboundedSource`` that generates periodic impulses.

  Each output element is an integer sequence number. The associated timestamp
  is the wall-clock time when the impulse was generated.

  This source supports checkpointing: if the pipeline is interrupted and
  resumed, it continues from where it left off based on the element count.

  Args:
    fire_interval: Seconds between each impulse (default 1.0).
    max_elements: If set, stop after this many elements. If None, runs
      indefinitely (truly unbounded).
  """
  def __init__(self, fire_interval=1.0, max_elements=None):
    self.fire_interval = fire_interval
    self.max_elements = max_elements

  def split(self, desired_num_splits, pipeline_options=None):
    """This source does not split — it is a single logical stream."""
    return [self]

  def create_reader(self, pipeline_options, checkpoint_mark=None):
    return _PeriodicImpulseReader(self, checkpoint_mark)

  def requires_deduping(self):
    return False
