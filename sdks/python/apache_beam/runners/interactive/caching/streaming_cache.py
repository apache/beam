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

from __future__ import absolute_import

import logging
import os
import shutil
import tempfile
import time
import traceback
from collections import OrderedDict

from google.protobuf.message import DecodeError

import apache_beam as beam
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.cache_manager import CacheManager
from apache_beam.runners.interactive.cache_manager import SafeFastPrimitivesCoder
from apache_beam.testing.test_stream import OutputFormat
from apache_beam.testing.test_stream import ReverseTestStream
from apache_beam.utils import timestamp

# We don't have an explicit pathlib dependency because this code only works with
# the interactive target installed which has an indirect dependency on pathlib
# and pathlib2 through ipython>=5.9.0.
try:
  from pathlib import Path
except ImportError:
  from pathlib2 import Path  # python 2 backport

_LOGGER = logging.getLogger(__name__)


class StreamingCacheSink(beam.PTransform):
  """A PTransform that writes TestStreamFile(Header|Records)s to file.

  This transform takes in an arbitrary element stream and writes the list of
  TestStream events (as TestStreamFileRecords) to file. When replayed, this
  will produce the best-effort replay of the original job (e.g. some elements
  may be produced slightly out of order from the original stream).

  Note that this PTransform is assumed to be only run on a single machine where
  the following assumptions are correct: elements come in ordered, no two
  transforms are writing to the same file. This PTransform is assumed to only
  run correctly with the DirectRunner.

  TODO(BEAM-9447): Generalize this to more source/sink types aside from file
  based. Also, generalize to cases where there might be multiple workers
  writing to the same sink.
  """
  def __init__(
      self,
      cache_dir,
      filename,
      sample_resolution_sec,
      coder=SafeFastPrimitivesCoder()):
    self._cache_dir = cache_dir
    self._filename = filename
    self._sample_resolution_sec = sample_resolution_sec
    self._coder = coder
    self._path = os.path.join(self._cache_dir, self._filename)

  @property
  def path(self):
    """Returns the path the sink leads to."""
    return self._path

  @property
  def size_in_bytes(self):
    """Returns the space usage in bytes of the sink."""
    try:
      return os.stat(self._path).st_size
    except OSError:
      _LOGGER.debug(
          'Failed to calculate cache size for file %s, the file might have not '
          'been created yet. Return 0. %s',
          self._path,
          traceback.format_exc())
      return 0

  def expand(self, pcoll):
    class StreamingWriteToText(beam.DoFn):
      """DoFn that performs the writing.

      Note that the other file writing methods cannot be used in streaming
      contexts.
      """
      def __init__(self, full_path, coder=SafeFastPrimitivesCoder()):
        self._full_path = full_path
        self._coder = coder

        # Try and make the given path.
        Path(os.path.dirname(full_path)).mkdir(parents=True, exist_ok=True)

      def start_bundle(self):
        # Open the file for 'append-mode' and writing 'bytes'.
        self._fh = open(self._full_path, 'ab')

      def finish_bundle(self):
        self._fh.close()

      def process(self, e):
        """Appends the given element to the file.
        """
        self._fh.write(self._coder.encode(e) + b'\n')

    return (
        pcoll
        | ReverseTestStream(
            output_tag=self._filename,
            sample_resolution_sec=self._sample_resolution_sec,
            output_format=OutputFormat.SERIALIZED_TEST_STREAM_FILE_RECORDS,
            coder=self._coder)
        | beam.ParDo(
            StreamingWriteToText(full_path=self._path, coder=self._coder)))


class StreamingCacheSource:
  """A class that reads and parses TestStreamFile(Header|Reader)s.

  This source operates in the following way:

    1. Wait for up to `timeout_secs` for the file to be available.
    2. Read, parse, and emit the entire contents of the file
    3. Wait for more events to come or until `is_cache_complete` returns True
    4. If there are more events, then go to 2
    5. Otherwise, stop emitting.

  This class is used to read from file and send its to the TestStream via the
  StreamingCacheManager.Reader.
  """
  def __init__(
      self,
      cache_dir,
      labels,
      is_cache_complete=None,
      coder=SafeFastPrimitivesCoder()):
    self._cache_dir = cache_dir
    self._coder = coder
    self._labels = labels
    self._path = os.path.join(self._cache_dir, *self._labels)
    self._is_cache_complete = (
        is_cache_complete if is_cache_complete else lambda _: True)

    from apache_beam.runners.interactive.pipeline_instrument import CacheKey
    self._pipeline_id = CacheKey.from_str(labels[-1]).pipeline_id

  def _wait_until_file_exists(self, timeout_secs=30):
    """Blocks until the file exists for a maximum of timeout_secs.
    """
    # Wait for up to `timeout_secs` for the file to be available.
    start = time.time()
    while not os.path.exists(self._path):
      time.sleep(1)
      if time.time() - start > timeout_secs:
        from apache_beam.runners.interactive.pipeline_instrument import CacheKey
        pcollection_var = CacheKey.from_str(self._labels[-1]).var
        raise RuntimeError(
            'Timed out waiting for cache file for PCollection `{}` to be '
            'available with path {}.'.format(pcollection_var, self._path))
    return open(self._path, mode='rb')

  def _emit_from_file(self, fh, tail):
    """Emits the TestStreamFile(Header|Record)s from file.

    This returns a generator to be able to read all lines from the given file.
    If `tail` is True, then it will wait until the cache is complete to exit.
    Otherwise, it will read the file only once.
    """
    # Always read at least once to read the whole file.
    while True:
      pos = fh.tell()
      line = fh.readline()

      # Check if we are at EOF or if we have an incomplete line.
      if not line or (line and line[-1] != b'\n'[0]):
        if not tail:
          break

        # Complete reading only when the cache is complete.
        if self._is_cache_complete(self._pipeline_id):
          break

        # Otherwise wait for new data in the file to be written.
        time.sleep(0.5)
        fh.seek(pos)
      else:
        # The first line at pos = 0 is always the header. Read the line without
        # the new line.
        to_decode = line[:-1]
        proto_cls = TestStreamFileHeader if pos == 0 else TestStreamFileRecord
        msg = self._try_parse_as(proto_cls, to_decode)
        if msg:
          yield msg
        else:
          break

  def _try_parse_as(self, proto_cls, to_decode):
    try:
      msg = proto_cls()
      msg.ParseFromString(self._coder.decode(to_decode))
    except DecodeError:
      _LOGGER.error(
          'Could not parse as %s. This can indicate that the cache is '
          'corruputed. Please restart the kernel. '
          '\nfile: %s \nmessage: %s',
          proto_cls,
          self._path,
          to_decode)
      msg = None
    return msg

  def read(self, tail):
    """Reads all TestStreamFile(Header|TestStreamFileRecord)s from file.

    This returns a generator to be able to read all lines from the given file.
    If `tail` is True, then it will wait until the cache is complete to exit.
    Otherwise, it will read the file only once.
    """
    with self._wait_until_file_exists() as f:
      for e in self._emit_from_file(f, tail):
        yield e


class StreamingCache(CacheManager):
  """Abstraction that holds the logic for reading and writing to cache.
  """
  def __init__(
      self, cache_dir, is_cache_complete=None, sample_resolution_sec=0.1):
    self._sample_resolution_sec = sample_resolution_sec
    self._is_cache_complete = is_cache_complete

    if cache_dir:
      self._cache_dir = cache_dir
    else:
      self._cache_dir = tempfile.mkdtemp(
          prefix='interactive-temp-', dir=os.environ.get('TEST_TMPDIR', None))

    # List of saved pcoders keyed by PCollection path. It is OK to keep this
    # list in memory because once FileBasedCacheManager object is
    # destroyed/re-created it loses the access to previously written cache
    # objects anyways even if cache_dir already exists. In other words,
    # it is not possible to resume execution of Beam pipeline from the
    # saved cache if FileBasedCacheManager has been reset.
    #
    # However, if we are to implement better cache persistence, one needs
    # to take care of keeping consistency between the cached PCollection
    # and its PCoder type.
    self._saved_pcoders = {}
    self._default_pcoder = SafeFastPrimitivesCoder()

    # The sinks to capture data from capturable sources.
    # Dict([str, StreamingCacheSink])
    self._capture_sinks = {}

  @property
  def capture_size(self):
    return sum([sink.size_in_bytes for _, sink in self._capture_sinks.items()])

  @property
  def capture_paths(self):
    return list(self._capture_sinks.keys())

  def exists(self, *labels):
    path = os.path.join(self._cache_dir, *labels)
    return os.path.exists(path)

  # TODO(srohde): Modify this to return the correct version.
  def read(self, *labels):
    """Returns a generator to read all records from file.

    Does not tail.
    """
    if not self.exists(*labels):
      return iter([]), -1

    reader = StreamingCacheSource(
        self._cache_dir, labels, self._is_cache_complete).read(tail=False)

    # Return an empty iterator if there is nothing in the file yet. This can
    # only happen when tail is False.
    try:
      header = next(reader)
    except StopIteration:
      return iter([]), -1
    return StreamingCache.Reader([header], [reader]).read(), 1

  def read_multiple(self, labels):
    """Returns a generator to read all records from file.

    Does tail until the cache is complete. This is because it is used in the
    TestStreamServiceController to read from file which is only used during
    pipeline runtime which needs to block.
    """
    readers = [
        StreamingCacheSource(self._cache_dir, l,
                             self._is_cache_complete).read(tail=True)
        for l in labels
    ]
    headers = [next(r) for r in readers]
    return StreamingCache.Reader(headers, readers).read()

  def write(self, values, *labels):
    """Writes the given values to cache.
    """
    directory = os.path.join(self._cache_dir, *labels[:-1])
    filepath = os.path.join(directory, labels[-1])
    if not os.path.exists(directory):
      os.makedirs(directory)
    with open(filepath, 'ab') as f:
      for v in values:
        if isinstance(v, (TestStreamFileHeader, TestStreamFileRecord)):
          val = v.SerializeToString()
        else:
          val = v
        f.write(self._default_pcoder.encode(val) + b'\n')

  def source(self, *labels):
    """Returns the StreamingCacheManager source.

    This is beam.Impulse() because unbounded sources will be marked with this
    and then the PipelineInstrument will replace these with a TestStream.
    """
    return beam.Impulse()

  def sink(self, labels, is_capture=False):
    """Returns a StreamingCacheSink to write elements to file.

    Note that this is assumed to only work in the DirectRunner as the underlying
    StreamingCacheSink assumes a single machine to have correct element
    ordering.
    """
    filename = labels[-1]
    cache_dir = os.path.join(self._cache_dir, *labels[:-1])
    sink = StreamingCacheSink(cache_dir, filename, self._sample_resolution_sec)
    if is_capture:
      self._capture_sinks[sink.path] = sink
    return sink

  def save_pcoder(self, pcoder, *labels):
    self._saved_pcoders[os.path.join(*labels)] = pcoder

  def load_pcoder(self, *labels):
    return (
        self._default_pcoder if self._default_pcoder is not None else
        self._saved_pcoders[os.path.join(*labels)])

  def cleanup(self):
    if os.path.exists(self._cache_dir):
      shutil.rmtree(self._cache_dir)
    self._saved_pcoders = {}
    self._capture_sinks = {}

  class Reader(object):
    """Abstraction that reads from PCollection readers.

    This class is an Abstraction layer over multiple PCollection readers to be
    used for supplying a TestStream service with events.

    This class is also responsible for holding the state of the clock, injecting
    clock advancement events, and watermark advancement events.
    """
    def __init__(self, headers, readers):
      # This timestamp is used as the monotonic clock to order events in the
      # replay.
      self._monotonic_clock = timestamp.Timestamp.of(0)

      # The PCollection cache readers.
      self._readers = {}

      # The file headers that are metadata for that particular PCollection.
      # The header allows for metadata about an entire stream, so that the data
      # isn't copied per record.
      self._headers = {header.tag: header for header in headers}
      self._readers = OrderedDict(
          ((h.tag, r) for (h, r) in zip(headers, readers)))

      # The most recently read timestamp per tag.
      self._stream_times = {
          tag: timestamp.Timestamp(seconds=0)
          for tag in self._headers
      }

    def _test_stream_events_before_target(self, target_timestamp):
      """Reads the next iteration of elements from each stream.

      Retrieves an element from each stream iff the most recently read timestamp
      from that stream is less than the target_timestamp. Since the amount of
      events may not fit into memory, this StreamingCache reads at most one
      element from each stream at a time.
      """
      records = []
      for tag, r in self._readers.items():
        # The target_timestamp is the maximum timestamp that was read from the
        # stream. Some readers may have elements that are less than this. Thus,
        # we skip all readers that already have elements that are at this
        # timestamp so that we don't read everything into memory.
        if self._stream_times[tag] >= target_timestamp:
          continue
        try:
          record = next(r).recorded_event
          if record.HasField('processing_time_event'):
            self._stream_times[tag] += timestamp.Duration(
                micros=record.processing_time_event.advance_duration)
          records.append((tag, record, self._stream_times[tag]))
        except StopIteration:
          pass
      return records

    def _merge_sort(self, previous_events, new_events):
      return sorted(
          previous_events + new_events, key=lambda x: x[2], reverse=True)

    def _min_timestamp_of(self, events):
      return events[-1][2] if events else timestamp.MAX_TIMESTAMP

    def _event_stream_caught_up_to_target(self, events, target_timestamp):
      empty_events = not events
      stream_is_past_target = self._min_timestamp_of(events) > target_timestamp
      return empty_events or stream_is_past_target

    def read(self):
      """Reads records from PCollection readers.
      """

      # The largest timestamp read from the different streams.
      target_timestamp = timestamp.MAX_TIMESTAMP

      # The events from last iteration that are past the target timestamp.
      unsent_events = []

      # Emit events until all events have been read.
      while True:
        # Read the next set of events. The read events will most likely be
        # out of order if there are multiple readers. Here we sort them into
        # a more manageable state.
        new_events = self._test_stream_events_before_target(target_timestamp)
        events_to_send = self._merge_sort(unsent_events, new_events)
        if not events_to_send:
          break

        # Get the next largest timestamp in the stream. This is used as the
        # timestamp for readers to "catch-up" to. This will only read from
        # readers with a timestamp less than this.
        target_timestamp = self._min_timestamp_of(events_to_send)

        # Loop through the elements with the correct timestamp.
        while not self._event_stream_caught_up_to_target(events_to_send,
                                                         target_timestamp):

          # First advance the clock to match the time of the stream. This has
          # a side-effect of also advancing this cache's clock.
          tag, r, curr_timestamp = events_to_send.pop()
          if curr_timestamp > self._monotonic_clock:
            yield self._advance_processing_time(curr_timestamp)

          # Then, send either a new element or watermark.
          if r.HasField('element_event'):
            r.element_event.tag = tag
            yield r
          elif r.HasField('watermark_event'):
            r.watermark_event.tag = tag
            yield r
        unsent_events = events_to_send
        target_timestamp = self._min_timestamp_of(unsent_events)

    def _advance_processing_time(self, new_timestamp):
      """Advances the internal clock and returns an AdvanceProcessingTime event.
      """
      advancy_by = new_timestamp.micros - self._monotonic_clock.micros
      e = TestStreamPayload.Event(
          processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
              advance_duration=advancy_by))
      self._monotonic_clock = new_timestamp
      return e
