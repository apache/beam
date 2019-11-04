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

from __future__ import absolute_import

from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Timestamp


class StreamingCache(object):
  """Abstraction that holds the logic for reading and writing to cache.
  """
  def __init__(self, readers):
    self._readers = readers

  class Reader(object):
    """Abstraction that reads from PCollection readers.

    This class is an Abstraction layer over multiple PCollection readers to be
    used for supplying the Interactive Service with TestStream events.

    This class is also responsible for holding the state of the clock, injecting
    clock advancement events, and watermark advancement events.
    """
    def __init__(self, readers):
      # This timestamp is used as the monotonic clock to order events in the
      # replay.
      self._monotonic_clock = timestamp.Timestamp.of(0)

      # The maximum timestamp read.
      self._target_timestamp = timestamp.Timestamp.of(0)

      # The PCollection cache readers.
      self._readers = {}

      # The file headers that are metadata for that particular PCollection.
      self._headers = {}

      # The header allows for metadata about an entire stream, so that the data
      # isn't copied per record.
      readers = [r.read() for r in readers]
      for r in readers:
        header = InteractiveStreamHeader()
        header.ParseFromString(next(r))

        # Main PCollections in Beam have a tag as None. Deserializing a Proto
        # with an empty tag becomes an empty string. Here we normalize to what
        # Beam expects.
        self._headers[header.tag if header.tag else None] = header
        self._readers[header.tag if header.tag else None] = r

      # The watermarks per tag. Useful for introspection in the stream.
      self._watermarks = {tag: timestamp.MIN_TIMESTAMP for tag in self._headers}

      # The most recently read timestamp per tag.
      self._stream_times = {tag: timestamp.MIN_TIMESTAMP
                            for tag in self._headers}

    def _read_next(self):
      """Reads the next iteration of elements from each stream.
      """
      records = []
      for tag, r in self._readers.items():
        # The target_timestamp is the maximum timestamp that was read from the
        # stream. Some readers may have elements that are less than this. Thus,
        # we skip all readers that already have elements that are at this
        # timestamp so that we don't read everything into memory.
        if self._stream_times[tag] >= self._target_timestamp:
          continue
        try:
          record = InteractiveStreamRecord()
          record.ParseFromString(next(r))
          records.append((tag, record))
          self._stream_times[tag] = Timestamp.from_proto(
              record.processing_time)
        except StopIteration:
          pass
      return records

    def read(self):
      """Reads records from PCollection readers.
      """
      # We use a generator here because the underlying readers may have to much
      # data to read into memory.

      events = []
      while True:
        # Read the next set of events. The read events will most likely be
        # out of order if there are multiple readers. Here we sort them into
        # a more manageable state.
        events = events + self._read_next()
        events = sorted(events,
                        key=lambda x: Timestamp.from_proto(
                            x[1].processing_time),
                        reverse=True)
        if not events:
          break

        # Retrieves the minimum timestamp from the read events.
        min_timestamp = (
            lambda: Timestamp.from_proto(events[-1][1].processing_time)
            if events else timestamp.MAX_TIMESTAMP)

        # Get the next largest timestamp in the stream. This is used as the
        # timestamp for readers to "catch-up" to. This will only read from
        # readers with a timestamp less than this.
        self._target_timestamp = min_timestamp()

        # Loop through the elements with the correct timestamp.
        while events and min_timestamp() <= self._target_timestamp:
          tag, r = events.pop()

          # First advance the clock to match the time of the stream. This has
          # a side-effect of also advancing this cache's clock.
          curr_timestamp = Timestamp.from_proto(r.processing_time)
          if curr_timestamp > self._monotonic_clock:
            yield self._advance_processing_time(curr_timestamp)

          # Then, send either a new element or watermark.
          if r.HasField('element'):
            yield self._add_element(r.element, tag)
          elif r.HasField('watermark'):
            yield self._advance_watermark(r.watermark, tag)
        self._target_timestamp = min_timestamp()

    def _add_element(self, element, tag):
      """Constructs an AddElement event for the specified element and tag.
      """
      return TestStreamPayload.Event(
          element_event=TestStreamPayload.Event.AddElements(
              elements=[element], tag=tag))

    def _advance_processing_time(self, new_timestamp):
      """Advances the internal clock and returns an AdvanceProcessingTime event.
      """
      advancy_by = new_timestamp.micros - self._monotonic_clock.micros
      e = TestStreamPayload.Event(
          processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
              advance_duration=advancy_by))
      self._monotonic_clock = new_timestamp
      return e

    def _advance_watermark(self, watermark, tag):
      """Advances the watermark for tag and returns AdvanceWatermark event.
      """
      self._watermarks[tag] = Timestamp.from_proto(watermark)
      e = TestStreamPayload.Event(
          watermark_event=TestStreamPayload.Event.AdvanceWatermark(
              new_watermark=self._watermarks[tag].micros, tag=tag))
      return e

    def stream_time(self):
      return self._monotonic_clock

    def watermarks(self):
      return self._watermarks

    def watermark(self):
      return min(self._watermarks.values())

  def reader(self):
    return StreamingCache.Reader(self._readers)
