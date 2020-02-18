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

from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.utils import timestamp


class StreamingCache(object):
  """Abstraction that holds the logic for reading and writing to cache.
  """
  def __init__(self, readers):
    self._readers = readers

  class Reader(object):
    """Abstraction that reads from PCollection readers.

    This class is an Abstraction layer over multiple PCollection readers to be
    used for supplying a TestStream service with events.

    This class is also responsible for holding the state of the clock, injecting
    clock advancement events, and watermark advancement events.
    """
    def __init__(self, readers):
      # This timestamp is used as the monotonic clock to order events in the
      # replay.
      self._monotonic_clock = timestamp.Timestamp.of(0)

      # The PCollection cache readers.
      self._readers = {}

      # The file headers that are metadata for that particular PCollection.
      # The header allows for metadata about an entire stream, so that the data
      # isn't copied per record.
      self._headers = {r.header().tag: r.header() for r in readers}
      self._readers = {r.header().tag: r.read() for r in readers}

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

  def reader(self):
    return StreamingCache.Reader(self._readers)
