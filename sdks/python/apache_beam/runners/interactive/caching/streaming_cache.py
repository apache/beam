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

import itertools

import apache_beam as beam
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.cache_manager import CacheManager
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Timestamp


class StreamingCacheReceiver(beam.transforms.PTransform):
  """Marks a PCollection to be read from cache.

  This class is used in the PipelineInstrument to mark that an unbounded
  Pcollection should be read from cache. This is because the TestStream needs
  to know all the PCollections before being created.
  """
  def expand(self, pbegin):
    assert isinstance(pbegin, beam.pvalue.PBegin)
    self.pipeline = pbegin.pipeline

    return beam.pvalue.PCollection(self.pipeline, is_bounded=False)

  def get_windowing(self, unused_inputs):
    return beam.Windowing(beam.window.GlobalWindows())


class StreamingCache(CacheManager):
  """Abstraction that holds the logic for reading and writing to cache.
  """
  def __init__(self, underlying_cache_manager):
    self._cache_manager = underlying_cache_manager

  def exists(self, *labels):
    return self._cache_manager.exists(*labels)

  def _latest_version(self, *labels):
    return self._cache_manager._latest_version(*labels)

  def read(self, *labels):
    if not self.exists(*labels):
      return itertools.chain([]), -1

    reader, version = self._cache_manager.read(*labels)
    header = next(reader)
    return StreamingCache.Reader([header], [reader]).read(), version

  def read_multiple(self, labels):
    readers = [self._cache_manager.read(l)[0] for l in labels]
    headers = [next(r) for r in readers]
    return StreamingCache.Reader(headers, readers).read()

  def write(self, values, *labels):
    # TODO(BEAM-8335): Modify write to correctly format value to proto.
    return self._cache_manager.write(values, *labels)

  def source(self, *labels):
    return StreamingCacheReceiver()

  def sink(self, *labels):
    # TODO(BEAM-8335): Create a StreamingCache write to cache transform.
    return self._cache_manager.write_transform(*labels)

  def save_pcoder(self, pcoder, *labels):
    self._cache_manager.save_pcoder(pcoder, *labels)

  def load_pcoder(self, *labels):
    return self._cache_manager.load_pcoder(*labels)

  def cleanup(self):
    self._cache_manager.cleanup()

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
      self._headers = {header.tag : header for header in headers}
      self._readers = {h.tag : r for (h, r) in zip(headers, readers)}

      # The watermarks per tag. Useful for introspection in the stream.
      self._watermarks = {tag: timestamp.MIN_TIMESTAMP for tag in self._headers}

      # The most recently read timestamp per tag.
      self._stream_times = {tag: timestamp.MIN_TIMESTAMP
                            for tag in self._headers}

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
          record = next(r)
          records.append((tag, record))
          self._stream_times[tag] = Timestamp.from_proto(record.processing_time)
        except StopIteration:
          pass
      return records

    def _merge_sort(self, previous_events, new_events):
      return sorted(previous_events + new_events,
                    key=lambda x: Timestamp.from_proto(
                        x[1].processing_time),
                    reverse=True)

    def _min_timestamp_of(self, events):
      return (Timestamp.from_proto(events[-1][1].processing_time)
              if events else timestamp.MAX_TIMESTAMP)

    def _event_stream_caught_up_to_target(self, events, target_timestamp):
      empty_events = not events
      stream_is_past_target = self._min_timestamp_of(events) > target_timestamp
      return empty_events or stream_is_past_target

    def read(self):
      """Reads records from PCollection readers.
      """

      # The largest timestamp read from the different streams.
      target_timestamp = timestamp.Timestamp.of(0)

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
          tag, r = events_to_send.pop()

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

    def _advance_watermark(self, watermark, tag):
      """Advances the watermark for tag and returns AdvanceWatermark event.
      """
      self._watermarks[tag] = Timestamp.from_proto(watermark)
      e = TestStreamPayload.Event(
          watermark_event=TestStreamPayload.Event.AdvanceWatermark(
              new_watermark=self._watermarks[tag].micros, tag=tag))
      return e
