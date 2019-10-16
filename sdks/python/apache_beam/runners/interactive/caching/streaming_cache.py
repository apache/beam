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
      self._timestamp = Timestamp.of(0)
      self._readers = {}
      self._headers = {}
      readers = [r.read() for r in readers]

      # The header allows for metadata about an entire stream, so that the data
      # isn't copied per record.
      for r in readers:
        header = InteractiveStreamHeader()
        header.ParseFromString(next(r))

        # Main PCollections in Beam have a tag as None. Deserializing a Proto
        # with an empty tag becomes an empty string. Here we normalize to what
        # Beam expects.
        self._headers[header.tag if header.tag else None] = header
        self._readers[header.tag if header.tag else None] = r

      self._watermarks = {tag: timestamp.MIN_TIMESTAMP for tag in self._headers}

    def read(self):
      """Reads records from PCollection readers.
      """
      records = {}
      for tag, r in self._readers.items():
        try:
          record = InteractiveStreamRecord()
          record.ParseFromString(next(r))
          records[tag] = record
        except StopIteration:
          pass

      events = []
      if not records:
        self.advance_watermark(timestamp.MAX_TIMESTAMP, events)

      records = sorted(records.items(), key=lambda x: x[1].processing_time)
      for tag, r in records:
        # We always send the processing time event first so that the TestStream
        # can sleep so as to emulate the original stream.
        self.advance_processing_time(
            Timestamp.from_proto(r.processing_time), events)
        self.advance_watermark(Timestamp.from_proto(r.watermark), events,
                               tag=tag)

        events.append(TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[r.element], tag=tag)))
      return events

    def advance_processing_time(self, processing_time, events):
      """Advances the internal clock state and injects an AdvanceProcessingTime
         event.
      """
      if self._timestamp != processing_time:
        duration = timestamp.Duration(
            micros=processing_time.micros - self._timestamp.micros)

        self._timestamp = processing_time
        processing_time_event = TestStreamPayload.Event.AdvanceProcessingTime(
            advance_duration=duration.micros)
        events.append(TestStreamPayload.Event(
            processing_time_event=processing_time_event))

    def advance_watermark(self, watermark, events, tag=None):
      """Advances the internal clock state and injects an AdvanceWatermark
         event.
      """
      if self._watermarks[tag] < watermark:
        self._watermarks[tag] = watermark
        payload = TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=self._watermarks[tag].micros, tag=tag))
        events.append(payload)

    def stream_time(self):
      return self._timestamp

    def watermark(self):
      return min(self._watermarks.values())

  def reader(self):
    return StreamingCache.Reader(self._readers)
