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

from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.utils import timestamp

from google.protobuf import timestamp_pb2


def to_timestamp(timestamp_secs):
  """Converts seconds since epoch to an apache_beam.util.Timestamp.
  """
  return timestamp.Timestamp.of(timestamp_secs)

def from_timestamp_proto(timestamp_proto):
  return timestamp.Timestamp(seconds=timestamp_proto.seconds,
                             micros=timestamp_proto.nanos * 1000)

def to_timestamp_usecs(ts):
  """Converts a google.protobuf.Timestamp and
     apache_beam.util.timestamp.Timestamp to seconds since epoch.
  """
  if isinstance(ts, timestamp_pb2.Timestamp):
    return (ts.seconds * 10**6) + (ts.nanos * 10**-3)
  if isinstance(ts, timestamp.Timestamp):
    return ts.micros

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
      self._readers = [reader.read() for reader in readers]
      self._watermark = timestamp.MIN_TIMESTAMP
      self._timestamp = timestamp.MIN_TIMESTAMP

    def read(self):
      """Reads records from PCollection readers.
      """
      records = []
      for r in self._readers:
        try:
          record = InteractiveStreamRecord()
          record.ParseFromString(next(r))
          records.append(record)
        except StopIteration:
          pass

      events = []
      if not records:
        self.advance_watermark(timestamp.MAX_TIMESTAMP, events)

      records.sort(key=lambda x: x.processing_time)
      for r in records:
        self.advance_processing_time(
            from_timestamp_proto(r.processing_time), events)
        self.advance_watermark(from_timestamp_proto(r.watermark), events)

        events.append(TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[r.element])))
      return events

    def advance_processing_time(self, processing_time, events):
      """Advances the internal clock state and injects an AdvanceProcessingTime
         event.
      """
      if self._timestamp != processing_time:
        duration = timestamp.Duration(
            micros=processing_time.micros - self._timestamp.micros)
        if self._timestamp == timestamp.MIN_TIMESTAMP:
          duration = timestamp.Duration(micros=processing_time.micros)
        self._timestamp = to_timestamp(processing_time)
        processing_time_event = TestStreamPayload.Event.AdvanceProcessingTime(
            advance_duration=duration.micros)
        events.append(TestStreamPayload.Event(
            processing_time_event=processing_time_event))

    def advance_watermark(self, watermark, events):
      """Advances the internal clock state and injects an AdvanceWatermark
         event.
      """
      if self._watermark < watermark:
        self._watermark = watermark
        payload = TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=to_timestamp_usecs(self._watermark)))
        events.append(payload)

    def stream_time(self):
      return self._timestamp

    def watermark(self):
      return self._watermark

  def reader(self):
    return StreamingCache.Reader(self._readers)
