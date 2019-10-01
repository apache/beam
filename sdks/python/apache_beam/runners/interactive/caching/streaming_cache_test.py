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

import grpc
import unittest

from apache_beam import coders
from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.utils import timestamp

from google.protobuf import timestamp_pb2

def to_timestamp_proto(timestamp_secs):
  """Converts seconds since epoch to a google.protobuf.Timestamp.

  Args:
    timestamp_secs: The timestamp in seconds since epoch.
  """
  seconds = int(timestamp_secs)
  nanos = int((timestamp_secs - seconds) * 10**9)
  return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)

class InMemoryReader:
  def __init__(self):
    self._records = []
    self._coder = coders.FastPrimitivesCoder()

  def add_element(self, element, event_time, processing_time, watermark):
    element_payload = TestStreamPayload.TimestampedElement(
        encoded_element=self._coder.encode(element),
        timestamp=event_time * 10**6)
    record = InteractiveStreamRecord(
        element=element_payload,
        processing_time=to_timestamp_proto(processing_time),
        watermark=to_timestamp_proto(watermark))
    self._records.append(record.SerializeToString())

  def read(self):
    for r in self._records:
      yield r

def all_events(reader):
  events = []
  while True:
    e = reader.read()
    if not e:
      break
    events.append(e)
  return events

class StreamingCacheTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_normal_run(self):
    in_memory_reader = InMemoryReader()
    in_memory_reader.add_element(
        element=0,
        event_time=0,
        processing_time=0,
        watermark=0)
    cache = StreamingCache([in_memory_reader])
    reader = cache.reader()
    coder = coders.FastPrimitivesCoder()
    events = all_events(reader)

    expected = []
    expected.append([
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=0)),
        TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=0)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(elements=[
                TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(0),
                    timestamp=0)]))
          ])
    expected.append([TestStreamPayload.Event(
      watermark_event=TestStreamPayload.Event.AdvanceWatermark(
        new_watermark=timestamp.MAX_TIMESTAMP.micros))])
    self.assertSequenceEqual(events, expected)

  def test_advances_processing_time(self):
    in_memory_reader = InMemoryReader()
    in_memory_reader.add_element(
        element=0,
        event_time=0,
        processing_time=0,
        watermark=0)
    in_memory_reader.add_element(
        element=1,
        event_time=10,
        processing_time=10,
        watermark=10)
    cache = StreamingCache([ in_memory_reader ])
    reader = cache.reader()
    coder = coders.FastPrimitivesCoder()
    events = all_events(reader)

    expected = []
    expected.append([
      TestStreamPayload.Event(
        processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
          advance_duration=0)),
      TestStreamPayload.Event(
        watermark_event=TestStreamPayload.Event.AdvanceWatermark(
          new_watermark=0)),
      TestStreamPayload.Event(
        element_event=TestStreamPayload.Event.AddElements(elements=[
          TestStreamPayload.TimestampedElement(encoded_element=coder.encode(0),
            timestamp=0)]))
        ])
    expected.append([
      TestStreamPayload.Event(
        processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
          advance_duration=10 * 10**6)),
      TestStreamPayload.Event(
        watermark_event=TestStreamPayload.Event.AdvanceWatermark(
          new_watermark=10 * 10**6)),
      TestStreamPayload.Event(
        element_event=TestStreamPayload.Event.AddElements(elements=[
          TestStreamPayload.TimestampedElement(encoded_element=coder.encode(1),
            timestamp=10 * 10**6)]))
        ])
    expected.append([TestStreamPayload.Event(
      watermark_event=TestStreamPayload.Event.AdvanceWatermark(
        new_watermark=timestamp.MAX_TIMESTAMP.micros))])
    self.assertSequenceEqual(events, expected)

if __name__ == '__main__':
  unittest.main()

