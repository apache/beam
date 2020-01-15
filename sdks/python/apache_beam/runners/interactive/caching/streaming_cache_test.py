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

import unittest

from apache_beam import coders
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.utils.timestamp import Timestamp

# Nose automatically detects tests if they match a regex. Here, it mistakens
# these protos as tests. For more info see the Nose docs at:
# https://nose.readthedocs.io/en/latest/writing_tests.html
TestStreamPayload.__test__ = False
TestStreamFileHeader.__test__ = False
TestStreamFileRecord.__test__ = False


class InMemoryReader(object):
  def __init__(self, tag=None):
    self._header = TestStreamFileHeader(tag=tag)
    self._records = []
    self._coder = coders.FastPrimitivesCoder()

  def add_element(self, element, event_time, processing_time):
    element_payload = TestStreamPayload.TimestampedElement(
        encoded_element=self._coder.encode(element),
        timestamp=Timestamp.of(event_time).micros)
    record = TestStreamFileRecord(
        element=element_payload,
        processing_time=Timestamp.of(processing_time).to_proto())
    self._records.append(record)

  def advance_watermark(self, watermark, processing_time):
    record = TestStreamFileRecord(
        watermark=Timestamp.of(watermark).to_proto(),
        processing_time=Timestamp.of(processing_time).to_proto())
    self._records.append(record)

  def header(self):
    return self._header

  def read(self):
    for r in self._records:
      yield r


def all_events(reader):
  events = []
  for e in reader.read():
    events.append(e)
  return events


class StreamingCacheTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_single_reader(self):
    """Tests that we expect to see all the correctly emitted TestStreamPayloads.
    """
    in_memory_reader = InMemoryReader()
    in_memory_reader.add_element(
        element=0,
        event_time=0,
        processing_time=0)
    in_memory_reader.add_element(
        element=1,
        event_time=1,
        processing_time=1)
    in_memory_reader.add_element(
        element=2,
        event_time=2,
        processing_time=2)
    cache = StreamingCache([in_memory_reader])
    reader = cache.reader()
    coder = coders.FastPrimitivesCoder()
    events = all_events(reader)

    expected = [
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(0),
                    timestamp=0)])),
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(1),
                    timestamp=1 * 10**6)])),
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(2),
                    timestamp=2 * 10**6)])),
    ]
    self.assertSequenceEqual(events, expected)

  def test_multiple_readers(self):
    """Tests that the service advances the clock with multiple outputs."""

    letters = InMemoryReader('letters')
    letters.advance_watermark(0, 1)
    letters.add_element(
        element='a',
        event_time=0,
        processing_time=1)
    letters.advance_watermark(10, 11)
    letters.add_element(
        element='b',
        event_time=10,
        processing_time=11)

    numbers = InMemoryReader('numbers')
    numbers.add_element(
        element=1,
        event_time=0,
        processing_time=2)
    numbers.add_element(
        element=2,
        event_time=0,
        processing_time=3)
    numbers.add_element(
        element=2,
        event_time=0,
        processing_time=4)

    late = InMemoryReader('late')
    late.add_element(
        element='late',
        event_time=0,
        processing_time=101)

    cache = StreamingCache([letters, numbers, late])
    reader = cache.reader()
    coder = coders.FastPrimitivesCoder()
    events = all_events(reader)

    expected = [
        # Advances clock from 0 to 1
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=0,
                tag='letters')),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode('a'),
                    timestamp=0)],
                tag='letters')),

        # Advances clock from 1 to 2
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(1),
                    timestamp=0)],
                tag='numbers')),

        # Advances clock from 2 to 3
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(2),
                    timestamp=0)],
                tag='numbers')),

        # Advances clock from 3 to 4
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=1 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode(2),
                    timestamp=0)],
                tag='numbers')),

        # Advances clock from 4 to 11
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=7 * 10**6)),
        TestStreamPayload.Event(
            watermark_event=TestStreamPayload.Event.AdvanceWatermark(
                new_watermark=10 * 10**6,
                tag='letters')),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode('b'),
                    timestamp=10 * 10**6)],
                tag='letters')),

        # Advances clock from 11 to 101
        TestStreamPayload.Event(
            processing_time_event=TestStreamPayload.Event.AdvanceProcessingTime(
                advance_duration=90 * 10**6)),
        TestStreamPayload.Event(
            element_event=TestStreamPayload.Event.AddElements(
                elements=[TestStreamPayload.TimestampedElement(
                    encoded_element=coder.encode('late'),
                    timestamp=0)],
                tag='late')),
    ]

    self.assertSequenceEqual(events, expected)


if __name__ == '__main__':
  unittest.main()
