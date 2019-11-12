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

import unittest

import grpc

from apache_beam import coders
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2_grpc
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.testing.test_stream_service import TestStreamServiceController
from apache_beam.utils.timestamp import Timestamp


class InMemoryReader(object):
  def __init__(self, tag=None):
    self._header = TestStreamFileHeader(tag=None)
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
    for i in range(10):
      element = TestStreamPayload.TimestampedElement(
          encoded_element=self._coder.encode(i), timestamp=i)
      record = TestStreamFileRecord(
          element=element,
          processing_time=Timestamp.of(i).to_proto())
      yield record


class TestStreamServiceTest(unittest.TestCase):
  def setUp(self):
    streaming_cache = StreamingCache(readers=[InMemoryReader()])
    self.controller = TestStreamServiceController(streaming_cache)
    self.controller.start()

    channel = grpc.insecure_channel(self.controller.endpoint)
    self.stub = beam_runner_api_pb2_grpc.TestStreamServiceStub(channel)

  def tearDown(self):
    self.controller.stop()

  def test_normal_run(self):
    session_id = self.stub.Connect(
        beam_runner_api_pb2.ConnectRequest()).session_id

    events = []
    token = None
    while True:
      r = self.stub.Events(
          beam_runner_api_pb2.EventsRequest(session_id=session_id, token=token))
      token = r.token
      if token:
        events.append(r.event)
      else:
        break

    streaming_cache_reader = StreamingCache(readers=[InMemoryReader()]).reader()
    expected_events = []
    for e in streaming_cache_reader.read():
      expected_events.append(e)

    self.assertEqual(events, expected_events)

  def test_multiple_sessions(self):
    session_a = self.stub.Connect(
        beam_runner_api_pb2.ConnectRequest()).session_id
    session_b = self.stub.Connect(
        beam_runner_api_pb2.ConnectRequest()).session_id

    self.assertNotEqual(session_a, session_b)

    events_a = []
    token_a = None

    events_b = []
    token_b = None
    while True:
      r = self.stub.Events(
          beam_runner_api_pb2.EventsRequest(session_id=session_a,
                                            token=token_a))
      token_a = r.token
      if token_a:
        events_a.append(r.event)

      r = self.stub.Events(
          beam_runner_api_pb2.EventsRequest(session_id=session_b,
                                            token=token_b))
      token_b = r.token
      if token_b:
        events_b.append(r.event)
      elif not token_a and not token_b:
        break

    streaming_cache_reader = StreamingCache(readers=[InMemoryReader()]).reader()
    expected_events = []
    for e in streaming_cache_reader.read():
      expected_events.append(e)

    self.assertEqual(events_a, expected_events)
    self.assertEqual(events_b, expected_events)


if __name__ == '__main__':
  unittest.main()
