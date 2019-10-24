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
from apache_beam.portability.api import beam_interactive_api_pb2 as interactive_api
from apache_beam.portability.api import beam_interactive_api_pb2_grpc as interactive_api_grpc
from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.testing.interactive_stream import InteractiveStreamController
from apache_beam.utils.timestamp import Timestamp


class InMemoryReader(object):
  def read(self):
    coder = coders.FastPrimitivesCoder()
    header = InteractiveStreamHeader(tag=None)

    yield header.SerializeToString()
    for i in range(10):
      element = TestStreamPayload.TimestampedElement(
          encoded_element=coder.encode(i), timestamp=i)
      record = InteractiveStreamRecord(
          element=element,
          processing_time=Timestamp.of(i).to_proto(),
          watermark=Timestamp.of(i).to_proto())
      yield record.SerializeToString()


class InteractiveStreamTest(unittest.TestCase):
  def setUp(self):
    streaming_cache = StreamingCache(readers=[InMemoryReader()])
    self.controller = InteractiveStreamController(streaming_cache)
    self.controller.start()

    channel = grpc.insecure_channel(self.controller.endpoint)
    self.stub = interactive_api_grpc.InteractiveServiceStub(channel)

  def tearDown(self):
    self.controller.stop()

  def test_normal_run(self):
    events = []
    while True:
      e = [e for e in self.stub.Events(interactive_api.EventsRequest())]
      if e[0].end_of_stream:
        break
      events = events + [j for i in e for j in i.events]

    streaming_cache_reader = StreamingCache(readers=[InMemoryReader()]).reader()
    expected_events = []
    while True:
      e = streaming_cache_reader.read()
      if e:
        expected_events += e
      else:
        break

    self.assertEqual(events, expected_events)


if __name__ == '__main__':
  unittest.main()
