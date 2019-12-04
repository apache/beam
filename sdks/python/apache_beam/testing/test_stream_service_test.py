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

from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.testing.test_stream_service import TestStreamServiceController

# Nose automatically detects tests if they match a regex. Here, it mistakens
# these protos as tests. For more info see the Nose docs at:
# https://nose.readthedocs.io/en/latest/writing_tests.html
TestStreamPayload.__test__ = False
TestStreamFileHeader.__test__ = False
TestStreamFileRecord.__test__ = False


class TestStreamServiceTest(unittest.TestCase):
  def events(self):
    events = []
    for i in range(10):
      e = TestStreamPayload.Event()
      e.element_event.elements.append(
          TestStreamPayload.TimestampedElement(timestamp=i))
      events.append(e)
    return events

  def setUp(self):
    self.controller = TestStreamServiceController(self.events())
    self.controller.start()

    channel = grpc.insecure_channel(self.controller.endpoint)
    self.stub = beam_runner_api_pb2_grpc.TestStreamServiceStub(channel)

  def tearDown(self):
    self.controller.stop()

  def test_normal_run(self):
    r = self.stub.Events(beam_runner_api_pb2.EventsRequest())
    events = [e for e in r]
    expected_events = [e for e in self.events()]

    self.assertEqual(events, expected_events)

  def test_multiple_sessions(self):
    resp_a = self.stub.Events(beam_runner_api_pb2.EventsRequest())
    resp_b = self.stub.Events(beam_runner_api_pb2.EventsRequest())

    events_a = []
    events_b = []

    done = False
    while not done:
      a_is_done = False
      b_is_done = False
      try:
        events_a.append(next(resp_a))
      except StopIteration:
        a_is_done = True

      try:
        events_b.append(next(resp_b))
      except StopIteration:
        b_is_done = True

      done = a_is_done and b_is_done

    expected_events = [e for e in self.events()]

    self.assertEqual(events_a, expected_events)
    self.assertEqual(events_b, expected_events)


if __name__ == '__main__':
  unittest.main()
