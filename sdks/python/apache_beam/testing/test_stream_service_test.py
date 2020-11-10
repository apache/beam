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

import sys
import unittest

import grpc

from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.testing.test_stream_service import TestStreamServiceController

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch  # type: ignore[misc]

# Nose automatically detects tests if they match a regex. Here, it mistakens
# these protos as tests. For more info see the Nose docs at:
# https://nose.readthedocs.io/en/latest/writing_tests.html
TestStreamPayload.__test__ = False  # type: ignore[attr-defined]
TestStreamFileHeader.__test__ = False  # type: ignore[attr-defined]
TestStreamFileRecord.__test__ = False  # type: ignore[attr-defined]


class EventsReader:
  def __init__(self, expected_key):
    self._expected_key = expected_key

  def read_multiple(self, keys):
    if keys != self._expected_key:
      raise ValueError(
          'Expected key ({}) is not argument({})'.format(
              self._expected_key, keys))

    for i in range(10):
      e = TestStreamPayload.Event()
      e.element_event.elements.append(
          TestStreamPayload.TimestampedElement(timestamp=i))
      yield e


EXPECTED_KEY = 'key'
EXPECTED_KEYS = [EXPECTED_KEY]


class TestStreamServiceTest(unittest.TestCase):
  def setUp(self):
    self.controller = TestStreamServiceController(
        EventsReader(expected_key=[('full', EXPECTED_KEY)]))
    self.controller.start()

    channel = grpc.insecure_channel(self.controller.endpoint)
    self.stub = beam_runner_api_pb2_grpc.TestStreamServiceStub(channel)

  def tearDown(self):
    self.controller.stop()

  def test_normal_run(self):
    r = self.stub.Events(
        beam_runner_api_pb2.EventsRequest(output_ids=EXPECTED_KEYS))
    events = [e for e in r]
    expected_events = [
        e for e in EventsReader(
            expected_key=[EXPECTED_KEYS]).read_multiple([EXPECTED_KEYS])
    ]

    self.assertEqual(events, expected_events)

  def test_multiple_sessions(self):
    resp_a = self.stub.Events(
        beam_runner_api_pb2.EventsRequest(output_ids=EXPECTED_KEYS))
    resp_b = self.stub.Events(
        beam_runner_api_pb2.EventsRequest(output_ids=EXPECTED_KEYS))

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

    expected_events = [
        e for e in EventsReader(
            expected_key=[EXPECTED_KEYS]).read_multiple([EXPECTED_KEYS])
    ]

    self.assertEqual(events_a, expected_events)
    self.assertEqual(events_b, expected_events)


@unittest.skipIf(
    sys.version_info < (3, 6), 'The tests require at least Python 3.6 to work.')
class TestStreamServiceStartStopTest(unittest.TestCase):

  # Weak internal use needs to be explicitly imported.
  from grpc import _server

  def setUp(self):
    self.controller = TestStreamServiceController(
        EventsReader(expected_key=[('full', EXPECTED_KEY)]))
    self.assertFalse(self.controller._server_started)
    self.assertFalse(self.controller._server_stopped)

  def tearDown(self):
    self.controller.stop()

  def test_start_when_never_started(self):
    with patch.object(self._server._Server,
                      'start',
                      wraps=self.controller._server.start) as mock_start:
      self.controller.start()
      mock_start.assert_called_once()
      self.assertTrue(self.controller._server_started)
      self.assertFalse(self.controller._server_stopped)

  def test_start_noop_when_already_started(self):
    with patch.object(self._server._Server,
                      'start',
                      wraps=self.controller._server.start) as mock_start:
      self.controller.start()
      mock_start.assert_called_once()
      self.controller.start()
      mock_start.assert_called_once()

  def test_start_noop_when_already_stopped(self):
    with patch.object(self._server._Server,
                      'start',
                      wraps=self.controller._server.start) as mock_start:
      self.controller.start()
      self.controller.stop()
      mock_start.assert_called_once()
      self.controller.start()
      mock_start.assert_called_once()

  def test_stop_noop_when_not_started(self):
    with patch.object(self._server._Server,
                      'stop',
                      wraps=self.controller._server.stop) as mock_stop:
      self.controller.stop()
      mock_stop.assert_not_called()

  def test_stop_when_already_started(self):
    with patch.object(self._server._Server,
                      'stop',
                      wraps=self.controller._server.stop) as mock_stop:
      self.controller.start()
      mock_stop.assert_not_called()
      self.controller.stop()
      mock_stop.assert_called_once()
      self.assertFalse(self.controller._server_started)
      self.assertTrue(self.controller._server_stopped)

  def test_stop_noop_when_already_stopped(self):
    with patch.object(self._server._Server,
                      'stop',
                      wraps=self.controller._server.stop) as mock_stop:
      self.controller.start()
      self.controller.stop()
      mock_stop.assert_called_once()
      self.controller.stop()
      mock_stop.assert_called_once()


if __name__ == '__main__':
  unittest.main()
