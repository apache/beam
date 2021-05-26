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

import logging
import re
import unittest

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.common import NameContext
from apache_beam.runners.worker import log_handler
from apache_beam.runners.worker import statesampler
from apache_beam.utils import thread_pool_executor

_LOGGER = logging.getLogger(__name__)


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):
  def __init__(self):
    self.log_records_received = []

  def Logging(self, request_iterator, context):

    for log_record in request_iterator:
      self.log_records_received.append(log_record)

    yield beam_fn_api_pb2.LogControl()


class FnApiLogRecordHandlerTest(unittest.TestCase):
  def setUp(self):
    self.test_logging_service = BeamFnLoggingServicer()
    self.server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        self.test_logging_service, self.server)
    self.test_port = self.server.add_insecure_port('[::]:0')
    self.server.start()

    self.logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    self.logging_service_descriptor.url = 'localhost:%s' % self.test_port
    self.fn_log_handler = log_handler.FnApiLogRecordHandler(
        self.logging_service_descriptor)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(self.fn_log_handler)

  def tearDown(self):
    # wait upto 5 seconds.
    self.server.stop(5)

  def _verify_fn_log_handler(self, num_log_entries):
    msg = 'Testing fn logging'
    _LOGGER.debug('Debug Message 1')
    for idx in range(num_log_entries):
      _LOGGER.info('%s: %s', msg, idx)
    _LOGGER.debug('Debug Message 2')

    # Wait for logs to be sent to server.
    self.fn_log_handler.close()

    num_received_log_entries = 0
    for outer in self.test_logging_service.log_records_received:
      for log_entry in outer.log_entries:
        self.assertEqual(
            beam_fn_api_pb2.LogEntry.Severity.INFO, log_entry.severity)
        self.assertEqual(
            '%s: %s' % (msg, num_received_log_entries), log_entry.message)
        self.assertTrue(
            re.match(r'.*log_handler_test.py:\d+', log_entry.log_location),
            log_entry.log_location)
        self.assertGreater(log_entry.timestamp.seconds, 0)
        self.assertGreaterEqual(log_entry.timestamp.nanos, 0)
        num_received_log_entries += 1

    self.assertEqual(num_received_log_entries, num_log_entries)

  def assertContains(self, haystack, needle):
    self.assertTrue(
        needle in haystack, 'Expected %r to contain %r.' % (haystack, needle))

  def test_exc_info(self):
    try:
      raise ValueError('some message')
    except ValueError:
      _LOGGER.error('some error', exc_info=True)

    self.fn_log_handler.close()

    log_entry = self.test_logging_service.log_records_received[0].log_entries[0]
    self.assertContains(log_entry.message, 'some error')
    self.assertContains(log_entry.trace, 'some message')
    self.assertContains(log_entry.trace, 'log_handler_test.py')

  def test_context(self):
    try:
      with statesampler.instruction_id('A'):
        tracker = statesampler.for_test()
        with tracker.scoped_state(NameContext('name', 'tid'), 'stage'):
          _LOGGER.info('message a')
      with statesampler.instruction_id('B'):
        _LOGGER.info('message b')
      _LOGGER.info('message c')

      self.fn_log_handler.close()
      a, b, c = sum(
          [list(logs.log_entries)
           for logs in self.test_logging_service.log_records_received], [])

      self.assertEqual(a.instruction_id, 'A')
      self.assertEqual(b.instruction_id, 'B')
      self.assertEqual(c.instruction_id, '')

      self.assertEqual(a.transform_id, 'tid')
      self.assertEqual(b.transform_id, '')
      self.assertEqual(c.transform_id, '')

    finally:
      statesampler.set_current_tracker(None)


# Test cases.
data = {
    'one_batch': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE - 47,
    'exact_multiple': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE,
    'multi_batch': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE * 3 + 47
}


def _create_test(name, num_logs):
  setattr(
      FnApiLogRecordHandlerTest,
      'test_%s' % name,
      lambda self: self._verify_fn_log_handler(num_logs))


for test_name, num_logs_entries in data.items():
  _create_test(test_name, num_logs_entries)

if __name__ == '__main__':
  unittest.main()
