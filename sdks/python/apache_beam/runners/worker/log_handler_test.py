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


import logging
import unittest

import grpc
from concurrent import futures

from apache_beam.runners.api import beam_fn_api_pb2
from apache_beam.runners.worker import log_handler


class BeamFnLoggingServicer(beam_fn_api_pb2.BeamFnLoggingServicer):

  def __init__(self):
    self.log_records_received = []

  def Logging(self, request_iterator, context):

    for log_record in request_iterator:
      self.log_records_received.append(log_record)

    yield beam_fn_api_pb2.LogControl()


class FnApiLogRecordHandlerTest(unittest.TestCase):

  def setUp(self):
    self.test_logging_service = BeamFnLoggingServicer()
    self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    beam_fn_api_pb2.add_BeamFnLoggingServicer_to_server(
        self.test_logging_service, self.server)
    self.test_port = self.server.add_insecure_port('[::]:0')
    self.server.start()

    self.logging_service_descriptor = beam_fn_api_pb2.ApiServiceDescriptor()
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
    logging.debug('Debug Message 1')
    for idx in range(num_log_entries):
      logging.info('%s: %s', msg, idx)
    logging.debug('Debug Message 2')

    # Wait for logs to be sent to server.
    self.fn_log_handler.close()

    num_received_log_entries = 0
    for outer in self.test_logging_service.log_records_received:
      for log_entry in outer.log_entries:
        self.assertEqual(beam_fn_api_pb2.LogEntry.INFO, log_entry.severity)
        self.assertEqual('%s: %s' % (msg, num_received_log_entries),
                         log_entry.message)
        self.assertEqual(u'log_handler_test._verify_fn_log_handler',
                         log_entry.log_location)
        self.assertGreater(log_entry.timestamp.seconds, 0)
        self.assertGreater(log_entry.timestamp.nanos, 0)
        num_received_log_entries += 1

    self.assertEqual(num_received_log_entries, num_log_entries)


# Test cases.
data = {
    'one_batch': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE - 47,
    'exact_multiple': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE,
    'multi_batch': log_handler.FnApiLogRecordHandler._MAX_BATCH_SIZE * 3 + 47
}


def _create_test(name, num_logs):
  setattr(FnApiLogRecordHandlerTest, 'test_%s' % name,
          lambda self: self._verify_fn_log_handler(num_logs))


if __name__ == '__main__':
  for test_name, num_logs_entries in data.iteritems():
    _create_test(test_name, num_logs_entries)

  unittest.main()
