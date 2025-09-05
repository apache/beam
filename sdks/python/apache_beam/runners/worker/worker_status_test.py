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
import threading
import time
import unittest

import grpc
import mock

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.worker_status import FnApiWorkerStatusHandler
from apache_beam.runners.worker.worker_status import heap_dump
from apache_beam.utils import thread_pool_executor
from apache_beam.utils.counters import CounterName


class BeamFnStatusServicer(beam_fn_api_pb2_grpc.BeamFnWorkerStatusServicer):
  def __init__(self, num_request):
    self.finished = threading.Condition()
    self.num_request = num_request
    self.response_received = []

  def WorkerStatus(self, response_iterator, context):
    for i in range(self.num_request):
      yield beam_fn_api_pb2.WorkerStatusRequest(id=str(i))
    for response in response_iterator:
      self.finished.acquire()
      self.response_received.append(response)
      if len(self.response_received) == self.num_request:
        self.finished.notifyAll()
      self.finished.release()


class FnApiWorkerStatusHandlerTest(unittest.TestCase):
  def setUp(self):
    self.num_request = 3
    self.test_status_service = BeamFnStatusServicer(self.num_request)
    self.server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_fn_api_pb2_grpc.add_BeamFnWorkerStatusServicer_to_server(
        self.test_status_service, self.server)
    self.test_port = self.server.add_insecure_port('[::]:0')
    self.server.start()
    self.url = 'localhost:%s' % self.test_port
    self.fn_status_handler = FnApiWorkerStatusHandler(
        self.url, element_processing_timeout_minutes=10)

  def tearDown(self):
    self.server.stop(5)

  def test_send_status_response(self):
    self.test_status_service.finished.acquire()
    while len(self.test_status_service.response_received) < self.num_request:
      self.test_status_service.finished.wait(1)
    self.test_status_service.finished.release()
    for response in self.test_status_service.response_received:
      self.assertIsNotNone(response.status_info)
    self.fn_status_handler.close()

  @mock.patch(
      'apache_beam.runners.worker.worker_status'
      '.FnApiWorkerStatusHandler.generate_status_response')
  def test_generate_error(self, mock_method):
    mock_method.side_effect = RuntimeError('error')
    self.test_status_service.finished.acquire()
    while len(self.test_status_service.response_received) < self.num_request:
      self.test_status_service.finished.wait(1)
    self.test_status_service.finished.release()
    for response in self.test_status_service.response_received:
      self.assertIsNotNone(response.error)
    self.fn_status_handler.close()

  def test_log_lull_in_bundle_processor(self):
    def get_state_sampler_info_for_lull(lull_duration_s):
      return "bundle-id", statesampler.StateSamplerInfo(
        CounterName('progress-msecs', 'stage_name', 'step_name'),
        1,
        lull_duration_s * 1e9,
        threading.current_thread())

    now = time.time()
    with mock.patch('logging.Logger.warning') as warn_mock:
      with mock.patch(
          'apache_beam.runners.worker.sdk_worker_main.terminate_sdk_harness'
      ) as flush_mock:
        with mock.patch('time.time') as time_mock:
          time_mock.return_value = now
          bundle_id, sampler_info = get_state_sampler_info_for_lull(21 * 60)
          self.fn_status_handler._log_lull_sampler_info(sampler_info, bundle_id)
          bundle_id_template = warn_mock.call_args[0][1]
          step_name_template = warn_mock.call_args[0][2]
          processing_template = warn_mock.call_args[0][3]
          traceback = warn_mock.call_args = warn_mock.call_args[0][4]

          self.assertIn('bundle-id', bundle_id_template)
          self.assertIn('step_name', step_name_template)
          self.assertEqual(21 * 60, processing_template)
          self.assertIn('test_log_lull_in_bundle_processor', traceback)
          flush_mock.assert_called_once()

        with mock.patch('time.time') as time_mock:
          time_mock.return_value = now + 6 * 60  # 6 minutes
          bundle_id, sampler_info = get_state_sampler_info_for_lull(21 * 60)
          self.fn_status_handler._log_lull_sampler_info(sampler_info, bundle_id)
          self.assertEqual(flush_mock.call_count, 2)

        with mock.patch('time.time') as time_mock:
          time_mock.return_value = now + 21 * 60  # 21 minutes
          bundle_id, sampler_info = get_state_sampler_info_for_lull(10 * 60)
          self.fn_status_handler._log_lull_sampler_info(sampler_info, bundle_id)
          self.assertEqual(flush_mock.call_count, 2)

        with mock.patch('time.time') as time_mock:
          time_mock.return_value = now + 42 * 60  # 42 minutes
          bundle_id, sampler_info = get_state_sampler_info_for_lull(11 * 60)
          self.fn_status_handler._log_lull_sampler_info(sampler_info, bundle_id)
          self.assertEqual(flush_mock.call_count, 3)

  def test_log_lull_creating_processor(self):
    """Test that lull logging works for processors being created."""
    # Mock bundle processor cache with creating_bundle_processors
    mock_cache = mock.Mock()
    mock_cache.active_bundle_processors = {}

    # Create a mock thread
    mock_thread = mock.Mock()
    mock_thread.ident = 12345

    # Set up creating_bundle_processors with a processor that's been creating for 6 minutes
    creation_start_time = time.time() - 6 * 60  # 6 minutes ago
    mock_cache.creating_bundle_processors = {
        'test-instruction-id': (
            'test-bundle-descriptor', creation_start_time, mock_thread)
    }

    with mock.patch('logging.Logger.warning') as warn_mock:
      with mock.patch('sys._current_frames') as frames_mock:
        # Mock the stack frame for the thread
        mock_frame = mock.Mock()
        frames_mock.return_value = {12345: mock_frame}

        with mock.patch('traceback.format_stack') as format_stack_mock:
          format_stack_mock.return_value = [
              '  File "test.py", line 1, in test_function\n    time.sleep(10)\n'
          ]

          # Call the method that checks for lulls directly
          self.fn_status_handler._log_lull_creating_processor(
              'test-instruction-id',
              'test-bundle-descriptor',
              6 * 60 * 1e9,
              mock_thread)

          # Verify warning was called
          warn_mock.assert_called_once()
          call_args = warn_mock.call_args[0]

          # Check the log message format - call_args[0] is the format string
          log_format = call_args[0]
          self.assertIn('Operation ongoing in bundle %s', log_format)
          self.assertIn('during BundleProcessor creation', log_format)
          self.assertIn('bundle_descriptor_id=%s', log_format)

          # Check the arguments - call_args[1] is instruction, call_args[2] is lull_seconds, etc.
          self.assertEqual('test-instruction-id', call_args[1])
          self.assertGreaterEqual(call_args[2], 6 * 60)  # At least 6 minutes
          self.assertEqual('test-bundle-descriptor', call_args[3])
          self.assertIn(
              'test_function', call_args[4])  # Stack trace should be included

  def test_log_lull_creating_processor_no_lull_if_recent(self):
    """Test that no lull is logged for recently started processor creation."""
    mock_thread = mock.Mock()
    mock_thread.ident = 12345

    # Test with a duration less than the timeout (1 minute < 5 minute default timeout)
    creation_duration_ns = 1 * 60 * 1e9  # 1 minute in nanoseconds

    with mock.patch('logging.Logger.warning') as warn_mock:
      with mock.patch.object(self.fn_status_handler,
                             '_passed_lull_timeout_since_last_log',
                             return_value=False):
        # Call _log_lull_creating_processor directly with short duration
        self.fn_status_handler._log_lull_creating_processor(
            'test-instruction-id',
            'test-bundle-descriptor',
            creation_duration_ns,
            mock_thread)

        # Verify no warning was called since _passed_lull_timeout_since_last_log returned False
        warn_mock.assert_not_called()

  def test_log_lull_creating_processor_thread_not_found(self):
    """Test handling when thread is not found in current frames."""
    mock_thread = mock.Mock()
    mock_thread.ident = 99999  # Non-existent thread ID

    creation_duration_ns = 6 * 60 * 1e9  # 6 minutes in nanoseconds

    with mock.patch('logging.Logger.warning') as warn_mock:
      with mock.patch('sys._current_frames') as frames_mock:
        frames_mock.return_value = {}  # Empty frames dict

        self.fn_status_handler._log_lull_creating_processor(
            'test-instruction-id',
            'test-bundle-descriptor',
            creation_duration_ns,
            mock_thread)

        # Verify warning was still called
        warn_mock.assert_called_once()
        call_args = warn_mock.call_args[0]

        # Check that it handled the missing thread gracefully
        self.assertIn('Thread not found or already terminated', call_args[4])

  def test_get_stack_trace_for_thread(self):
    """Test the _get_stack_trace_for_thread method."""
    mock_thread = mock.Mock()
    mock_thread.ident = 12345

    with mock.patch('sys._current_frames') as frames_mock:
      mock_frame = mock.Mock()
      frames_mock.return_value = {12345: mock_frame}

      with mock.patch('traceback.format_stack') as format_stack_mock:
        format_stack_mock.return_value = ['line1\n', 'line2\n']

        result = self.fn_status_handler._get_stack_trace_for_thread(mock_thread)

        self.assertEqual('line1\nline2\n', result)
        format_stack_mock.assert_called_once_with(mock_frame)

  def test_get_stack_trace_for_thread_error(self):
    """Test error handling in _get_stack_trace_for_thread."""
    mock_thread = mock.Mock()
    mock_thread.ident = 12345

    with mock.patch('sys._current_frames') as frames_mock:
      frames_mock.side_effect = Exception('Test error')

      result = self.fn_status_handler._get_stack_trace_for_thread(mock_thread)

      self.assertIn('Error getting stack trace: Test error', result)


class HeapDumpTest(unittest.TestCase):
  @mock.patch('apache_beam.runners.worker.worker_status.hpy', None)
  def test_skip_heap_dump(self):
    result = '%s' % heap_dump()
    self.assertTrue(
        'Unable to import guppy, the heap dump will be skipped' in result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
