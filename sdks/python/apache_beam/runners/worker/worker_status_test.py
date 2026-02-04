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
        self.finished.notify_all()
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
    self.fn_status_handler.close()
    self.server.stop(5)

  def test_send_status_response(self):
    self.test_status_service.finished.acquire()
    while len(self.test_status_service.response_received) < self.num_request:
      self.test_status_service.finished.wait(1)
    self.test_status_service.finished.release()
    for response in self.test_status_service.response_received:
      self.assertIsNotNone(response.status_info)

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

  def test_lull_logs_emitted_when_creating_bundle_processor_takes_time(self):
    instruction_id = "instruction-1"
    bundle_id = "bundle-1"
    thread = threading.current_thread()
    now = time.time()
    creation_time = now

    with (
        mock.patch('logging.Logger.warning') as warn_mock,
        mock.patch('logging.Logger.error') as error_mock,
        mock.patch('time.time') as time_mock,
        mock.patch(
            'apache_beam.runners.worker.sdk_worker_main.terminate_sdk_harness',
        ) as terminate_mock):
      # Set time to be past the lull timeout
      time_mock.return_value = (
          now + self.fn_status_handler.log_lull_timeout_ns / 1e9 + 1)
      self.fn_status_handler._log_lull_in_creating_bundle_descriptor(
          instruction_id, bundle_id, thread, creation_time)
      warn_mock.assert_called_once()
      args, _ = warn_mock.call_args
      self.assertIn(
          'Bundle processor for instruction %s (bundle %s) has been '
          'creating for at least %.2f seconds',
          args[0])

      # Set time to be past the element processing timeout
      time_mock.return_value = (
          now + self.fn_status_handler._element_processing_timeout_ns / 1e9 + 1)

      self.fn_status_handler._log_lull_in_creating_bundle_descriptor(
          instruction_id, bundle_id, thread, creation_time)

      error_mock.assert_called_once()
      args, _ = error_mock.call_args
      self.assertIn(
          'Creation of bundle processor for instruction %s (bundle %s) '
          'has exceeded the specified timeout',
          args[0])

      terminate_mock.assert_called_once()

  def test_lull_logs_emitted_when_processing_a_bundle_takes_time(self):
    instruction_id = "instruction-1"
    now = time.time()
    thread = threading.current_thread()

    with (
        mock.patch('logging.Logger.warning') as warn_mock,
        mock.patch('logging.Logger.error') as error_mock,
        mock.patch('time.time') as time_mock,
        mock.patch(
            'apache_beam.runners.worker.sdk_worker_main.terminate_sdk_harness',
        ) as terminate_mock):
      time_mock.return_value = now + 1
      # Set time to be past the lull timeout
      sampler_info = statesampler.StateSamplerInfo(
          state_name=CounterName('test_counter', 'test_stage', 'test_step'),
          transition_count=1,
          # Set time to be past the lull timeout
          time_since_transition=(
              self.fn_status_handler.log_lull_timeout_ns + 1),
          tracked_thread=thread)
      self.fn_status_handler._log_lull_sampler_info(
          sampler_info, instruction_id)
      warn_mock.assert_called_once()
      args, _ = warn_mock.call_args
      self.assertIn(
          'Operation ongoing in bundle %s%s for at least %.2f seconds', args[0])

      time_mock.return_value = now + 2

      sampler_info = statesampler.StateSamplerInfo(
          state_name=CounterName('test_counter', 'test_stage', 'test_step'),
          transition_count=1,
          # Set time to be past the element processing timeout
          time_since_transition=(
              self.fn_status_handler._element_processing_timeout_ns + 1),
          tracked_thread=thread)
      self.fn_status_handler._log_lull_sampler_info(
          sampler_info, instruction_id)

      error_mock.assert_called_once()
      args, _ = error_mock.call_args
      self.assertIn(
          'Processing of an element in bundle %s%s has exceeded the '
          'specified timeout of %.2f minutes',
          args[0])

      terminate_mock.assert_called_once()


class HeapDumpTest(unittest.TestCase):
  @mock.patch('apache_beam.runners.worker.worker_status.hpy', None)
  def test_skip_heap_dump(self):
    result = '%s' % heap_dump()
    self.assertTrue(
        'Unable to import guppy, the heap dump will be skipped' in result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
