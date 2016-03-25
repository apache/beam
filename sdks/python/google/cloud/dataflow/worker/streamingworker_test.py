# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the streaming worker.

These tests check that the streaming worker harness runs properly, with mocked
interactions with Windmill.
"""

import logging
import unittest


import mock

from google.cloud.dataflow.internal import windmill_pb2
from google.cloud.dataflow.worker.streamingworker import StreamingWorker


class StreamingWorkerTest(unittest.TestCase):

  @mock.patch(
      'google.cloud.dataflow.worker.streamingworker.WindmillClient')
  def _get_worker(self, *unused_mocks):
    fake_properties = {
        'project_id': 'fake_project',
        'job_id': 'fake_job',
        'worker_id': 'fake_worker',
        'windmill.host': 'fake_host',
        'windmill.grpc_port': '12345',
    }
    return StreamingWorker(fake_properties)

  def _get_worker_and_single_computation(self):
    worker = self._get_worker()
    computation_work = windmill_pb2.ComputationWorkItems(
        computation_id='A1',
        work=[windmill_pb2.WorkItem(
            key='k',
            work_token=12345)])
    worker.instruction_map['A1'] = mock.Mock()
    return worker, computation_work

  @mock.patch('google.cloud.dataflow.worker.streamingworker.StreamingWorker.'
              'process_work_item')
  def test_successful_work_item(self, *unused_mocks):
    worker, computation_work = self._get_worker_and_single_computation()
    worker.process_computation(computation_work)
    self.assertEqual(0, len(worker.windmill.ReportStats.call_args_list))
    self.assertEqual(1, len(worker.process_work_item.call_args_list))

  @mock.patch('google.cloud.dataflow.worker.streamingworker.StreamingWorker.'
              'process_work_item')
  @mock.patch('logging.error')
  def test_failed_work_item(self, *unused_mocks):
    worker, computation_work = self._get_worker_and_single_computation()
    worker.windmill.ReportStats.return_value = (
        windmill_pb2.ReportStatsResponse(failed=True))
    worker.process_work_item.side_effect = Exception

    worker.process_computation(computation_work)

    # Verify number of attempts and that failed work was reported.
    self.assertEqual(1, len(worker.windmill.ReportStats.call_args_list))
    self.assertEqual(1, len(worker.process_work_item.call_args_list))
    logging.error.assert_called_with(
        'Execution of work in computation %s for key %r failed; Windmill '
        'indicated to not retry locally.', u'A1', 'k')

  @mock.patch('google.cloud.dataflow.worker.streamingworker.StreamingWorker.'
              'process_work_item')
  @mock.patch('logging.error')
  @mock.patch('time.sleep')
  def test_retrying_failed_work_item(self, *unused_mocks):
    worker, computation_work = self._get_worker_and_single_computation()
    retries = 5
    worker.windmill.ReportStats.side_effect = (
        [windmill_pb2.ReportStatsResponse(failed=False)] * retries)
    worker.process_work_item.side_effect = (
        [Exception] * retries + [None])

    worker.process_computation(computation_work)

    # Verify number of attempts and that failed work was reported the correct
    # number of times.
    self.assertEqual(retries, len(worker.windmill.ReportStats.call_args_list))
    self.assertEqual(retries + 1, len(worker.process_work_item.call_args_list))
    logging.error.assert_called_with(
        'Execution of work in computation %s for key %r failed; will retry '
        'locally.', u'A1', 'k')

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
