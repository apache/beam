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

"""Unit tests for BatchWorker."""

import logging
import threading
import time
import unittest

import mock
from mock import patch
from google.cloud.dataflow.internal import apiclient
from google.cloud.dataflow.worker import batchworker
from google.cloud.dataflow.worker import executor
from google.cloud.dataflow.worker import workitem


class BatchWorkerTest(unittest.TestCase):

  def dummy_properties(self):
    return {
        'project_id': 'test_project',
        'job_id': 'test_job',
        'worker_id': 'test_worker',
        'service_path': 'test_services_path',
        'root_url': 'test_root_url',
        'reporting_enabled': 'test_reporting_enabled',
        'temp_gcs_directory': 'test_temp_gcs_directory'
    }

  @patch('google.cloud.dataflow.worker.batchworker.workitem')
  @patch.object(apiclient.DataflowWorkerClient, 'lease_work')
  def test_worker_requests_for_work(self, mock_lease_work, mock_workitem):
    worker = batchworker.BatchWorker(self.dummy_properties(), {})
    rand_work = object()
    mock_lease_work.return_value = rand_work
    mock_workitem.get_work_items.return_value = None
    thread = threading.Thread(target=worker.run)
    thread.start()
    time.sleep(5)
    worker.shutdown()

    mock_lease_work.assert_called_with(
        mock.ANY, worker.default_desired_lease_duration())
    mock_workitem.get_work_items.assert_called_with(
        rand_work, mock.ANY, mock.ANY)

  @patch('google.cloud.dataflow.worker.batchworker.workitem')
  @patch.object(apiclient.DataflowWorkerClient, 'lease_work')
  def test_worker_requests_for_work_after_lease_error(
      self, mock_lease_work, mock_workitem):
    worker = batchworker.BatchWorker(self.dummy_properties(), {})

    rand_work = object()
    mock_lease_work.side_effect = [Exception('test exception'), rand_work]
    mock_workitem.get_work_items.return_value = None
    thread = threading.Thread(target=worker.run)
    thread.start()
    time.sleep(5)
    worker.shutdown()

    mock_lease_work.assert_called_with(
        mock.ANY, worker.default_desired_lease_duration())
    mock_workitem.get_work_items.assert_called_with(
        mock.ANY, mock.ANY, mock.ANY)

  @patch.object(executor.MapTaskExecutor, 'execute')
  @patch.object(batchworker.ProgressReporter, 'start_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'stop_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'report_status')
  def test_worker_starts_and_stops_progress_reporter(
      self, mock_report_status, mock_stop, mock_start, mock_execute):
    worker = batchworker.BatchWorker(self.dummy_properties(), {})
    mock_work_item = mock.MagicMock()
    worker.do_work(mock_work_item)

    mock_report_status.assert_called_with(
        completed=True, exception_details=None)
    mock_start.assert_called_once_with()
    mock_execute.assert_called_once_with(mock.ANY)
    mock_stop.assert_called_once_with()

  @patch.object(executor.MapTaskExecutor, 'execute')
  @patch.object(batchworker.ProgressReporter, 'start_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'stop_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'report_status')
  def test_worker_fails_for_deferred_exceptions(
      self, mock_report_status, mock_stop, mock_start, mock_execute):
    worker = batchworker.BatchWorker(self.dummy_properties(), {})
    mock_work_item = mock.MagicMock()
    worker.do_work(mock_work_item, deferred_exception_details='deferred_exc')

    mock_report_status.assert_called_with(
        completed=True, exception_details='deferred_exc')
    assert not mock_stop.called
    assert not mock_start.called
    assert not mock_execute.called

  def _run_send_completion_test(self, mock_report_status, mock_stop, mock_start,
                                mock_execute, expected_exception):
    worker = batchworker.BatchWorker(self.dummy_properties(), {})
    mock_work_item = mock.MagicMock()
    worker.do_work(mock_work_item)

    class AnyStringWith(str):

      def __eq__(self, other):
        return self in other

    mock_report_status.assert_called_with(
        completed=True,
        exception_details=AnyStringWith(expected_exception))

    mock_start.assert_called_once_with()
    mock_execute.assert_called_once_with(mock.ANY)
    mock_stop.assert_called_once_with()

  @patch.object(executor.MapTaskExecutor, 'execute')
  @patch.object(batchworker.ProgressReporter, 'start_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'stop_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'report_status')
  def test_send_completion_execute_failure(self, mock_report_status, mock_stop,
                                           mock_start, mock_execute):
    mock_execute.side_effect = Exception('test_exception')
    self._run_send_completion_test(mock_report_status, mock_stop, mock_start,
                                   mock_execute, 'test_exception')

  @patch.object(executor.MapTaskExecutor, 'execute')
  @patch.object(batchworker.ProgressReporter, 'start_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'stop_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'report_status')
  def test_send_completion_stop_progress_reporter_failure(self,
                                                          mock_report_status,
                                                          mock_stop, mock_start,
                                                          mock_execute):
    mock_stop.side_effect = Exception('test_exception')
    self._run_send_completion_test(mock_report_status, mock_stop, mock_start,
                                   mock_execute, 'test_exception')

  @patch.object(executor.MapTaskExecutor, 'execute')
  @patch.object(batchworker.ProgressReporter, 'start_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'stop_reporting_progress')
  @patch.object(batchworker.ProgressReporter, 'report_status')
  def test_send_completion_execute_and_stop_progress_reporter_failure(
      self, mock_report_status, mock_stop, mock_start, mock_execute):
    mock_execute.side_effect = Exception('test_exception_1')
    mock_stop.side_effect = Exception('test_exception_2')
    self._run_send_completion_test(mock_report_status, mock_stop, mock_start,
                                   mock_execute, 'test_exception_1')


class ProgressReporterTest(unittest.TestCase):

  @patch.object(batchworker.ProgressReporter, 'next_progress_report_interval')
  @patch.object(batchworker.ProgressReporter, 'process_report_status_response')
  def test_progress_reporter_reports_progress(
      self, mock_report_response, mock_next_progress):  # pylint: disable=unused-argument
    work_item = workitem.BatchWorkItem(
        proto=mock.MagicMock(), map_task=mock.MagicMock())
    mock_work_executor = mock.MagicMock()
    mock_batch_worker = mock.MagicMock()
    mock_client = mock.MagicMock()

    mock_next_progress.return_value = 1

    progress_reporter = batchworker.ProgressReporter(
        work_item, mock_work_executor, mock_batch_worker, mock_client)
    progress_reporter.start_reporting_progress()
    time.sleep(10)
    progress_reporter.stop_reporting_progress()
    mock_client.report_status.assert_called_with(
        mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY)

  @patch.object(batchworker.ProgressReporter, 'next_progress_report_interval')
  @patch.object(batchworker.ProgressReporter, 'process_report_status_response')
  def test_progress_reporter_sends_last_update(
      self, mock_report_response, mock_next_progress):  # pylint: disable=unused-argument
    mock_work_item = mock.MagicMock()
    mock_work_executor = mock.MagicMock()
    mock_batch_worker = mock.MagicMock()
    mock_client = mock.MagicMock()
    progress_reporter = batchworker.ProgressReporter(
        mock_work_item, mock_work_executor, mock_batch_worker, mock_client)
    mock_split_result = mock.MagicMock()
    progress_reporter.dynamic_split_result_to_report = mock_split_result

    progress_reporter._stopped = True
    progress_reporter.stop_reporting_progress()
    mock_client.report_status.assert_called_with(
        mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock_split_result,
        mock.ANY)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
