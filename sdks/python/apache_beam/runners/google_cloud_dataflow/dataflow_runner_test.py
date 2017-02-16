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

"""Unit tests for the DataflowRunner class."""

import unittest

import mock

from apache_beam.runners.google_cloud_dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.google_cloud_dataflow.dataflow_runner import DataflowRuntimeException
from apache_beam.runners.google_cloud_dataflow.internal.clients import dataflow as dataflow_api


class DataflowRunnerTest(unittest.TestCase):

  def test_dataflow_runner_has_metrics(self):
    df_result = DataflowPipelineResult('somejob', 'somerunner')
    self.assertTrue(df_result.metrics())
    self.assertTrue(df_result.metrics().query())

  @mock.patch('time.sleep', return_value=None)
  def test_wait_until_finish(self, patched_time_sleep):
    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    class MockDataflowRunner(object):

      def __init__(self, final_state):
        self.dataflow_client = mock.MagicMock()
        self.job = mock.MagicMock()
        self.job.currentState = values_enum.JOB_STATE_UNKNOWN

        def get_job_side_effect(*args, **kwargs):
          self.job.currentState = final_state
          return mock.DEFAULT

        self.dataflow_client.get_job = mock.MagicMock(
            return_value=self.job, side_effect=get_job_side_effect)
        self.dataflow_client.list_messages = mock.MagicMock(
            return_value=([], None))

    with self.assertRaisesRegexp(
        DataflowRuntimeException, 'Dataflow pipeline failed. State: FAILED'):
      failed_runner = MockDataflowRunner(values_enum.JOB_STATE_FAILED)
      failed_result = DataflowPipelineResult(failed_runner.job, failed_runner)
      failed_result.wait_until_finish()

    succeeded_runner = MockDataflowRunner(values_enum.JOB_STATE_DONE)
    succeeded_result = DataflowPipelineResult(
        succeeded_runner.job, succeeded_runner)
    succeeded_result.wait_until_finish()


if __name__ == '__main__':
  unittest.main()
