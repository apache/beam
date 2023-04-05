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
import unittest

import mock

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import local_job_service
from apache_beam.runners.portability.portable_runner import JobServiceHandle
from apache_beam.runners.portability.portable_runner import PortableRunner


class TestJobServicePlan(JobServiceHandle):
  def __init__(self, job_service):
    self.job_service = job_service
    self.options = None
    self.timeout = None
    self.artifact_endpoint = None

  def get_pipeline_options(self):
    return None


class LocalJobServerTest(unittest.TestCase):
  def test_end_to_end(self):

    job_service = local_job_service.LocalJobServicer()
    job_service.start_grpc_server()

    plan = TestJobServicePlan(job_service)

    _, message_stream, state_stream = plan.submit(
        beam_runner_api_pb2.Pipeline())

    state_results = list(state_stream)
    message_results = list(message_stream)

    expected_states = [
        beam_job_api_pb2.JobState.STOPPED,
        beam_job_api_pb2.JobState.STARTING,
        beam_job_api_pb2.JobState.RUNNING,
        beam_job_api_pb2.JobState.DONE,
    ]
    self.assertEqual([s.state for s in state_results], expected_states)

    self.assertEqual([
        s.state_response.state
        for s in message_results if s.HasField('state_response')
    ],
                     expected_states)

  def test_error_messages_after_pipeline_failure(self):
    job_service = local_job_service.LocalJobServicer()
    job_service.start_grpc_server()

    plan = TestJobServicePlan(job_service)

    job_id, message_stream, state_stream = plan.submit(
        beam_runner_api_pb2.Pipeline(requirements=['unsupported_requirement']))

    message_results = list(message_stream)
    state_results = list(state_stream)

    expected_states = [
        beam_job_api_pb2.JobState.STOPPED,
        beam_job_api_pb2.JobState.STARTING,
        beam_job_api_pb2.JobState.RUNNING,
        beam_job_api_pb2.JobState.FAILED,
    ]
    self.assertEqual([s.state for s in state_results], expected_states)
    self.assertTrue(
        any(
            'unsupported_requirement' in m.message_response.message_text
            for m in message_results),
        message_results)

    # Assert we still see the error message if we fetch error messages after
    # the job has completed.
    messages_again = list(
        plan.job_service.GetMessageStream(
            beam_job_api_pb2.JobMessagesRequest(job_id=job_id)))
    self.assertTrue(
        any(
            'unsupported_requirement' in m.message_response.message_text
            for m in message_results),
        messages_again)

  def test_artifact_service_override(self):
    job_service = local_job_service.LocalJobServicer()
    port = job_service.start_grpc_server()

    test_artifact_endpoint = 'testartifactendpoint:4242'

    options = pipeline_options.PipelineOptions([
        '--job_endpoint',
        'localhost:%d' % port,
        '--artifact_endpoint',
        test_artifact_endpoint,
    ])
    runner = PortableRunner()
    job_service_handle = runner.create_job_service(options)

    with mock.patch.object(job_service_handle, 'stage') as mocked_stage:
      job_service_handle.submit(beam_runner_api_pb2.Pipeline())
      mocked_stage.assert_called_once_with(
          mock.ANY, test_artifact_endpoint, mock.ANY)

    # Confirm the artifact_endpoint is in the options protobuf
    options_proto = job_service_handle.get_pipeline_options()
    self.assertEqual(
        options_proto['beam:option:artifact_endpoint:v1'],
        test_artifact_endpoint)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
