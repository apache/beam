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
from __future__ import print_function

import logging
import unittest

import grpc

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import local_job_service
from apache_beam.runners.portability.portable_runner import JobServiceHandle


class TestJobServicePlan(JobServiceHandle):
  def __init__(self, job_service):
    self.job_service = job_service
    self.options = None
    self.timeout = None

  def get_pipeline_options(self):
    return None

  def stage(self, artifact_staging_endpoint, staging_session_token):
    channel = grpc.insecure_channel(artifact_staging_endpoint)
    staging_stub = beam_artifact_api_pb2_grpc.ArtifactStagingServiceStub(
        channel)
    manifest_response = staging_stub.CommitManifest(
        beam_artifact_api_pb2.CommitManifestRequest(
            staging_session_token=staging_session_token,
            manifest=beam_artifact_api_pb2.Manifest()))
    channel.close()
    return manifest_response.retrieval_token


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

    self.assertEqual([s.state_response.state for s in message_results],
                     expected_states)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
