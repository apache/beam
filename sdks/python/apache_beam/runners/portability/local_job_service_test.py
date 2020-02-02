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

import itertools
import logging
import unittest

import grpc

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import local_job_service


def increment_iter(iter):
  return itertools.chain([next(iter)], iter)


class LocalJobServerTest(unittest.TestCase):

  def test_end_to_end(self):

    job_service = local_job_service.LocalJobServicer()
    job_service.start_grpc_server()

    # this logic is taken roughly from PortableRunner.run_pipeline()

    # Prepare the job.
    prepare_response = job_service.Prepare(
        beam_job_api_pb2.PrepareJobRequest(
            job_name='job',
            pipeline=beam_runner_api_pb2.Pipeline()))
    channel = grpc.insecure_channel(
        prepare_response.artifact_staging_endpoint.url)
    retrieval_token = beam_artifact_api_pb2_grpc.ArtifactStagingServiceStub(
        channel).CommitManifest(
            beam_artifact_api_pb2.CommitManifestRequest(
                staging_session_token=prepare_response.staging_session_token,
                manifest=beam_artifact_api_pb2.Manifest())
        ).retrieval_token
    channel.close()

    state_stream = job_service.GetStateStream(
        beam_job_api_pb2.GetJobStateRequest(
            job_id=prepare_response.preparation_id))
    # If there's an error, we don't always get it until we try to read.
    # Fortunately, there's always an immediate current state published.
    # state_results.append(next(state_stream))
    state_stream = increment_iter(state_stream)

    message_stream = job_service.GetMessageStream(
        beam_job_api_pb2.JobMessagesRequest(
            job_id=prepare_response.preparation_id))

    job_service.Run(
        beam_job_api_pb2.RunJobRequest(
            preparation_id=prepare_response.preparation_id,
            retrieval_token=retrieval_token))

    state_results = list(state_stream)
    message_results = list(message_stream)

    expected_states = [
        beam_job_api_pb2.JobState.STOPPED,
        beam_job_api_pb2.JobState.STARTING,
        beam_job_api_pb2.JobState.RUNNING,
        beam_job_api_pb2.JobState.DONE,
    ]
    self.assertEqual(
        [s.state for s in state_results],
        expected_states)

    self.assertEqual(
        [s.state_response.state for s in message_results],
        expected_states)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
