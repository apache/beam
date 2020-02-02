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

import contextlib
import logging
import os
import sys
import tempfile
import unittest
import zipfile

import grpc
import requests_mock

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import flink_uber_jar_job_server


@contextlib.contextmanager
def temp_name(*args, **kwargs):
  with tempfile.NamedTemporaryFile(*args, **kwargs) as t:
    name = t.name
  yield name
  if os.path.exists(name):
    os.unlink(name)


@unittest.skipIf(sys.version_info < (3, 6), "Requires Python 3.6+")
class FlinkUberJarJobServerTest(unittest.TestCase):

  @requests_mock.mock()
  def test_flink_version(self, http_mock):
    http_mock.get('http://flink/v1/config', json={'flink-version': '3.1.4.1'})
    job_server = flink_uber_jar_job_server.FlinkUberJarJobServer(
        'http://flink', pipeline_options.FlinkRunnerOptions())
    self.assertEqual(job_server.flink_version(), "3.1")

  @requests_mock.mock()
  def test_end_to_end(self, http_mock):
    with temp_name(suffix='fake.jar') as fake_jar:
      # Create the jar file with some trivial contents.
      with zipfile.ZipFile(fake_jar, 'w') as zip:
        with zip.open('FakeClass.class', 'w') as fout:
          fout.write(b'[original_contents]')

      options = pipeline_options.FlinkRunnerOptions()
      options.flink_job_server_jar = fake_jar
      job_server = flink_uber_jar_job_server.FlinkUberJarJobServer(
          'http://flink', options)

      # Prepare the job.
      prepare_response = job_server.Prepare(
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

      # Now actually run the job.
      http_mock.post(
          'http://flink/v1/jars/upload',
          json={'filename': '/path/to/jar/nonce'})
      http_mock.post(
          'http://flink/v1/jars/nonce/run',
          json={'jobid': 'some_job_id'})
      job_server.Run(
          beam_job_api_pb2.RunJobRequest(
              preparation_id=prepare_response.preparation_id,
              retrieval_token=retrieval_token))

      # Check the status until the job is "done" and get all error messages.
      http_mock.get(
          'http://flink/v1/jobs/some_job_id/execution-result',
          [{'json': {'status': {'id': 'IN_PROGRESS'}}},
           {'json': {'status': {'id': 'IN_PROGRESS'}}},
           {'json': {'status': {'id': 'COMPLETED'}}}])
      http_mock.get(
          'http://flink/v1/jobs/some_job_id',
          json={'state': 'FINISHED'})
      http_mock.delete(
          'http://flink/v1/jars/nonce')

      state_stream = job_server.GetStateStream(
          beam_job_api_pb2.GetJobStateRequest(
              job_id=prepare_response.preparation_id))

      self.assertEqual(
          [s.state for s in state_stream],
          [beam_job_api_pb2.JobState.STOPPED,
           beam_job_api_pb2.JobState.RUNNING,
           beam_job_api_pb2.JobState.DONE])

      http_mock.get(
          'http://flink/v1/jobs/some_job_id/exceptions',
          json={'all-exceptions': [{'exception': 'exc_text', 'timestamp': 0}]})
      message_stream = job_server.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(
              job_id=prepare_response.preparation_id))

      def get_item(x):
        if x.HasField('message_response'):
          return x.message_response
        else:
          return x.state_response.state

      self.assertEqual(
          [get_item(m) for m in message_stream],
          [
              beam_job_api_pb2.JobState.STOPPED,
              beam_job_api_pb2.JobState.RUNNING,
              beam_job_api_pb2.JobMessage(
                  message_id='message0',
                  time='0',
                  importance=beam_job_api_pb2.JobMessage.MessageImportance
                  .JOB_MESSAGE_ERROR,
                  message_text='exc_text'),
              beam_job_api_pb2.JobState.DONE,
          ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
