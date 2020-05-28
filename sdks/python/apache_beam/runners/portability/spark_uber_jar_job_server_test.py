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

import freezegun
import requests_mock

from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SparkRunnerOptions
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import spark_runner
from apache_beam.runners.portability import spark_uber_jar_job_server
from apache_beam.runners.portability.local_job_service_test import TestJobServicePlan


@contextlib.contextmanager
def temp_name(*args, **kwargs):
  with tempfile.NamedTemporaryFile(*args, **kwargs) as t:
    name = t.name
  yield name
  if os.path.exists(name):
    os.unlink(name)


def spark_job():
  return spark_uber_jar_job_server.SparkBeamJob(
      'http://host:6066',
      '',
      '',
      '',
      '',
      '',
      pipeline_options.SparkRunnerOptions())


@unittest.skipIf(sys.version_info < (3, 6), "Requires Python 3.6+")
class SparkUberJarJobServerTest(unittest.TestCase):
  @requests_mock.mock()
  def test_get_server_spark_version(self, http_mock):
    http_mock.get(
        'http://host:6066',
        json={
            "action": "ErrorResponse",
            "message": "Missing protocol version. Please submit requests "
            "through http://[host]:[port]/v1/submissions/...",
            "serverSparkVersion": "1.2.3"
        },
        status_code=400)
    self.assertEqual(spark_job()._get_server_spark_version(), "1.2.3")

  def test_get_client_spark_version_from_properties(self):
    with temp_name(suffix='fake.jar') as fake_jar:
      with zipfile.ZipFile(fake_jar, 'w') as zip:
        with zip.open('spark-version-info.properties', 'w') as fout:
          fout.write(b'version=4.5.6')
      self.assertEqual(
          spark_job()._get_client_spark_version_from_properties(fake_jar),
          "4.5.6")

  def test_get_client_spark_version_from_properties_no_properties_file(self):
    with self.assertRaises(KeyError):
      with temp_name(suffix='fake.jar') as fake_jar:
        with zipfile.ZipFile(fake_jar, 'w') as zip:
          # Write some other file to the jar.
          with zip.open('FakeClass.class', 'w') as fout:
            fout.write(b'[original_contents]')
        spark_job()._get_client_spark_version_from_properties(fake_jar)

  def test_get_client_spark_version_from_properties_missing_version(self):
    with self.assertRaises(ValueError):
      with temp_name(suffix='fake.jar') as fake_jar:
        with zipfile.ZipFile(fake_jar, 'w') as zip:
          with zip.open('spark-version-info.properties', 'w') as fout:
            fout.write(b'version=')
        spark_job()._get_client_spark_version_from_properties(fake_jar)

  @requests_mock.mock()
  @freezegun.freeze_time("1970-01-01")
  def test_end_to_end(self, http_mock):
    submission_id = "submission-id"
    worker_host_port = "workerhost:12345"
    worker_id = "worker-id"
    server_spark_version = "1.2.3"

    def spark_submission_status_response(state):
      return {
          'json': {
              "action": "SubmissionStatusResponse",
              "driverState": state,
              "serverSparkVersion": server_spark_version,
              "submissionId": submission_id,
              "success": "true",
              "workerHostPort": worker_host_port,
              "workerId": worker_id
          }
      }

    with temp_name(suffix='fake.jar') as fake_jar:
      with zipfile.ZipFile(fake_jar, 'w') as zip:
        with zip.open('spark-version-info.properties', 'w') as fout:
          fout.write(b'version=4.5.6')

      options = pipeline_options.SparkRunnerOptions()
      options.spark_job_server_jar = fake_jar
      job_server = spark_uber_jar_job_server.SparkUberJarJobServer(
          'http://host:6066', options)

      # Prepare the job.
      plan = TestJobServicePlan(job_server)

      # Prepare the job.
      prepare_response = plan.prepare(beam_runner_api_pb2.Pipeline())
      retrieval_token = plan.stage(
          beam_runner_api_pb2.Pipeline(),
          prepare_response.artifact_staging_endpoint.url,
          prepare_response.staging_session_token)

      # Now actually run the job.
      http_mock.post(
          'http://host:6066/v1/submissions/create',
          json={
              "action": "CreateSubmissionResponse",
              "message": "Driver successfully submitted as submission-id",
              "serverSparkVersion": "1.2.3",
              "submissionId": "submission-id",
              "success": "true"
          })
      job_server.Run(
          beam_job_api_pb2.RunJobRequest(
              preparation_id=prepare_response.preparation_id,
              retrieval_token=retrieval_token))

      # Check the status until the job is "done" and get all error messages.
      http_mock.get(
          'http://host:6066/v1/submissions/status/submission-id',
          [
              spark_submission_status_response('RUNNING'),
              spark_submission_status_response('RUNNING'),
              {
                  'json': {
                      "action": "SubmissionStatusResponse",
                      "driverState": "ERROR",
                      "message": "oops",
                      "serverSparkVersion": "1.2.3",
                      "submissionId": submission_id,
                      "success": "true",
                      "workerHostPort": worker_host_port,
                      "workerId": worker_id
                  }
              }
          ])

      state_stream = job_server.GetStateStream(
          beam_job_api_pb2.GetJobStateRequest(
              job_id=prepare_response.preparation_id))

      self.assertEqual([s.state for s in state_stream],
                       [
                           beam_job_api_pb2.JobState.STOPPED,
                           beam_job_api_pb2.JobState.RUNNING,
                           beam_job_api_pb2.JobState.RUNNING,
                           beam_job_api_pb2.JobState.FAILED
                       ])

      message_stream = job_server.GetMessageStream(
          beam_job_api_pb2.JobMessagesRequest(
              job_id=prepare_response.preparation_id))

      def get_item(x):
        if x.HasField('message_response'):
          return x.message_response
        else:
          return x.state_response.state

      self.assertEqual([get_item(m) for m in message_stream],
                       [
                           beam_job_api_pb2.JobState.STOPPED,
                           beam_job_api_pb2.JobState.RUNNING,
                           beam_job_api_pb2.JobMessage(
                               message_id='message0',
                               time='0',
                               importance=beam_job_api_pb2.JobMessage.
                               MessageImportance.JOB_MESSAGE_ERROR,
                               message_text="oops"),
                           beam_job_api_pb2.JobState.FAILED,
                       ])

  def test_retain_unknown_options(self):
    original_options = PipelineOptions(['--unknown_option_foo', 'some_value'])
    spark_options = original_options.view_as(SparkRunnerOptions)
    spark_options.spark_submit_uber_jar = True
    spark_options.spark_rest_url = 'spark://localhost:6066'
    runner = spark_runner.SparkRunner()

    job_service_handle = runner.create_job_service(original_options)
    options_proto = job_service_handle.get_pipeline_options()

    self.assertEqual(
        options_proto['beam:option:unknown_option_foo:v1'], 'some_value')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
