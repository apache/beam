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

import requests_mock

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import flink_runner
from apache_beam.runners.portability import flink_uber_jar_job_server
from apache_beam.runners.portability.local_job_service_test import TestJobServicePlan


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
  def test_get_job_metrics(self, http_mock):
    response = {
        "user-task-accumulators": [{
            "name": "__metricscontainers",
            "type": "MetricsAccumulator",
            "value": "{\"metrics\": {\"attempted\": [{\"urn\": "
            "\"metric_urn\", \"type\": \"beam:metrics:sum_int64:v1\", "
            "\"payload\": \"AA==\", \"labels\": "
            "{\"PTRANSFORM\": \"ptransform_id\"}}]}}"
        }]
    }
    http_mock.get(
        'http://flink/v1/jobs/flink_job_id/accumulators', json=response)
    options = pipeline_options.FlinkRunnerOptions()
    job_server = flink_uber_jar_job_server.FlinkUberJarJobServer(
        'http://flink', options)
    job = flink_uber_jar_job_server.FlinkBeamJob(
        'http://flink', None, 'job_id', 'job_name', None, options)
    job._flink_job_id = 'flink_job_id'
    job_server._jobs['job_id'] = job
    request = beam_job_api_pb2.GetJobMetricsRequest(job_id='job_id')
    expected = beam_job_api_pb2.GetJobMetricsResponse(
        metrics=beam_job_api_pb2.MetricResults(
            attempted=[{
                "urn": "metric_urn",
                "type": "beam:metrics:sum_int64:v1",
                "payload": b'\000',
                "labels": {
                    "PTRANSFORM": "ptransform_id"
                }
            }]))

    actual = job_server.GetJobMetrics(request)
    self.assertEqual(actual, expected)

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

      plan = TestJobServicePlan(job_server)

      # Prepare the job.
      prepare_response = plan.prepare(beam_runner_api_pb2.Pipeline())
      plan.stage(
          beam_runner_api_pb2.Pipeline(),
          prepare_response.artifact_staging_endpoint.url,
          prepare_response.staging_session_token)

      # Now actually run the job.
      http_mock.post(
          'http://flink/v1/jars/upload',
          json={'filename': '/path/to/jar/nonce'})
      http_mock.post(
          'http://flink/v1/jars/nonce/run', json={'jobid': 'some_job_id'})

      _, message_stream, state_stream = plan.run(
          prepare_response.preparation_id)

      # Check the status until the job is "done" and get all error messages.
      http_mock.get(
          'http://flink/v1/jobs/some_job_id/execution-result',
          [{
              'json': {
                  'status': {
                      'id': 'IN_PROGRESS'
                  }
              }
          }, {
              'json': {
                  'status': {
                      'id': 'IN_PROGRESS'
                  }
              }
          }, {
              'json': {
                  'status': {
                      'id': 'COMPLETED'
                  }
              }
          }])
      http_mock.get(
          'http://flink/v1/jobs/some_job_id', json={'state': 'FINISHED'})
      http_mock.delete('http://flink/v1/jars/nonce')

      self.assertEqual([s.state for s in state_stream],
                       [
                           beam_job_api_pb2.JobState.STOPPED,
                           beam_job_api_pb2.JobState.RUNNING,
                           beam_job_api_pb2.JobState.RUNNING,
                           beam_job_api_pb2.JobState.DONE
                       ])

      http_mock.get(
          'http://flink/v1/jobs/some_job_id/exceptions',
          json={'all-exceptions': [{
              'exception': 'exc_text', 'timestamp': 0
          }]})

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
                               message_text='exc_text'),
                           beam_job_api_pb2.JobState.DONE,
                       ])

  def test_retain_unknown_options(self):
    original_options = pipeline_options.PipelineOptions(
        ['--unknown_option_foo=some_value'])
    flink_options = original_options.view_as(
        pipeline_options.FlinkRunnerOptions)
    flink_options.flink_submit_uber_jar = True
    flink_options.flink_master = 'http://host:port'
    runner = flink_runner.FlinkRunner()

    job_service_handle = runner.create_job_service(original_options)
    options_proto = job_service_handle.get_pipeline_options()

    self.assertEqual(
        options_proto['beam:option:unknown_option_foo:v1'], 'some_value')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
