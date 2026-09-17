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
import tempfile
import unittest

import mock

from apache_beam.options import pipeline_options
from apache_beam.runners.portability.kafka_streams_runner import KafkaStreamsJarJobServer
from apache_beam.utils import subprocess_server
from apache_beam.runners.portability.kafka_streams_runner import KafkaStreamsRunner


class KafkaStreamsTestPipelineOptions(pipeline_options.PipelineOptions):
  def view_as(self, cls):
    # Ensure only KafkaStreamsRunnerOptions and JobServerOptions are used when
    # calling default_job_server. If other options classes are needed, the
    # cache key must include them to prevent incorrect hits.
    assert (
        cls is pipeline_options.KafkaStreamsRunnerOptions or
        cls is pipeline_options.JobServerOptions)
    return super().view_as(cls)


class KafkaStreamsJavaJobServerTest(unittest.TestCase):
  def test_job_server_cache(self):
    # Multiple KafkaStreamsRunner instances may be created, so job servers have
    # to be cached across runner instances: each one is an external Java
    # process, and starting a second for the same configuration would fail to
    # bind the same ports.

    # Options that do not affect job server configuration, such as
    # sdk_worker_parallelism, should still hit the same cache entry.
    job_server1 = KafkaStreamsRunner().default_job_server(
        KafkaStreamsTestPipelineOptions(['--sdk_worker_parallelism=1']))
    job_server2 = KafkaStreamsRunner().default_job_server(
        KafkaStreamsTestPipelineOptions(['--sdk_worker_parallelism=2']))
    self.assertIs(job_server2, job_server1)

    # JobServerOptions do affect it, so a different port is a different server.
    job_server3 = KafkaStreamsRunner().default_job_server(
        KafkaStreamsTestPipelineOptions(['--job_port=1234']))
    self.assertIsNot(job_server3, job_server1)

    # So do the runner's own options.
    job_server4 = KafkaStreamsRunner().default_job_server(
        KafkaStreamsTestPipelineOptions(['--bootstrap_servers=other:9092']))
    self.assertIsNot(job_server4, job_server1)
    self.assertIsNot(job_server4, job_server3)

    job_server5 = KafkaStreamsRunner().default_job_server(
        KafkaStreamsTestPipelineOptions(['--application_id=other-pipeline']))
    self.assertIsNot(job_server5, job_server1)
    self.assertIsNot(job_server5, job_server4)

  def test_java_arguments(self):
    # These are what the job server driver is launched with, so they have to be
    # options it accepts.
    job_server = KafkaStreamsJarJobServer(
        pipeline_options.PipelineOptions(['--application_id=test-pipeline']))
    self.assertEqual([
        '--artifacts-dir',
        '/tmp/artifacts',
        '--job-port',
        8099,
        '--artifact-port',
        8098,
        '--expansion-port',
        8097
    ],
                     job_server.java_arguments(
                         8099, 8098, 8097, '/tmp/artifacts'))

  def test_path_to_jar_uses_a_jar_built_from_this_source_tree(self):
    job_server = KafkaStreamsJarJobServer(pipeline_options.PipelineOptions([]))
    # Without an explicit jar the runner uses the one the job server module
    # builds, which is what lets someone working in a Beam checkout run a
    # pipeline without passing anything.
    with tempfile.NamedTemporaryFile(suffix='.jar') as jar:
      with mock.patch.object(subprocess_server.JavaJarServer,
                             'path_to_dev_beam_jar',
                             return_value=jar.name) as path_to_dev_beam_jar:
        self.assertEqual(jar.name, job_server.path_to_jar())
      path_to_dev_beam_jar.assert_called_once_with(
          ':runners:kafka-streams:job-server:shadowJar')

  def test_path_to_jar_explains_itself_when_nothing_is_built(self):
    job_server = KafkaStreamsJarJobServer(pipeline_options.PipelineOptions([]))
    # The job server is not published with any Beam release, so falling back to
    # a download would fetch an artifact that does not exist and fail with a
    # 404. Say what is actually wrong instead, and how to build one.
    with mock.patch.object(subprocess_server.JavaJarServer,
                           'path_to_dev_beam_jar',
                           return_value='/no/such/built.jar'):
      with self.assertRaises(RuntimeError) as context:
        job_server.path_to_jar()
    message = str(context.exception)
    self.assertIn('not part of any Apache Beam release', message)
    self.assertIn('-Pwith-kafka-streams-runner', message)

  def test_path_to_jar_uses_an_explicit_jar(self):
    with tempfile.NamedTemporaryFile(suffix='.jar') as jar:
      job_server = KafkaStreamsJarJobServer(
          pipeline_options.PipelineOptions(
              ['--kafka_streams_job_server_jar=%s' % jar.name]))
      self.assertEqual(jar.name, job_server.path_to_jar())

  def test_path_to_jar_rejects_an_unusable_path(self):
    job_server = KafkaStreamsJarJobServer(
        pipeline_options.PipelineOptions(
            ['--kafka_streams_job_server_jar=/no/such/jar.jar']))
    # A path that is neither an existing file nor a URL cannot be recovered
    # from, so it fails with the command that would produce a jar rather than
    # letting the job server fail to start later.
    with self.assertRaises(ValueError) as context:
      job_server.path_to_jar()
    self.assertIn('job-server:shadowJar', str(context.exception))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
