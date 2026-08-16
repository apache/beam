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

  def test_path_to_jar_defaults_to_the_job_server_module(self):
    job_server = KafkaStreamsJarJobServer(pipeline_options.PipelineOptions([]))
    # Without an explicit jar the runner resolves the one built by the job
    # server module, which is what lets a user run a pipeline without having
    # built or started anything first. Resolving it for real would either
    # download or demand a built jar, so only the target is checked here.
    with mock.patch.object(job_server, 'path_to_beam_jar') as path_to_beam_jar:
      job_server.path_to_jar()
    path_to_beam_jar.assert_called_once_with(
        ':runners:kafka-streams:job-server:shadowJar')

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
