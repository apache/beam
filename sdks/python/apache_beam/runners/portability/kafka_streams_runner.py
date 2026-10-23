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

"""A runner for executing portable pipelines on Kafka Streams.

The Kafka Streams runner is experimental and is not production ready. It is
not part of any Apache Beam release: its job server is only built when the
Beam build is run with -Pwith-kafka-streams-runner, and no released artifact
contains it. Using it means building the job server from a Beam source tree.

See https://beam.apache.org/documentation/runners/kafkastreams/ for what the
runner supports and what it does not.
"""

# pytype: skip-file

import os
import urllib

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.utils import subprocess_server

# A Java job server is a heavyweight external process, so reuse one across
# pipelines configured the same way.
JOB_SERVER_CACHE = {}


class KafkaStreamsRunner(portable_runner.PortableRunner):
  """A runner for executing pipelines on Kafka Streams.

  Experimental and not production ready; see the module docstring. The job
  server is not published with any Beam release, so it has to be built from
  source or passed with --kafka_streams_job_server_jar.

  Starts a job server automatically, so a pipeline can be submitted without
  running one by hand:

      python my_pipeline.py \\
          --runner=KafkaStreamsRunner \\
          --bootstrap_servers=localhost:9092 \\
          --application_id=my-pipeline

  Pass --job_endpoint instead to submit to a job server that is already
  running.
  """

  # Inherits run_portable_pipeline from PortableRunner.

  def default_environment(self, options):
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (not portable_options.environment_type and
        not portable_options.output_executable_path):
      # The job server runs on this machine, so the SDK harness can too, which
      # saves the user from needing Docker for a local run.
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
    # Only these two option groups affect how the job server is configured, so
    # they are what the cache is keyed on.
    kafka_streams_options = options.view_as(
        pipeline_options.KafkaStreamsRunnerOptions)
    job_server_options = options.view_as(pipeline_options.JobServerOptions)
    options_str = str(kafka_streams_options) + str(job_server_options)
    if options_str not in JOB_SERVER_CACHE:
      JOB_SERVER_CACHE[options_str] = job_server.StopOnExitJobServer(
          KafkaStreamsJarJobServer(options))
    return JOB_SERVER_CACHE[options_str]


class KafkaStreamsJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super().__init__(options)
    kafka_streams_options = options.view_as(
        pipeline_options.KafkaStreamsRunnerOptions)
    self._jar = kafka_streams_options.kafka_streams_job_server_jar

  def path_to_jar(self):
    if self._jar:
      if not os.path.exists(self._jar):
        url = urllib.parse.urlparse(self._jar)
        if not url.scheme:
          raise ValueError(
              'Unable to parse jar URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file path, make sure '
              'the file exists; you may have to first build the job server '
              'using `./gradlew -Pwith-kafka-streams-runner '
              'runners:kafka-streams:job-server:shadowJar`.' % self._jar)
      return self._jar

    # No jar was given, so look for one built from this source tree. The base
    # class would fall back to Maven Central, but the job server is not
    # published for any Beam release, so that download always fails and says
    # nothing useful about why.
    local_jar = subprocess_server.JavaJarServer.path_to_dev_beam_jar(
        ':runners:kafka-streams:job-server:shadowJar')
    if os.path.exists(local_jar):
      return local_jar
    raise RuntimeError(
        'The Kafka Streams runner is experimental and is not part of any '
        'Apache Beam release, so there is no published job server jar to '
        'download. Build one from a Beam source tree with\n'
        '  ./gradlew -Pwith-kafka-streams-runner '
        ':runners:kafka-streams:job-server:shadowJar\n'
        'and pass it as --kafka_streams_job_server_jar, or point that option '
        'at a jar you already have. The runner is opt-in at build time, so '
        'the -Pwith-kafka-streams-runner flag is required; without it the '
        'runner is not part of the build at all.')

  def java_arguments(
      self, job_port, artifact_port, expansion_port, artifacts_dir):
    return [
        '--artifacts-dir',
        artifacts_dir,
        '--job-port',
        job_port,
        '--artifact-port',
        artifact_port,
        '--expansion-port',
        expansion_port
    ]
