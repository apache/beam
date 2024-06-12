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

"""A runner for executing portable pipelines on Apache Beam Prism."""

# pytype: skip-file

import logging
import os
import re
import urllib

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version


MAGIC_HOST_NAMES = ['[local]', '[auto]']

_LOGGER = logging.getLogger(__name__)


class PrismRunner(portable_runner.PortableRunner):
  """A runner for launching jobs on Prism, automatically downloading and starting a
  Prism instance if needed.
  """

  # Inherits run_portable_pipeline from PortableRunner.

  def default_environment(self, options):
    portable_options = options.view_as(pipeline_options.PortableOptions)
    prism_options = options.view_as(pipeline_options.PrismRunnerOptions)
    if (not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
      return job_server.StopOnExitJobServer(PrismJobServer(options))

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service,
        options,
        retain_unknown_options=True)


class PrismJobServer(job_server.SubprocessJobServer):
  def __init__(self, options):
    super().__init__(options)
    options = options.view_as(pipeline_options.PrismRunnerOptions)

  def path_to_jar(self):
    if self._jar:
      if not os.path.exists(self._jar):
        url = urllib.parse.urlparse(self._jar)
        if not url.scheme:
          raise ValueError(
              'Unable to parse jar URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file path, make sure '
              'the file exists; you may have to first build the job server '
              'using `./gradlew runners:flink:%s:job-server:shadowJar`.' %
              (self._jar, self._flink_version))
      return self._jar
    else:
      return self.path_to_beam_jar(
          ':runners:flink:%s:job-server:shadowJar' % self._flink_version)

  def subprocess_cmd_and_endpoint(self):
    jar_path = self.local_jar(self.path_to_jar())
    artifacts_dir = (
        self._artifacts_dir if self._artifacts_dir else self.local_temp_dir(
            prefix='artifacts'))
    job_port, = subprocess_server.pick_port(self._job_port)
    subprocess_cmd = [self._java_launcher, '-jar'] + self._jvm_properties + [
        jar_path
    ] + list(
        self.prism_arguments(
            job_port, self._artifact_port, self._expansion_port, artifacts_dir))
    return (subprocess_cmd, 'localhost:%s' % job_port)
  
  def prism_arguments(
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
