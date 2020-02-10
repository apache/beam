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

"""A runner for executing portable pipelines on Flink."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import logging
import re
import sys

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import flink_uber_jar_job_server
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner

MAGIC_HOST_NAMES = ['[local]', '[auto]']

_LOGGER = logging.getLogger(__name__)


class FlinkRunner(portable_runner.PortableRunner):
  def run_pipeline(self, pipeline, options):
    portable_options = options.view_as(pipeline_options.PortableOptions)
    flink_options = options.view_as(pipeline_options.FlinkRunnerOptions)
    if (flink_options.flink_master in MAGIC_HOST_NAMES and
        not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super(FlinkRunner, self).run_pipeline(pipeline, options)

  def default_job_server(self, options):
    flink_options = options.view_as(pipeline_options.FlinkRunnerOptions)
    flink_master = self.add_http_scheme(flink_options.flink_master)
    flink_options.flink_master = flink_master
    if (flink_options.flink_submit_uber_jar and
        flink_master not in MAGIC_HOST_NAMES):
      if sys.version_info < (3, 6):
        raise ValueError(
            'flink_submit_uber_jar requires Python 3.6+, current version %s' %
            sys.version)
      # This has to be changed [auto], otherwise we will attempt to submit a
      # the pipeline remotely on the Flink JobMaster which will _fail_.
      # DO NOT CHANGE the following line, unless you have tested this.
      flink_options.flink_master = '[auto]'
      return flink_uber_jar_job_server.FlinkUberJarJobServer(
          flink_master, options)
    else:
      return job_server.StopOnExitJobServer(FlinkJarJobServer(options))

  @staticmethod
  def add_http_scheme(flink_master):
    """Adds a http protocol scheme if none provided."""
    flink_master = flink_master.strip()
    if not flink_master in MAGIC_HOST_NAMES and \
          not re.search('^http[s]?://', flink_master):
      _LOGGER.info(
          'Adding HTTP protocol scheme to flink_master parameter: '
          'http://%s',
          flink_master)
      flink_master = 'http://' + flink_master
    return flink_master


class FlinkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super(FlinkJarJobServer, self).__init__(options)
    options = options.view_as(pipeline_options.FlinkRunnerOptions)
    self._jar = options.flink_job_server_jar
    self._master_url = options.flink_master
    self._flink_version = options.flink_version

  def path_to_jar(self):
    if self._jar:
      return self._jar
    else:
      return self.path_to_beam_jar(
          'runners:flink:%s:job-server:shadowJar' % self._flink_version)

  def java_arguments(
      self, job_port, artifact_port, expansion_port, artifacts_dir):
    return [
        '--flink-master',
        self._master_url,
        '--artifacts-dir',
        artifacts_dir,
        '--job-port',
        job_port,
        '--artifact-port',
        artifact_port,
        '--expansion-port',
        expansion_port
    ]
