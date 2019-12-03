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

from __future__ import absolute_import
from __future__ import print_function

import logging
import re
import sys

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import flink_uber_jar_job_server
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner

PUBLISHED_FLINK_VERSIONS = ['1.7', '1.8', '1.9']
MAGIC_HOST_NAMES = ['[local]', '[auto]']


class FlinkRunner(portable_runner.PortableRunner):
  def run_pipeline(self, pipeline, options):
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (options.view_as(FlinkRunnerOptions).flink_master in MAGIC_HOST_NAMES
        and not portable_options.environment_type
        and not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super(FlinkRunner, self).run_pipeline(pipeline, options)

  def default_job_server(self, options):
    flink_master = self.add_http_scheme(
        options.view_as(FlinkRunnerOptions).flink_master)
    options.view_as(FlinkRunnerOptions).flink_master = flink_master
    if (options.view_as(FlinkRunnerOptions).flink_submit_uber_jar
        and flink_master not in MAGIC_HOST_NAMES):
      if sys.version_info < (3, 6):
        raise ValueError(
            'flink_submit_uber_jar requires Python 3.6+, current version %s'
            % sys.version)
      # This has to be changed [auto], otherwise we will attempt to submit a
      # the pipeline remotely on the Flink JobMaster which will _fail_.
      # DO NOT CHANGE the following line, unless you have tested this.
      options.view_as(FlinkRunnerOptions).flink_master = '[auto]'
      return flink_uber_jar_job_server.FlinkUberJarJobServer(flink_master)
    else:
      return job_server.StopOnExitJobServer(FlinkJarJobServer(options))

  @staticmethod
  def add_http_scheme(flink_master):
    """Adds a http protocol scheme if none provided."""
    flink_master = flink_master.strip()
    if not flink_master in MAGIC_HOST_NAMES and \
          not re.search('^http[s]?://', flink_master):
      logging.info('Adding HTTP protocol scheme to flink_master parameter: '
                   'http://%s', flink_master)
      flink_master = 'http://' + flink_master
    return flink_master


class FlinkRunnerOptions(pipeline_options.PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--flink_master',
                        default='[auto]',
                        help='Flink master address (http://host:port)'
                             ' Use "[local]" to start a local cluster'
                             ' for the execution. Use "[auto]" if you'
                             ' plan to either execute locally or let the'
                             ' Flink job server infer the cluster address.')
    parser.add_argument('--flink_version',
                        default=PUBLISHED_FLINK_VERSIONS[-1],
                        choices=PUBLISHED_FLINK_VERSIONS,
                        help='Flink version to use.')
    parser.add_argument('--flink_job_server_jar',
                        help='Path or URL to a flink jobserver jar.')
    parser.add_argument('--artifacts_dir', default=None)
    parser.add_argument('--flink_submit_uber_jar',
                        default=False,
                        action='store_true',
                        help='Create and upload an uberjar to the flink master'
                             ' directly, rather than starting up a job server.'
                             ' Only applies when flink_master is set to a'
                             ' cluster address.  Requires Python 3.6+.')


class FlinkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super(FlinkJarJobServer, self).__init__()
    options = options.view_as(FlinkRunnerOptions)
    self._jar = options.flink_job_server_jar
    self._master_url = options.flink_master
    self._flink_version = options.flink_version
    self._artifacts_dir = options.artifacts_dir

  def path_to_jar(self):
    if self._jar:
      return self._jar
    else:
      return self.path_to_beam_jar(
          'runners:flink:%s:job-server:shadowJar' % self._flink_version)

  def java_arguments(self, job_port, artifacts_dir):
    return [
        '--flink-master-url', self._master_url,
        '--artifacts-dir', (self._artifacts_dir
                            if self._artifacts_dir else artifacts_dir),
        '--job-port', job_port,
        '--artifact-port', 0,
        '--expansion-port', 0
    ]
