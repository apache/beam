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

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner

PUBLISHED_FLINK_VERSIONS = ['1.6', '1.7', '1.8']


class FlinkRunner(portable_runner.PortableRunner):
  def default_job_server(self, options):
    return FlinkJarJobServer(options)


class FlinkRunnerOptions(pipeline_options.PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--flink_master_url', default='[local]')
    parser.add_argument('--flink_version',
                        default=PUBLISHED_FLINK_VERSIONS[-1],
                        choices=PUBLISHED_FLINK_VERSIONS,
                        help='Flink version to use.')
    parser.add_argument('--flink_job_server_jar',
                        help='Path or URL to a flink jobserver jar.')


class FlinkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super(FlinkJarJobServer, self).__init__()
    options = options.view_as(FlinkRunnerOptions)
    self._jar = options.flink_job_server_jar
    self._master_url = options.flink_master_url
    self._flink_version = options.flink_version

  def path_to_jar(self):
    if self._jar:
      return self._jar
    else:
      return self.path_to_gradle_target_jar(
          'runners:flink:%s:job-server:shadowJar' % self._flink_version)

  def java_arguments(self, job_port, artifacts_dir):
    return [
        '--flink-master-url', self._master_url,
        '--artifacts-dir', artifacts_dir,
        '--job-port', job_port,
        '--artifact-port', 0,
        '--expansion-port', 0
    ]
