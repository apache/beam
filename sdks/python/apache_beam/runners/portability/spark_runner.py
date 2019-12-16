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

"""A runner for executing portable pipelines on Spark."""

from __future__ import absolute_import
from __future__ import print_function

import re

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner

# https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
LOCAL_MASTER_PATTERN = r'^local(\[.+\])?$'


class SparkRunner(portable_runner.PortableRunner):
  def run_pipeline(self, pipeline, options):
    spark_options = options.view_as(SparkRunnerOptions)
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (re.match(LOCAL_MASTER_PATTERN, spark_options.spark_master_url)
        and not portable_options.environment_type
        and not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super(SparkRunner, self).run_pipeline(pipeline, options)

  def default_job_server(self, options):
    # TODO(BEAM-8139) submit a Spark jar to a cluster
    return job_server.StopOnExitJobServer(SparkJarJobServer(options))


class SparkRunnerOptions(pipeline_options.PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--spark_master_url',
                        default='local[4]',
                        help='Spark master URL (spark://HOST:PORT). '
                             'Use "local" (single-threaded) or "local[*]" '
                             '(multi-threaded) to start a local cluster for '
                             'the execution.')
    parser.add_argument('--spark_job_server_jar',
                        help='Path or URL to a Beam Spark jobserver jar.')


class SparkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super(SparkJarJobServer, self).__init__(options)
    options = options.view_as(SparkRunnerOptions)
    self._jar = options.spark_job_server_jar
    self._master_url = options.spark_master_url

  def path_to_jar(self):
    if self._jar:
      return self._jar
    else:
      return self.path_to_beam_jar('runners:spark:job-server:shadowJar')

  def java_arguments(
      self, job_port, artifact_port, expansion_port, artifacts_dir):
    return [
        '--spark-master-url', self._master_url,
        '--artifacts-dir', artifacts_dir,
        '--job-port', job_port,
        '--artifact-port', artifact_port,
        '--expansion-port', expansion_port
    ]
