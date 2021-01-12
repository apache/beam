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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import os
import re
import sys
import urllib

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import spark_uber_jar_job_server

# https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
LOCAL_MASTER_PATTERN = r'^local(\[.+\])?$'


class SparkRunner(portable_runner.PortableRunner):
  def run_pipeline(self, pipeline, options):
    spark_options = options.view_as(pipeline_options.SparkRunnerOptions)
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (re.match(LOCAL_MASTER_PATTERN, spark_options.spark_master_url) and
        not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super(SparkRunner, self).run_pipeline(pipeline, options)

  def default_job_server(self, options):
    spark_options = options.view_as(pipeline_options.SparkRunnerOptions)
    if spark_options.spark_submit_uber_jar:
      if sys.version_info < (3, 6):
        raise ValueError(
            'spark_submit_uber_jar requires Python 3.6+, current version %s' %
            sys.version)
      if not spark_options.spark_rest_url:
        raise ValueError('Option spark_rest_url must be set.')
      return spark_uber_jar_job_server.SparkUberJarJobServer(
          spark_options.spark_rest_url, options)
    return job_server.StopOnExitJobServer(SparkJarJobServer(options))

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service,
        options,
        retain_unknown_options=options.view_as(
            pipeline_options.SparkRunnerOptions).spark_submit_uber_jar)


class SparkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super(SparkJarJobServer, self).__init__(options)
    options = options.view_as(pipeline_options.SparkRunnerOptions)
    self._jar = options.spark_job_server_jar
    self._master_url = options.spark_master_url

  def path_to_jar(self):
    if self._jar:
      if not os.path.exists(self._jar):
        url = urllib.parse.urlparse(self._jar)
        if not url.scheme:
          raise ValueError(
              'Unable to parse jar URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file path, make sure '
              'the file exists; you may have to first build the job server '
              'using `./gradlew runners:spark:job-server:shadowJar`.' %
              self._jar)
      return self._jar
    else:
      return self.path_to_beam_jar(':runners:spark:job-server:shadowJar')

  def java_arguments(
      self, job_port, artifact_port, expansion_port, artifacts_dir):
    return [
        '--spark-master-url',
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
