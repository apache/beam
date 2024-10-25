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

import os
import re
import urllib

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import spark_uber_jar_job_server

# https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
LOCAL_MASTER_PATTERN = r'^local(\[.+\])?$'

# Since Java job servers are heavyweight external processes, cache them.
# This applies only to SparkJarJobServer, not SparkUberJarJobServer.
JOB_SERVER_CACHE = {}


class SparkRunner(portable_runner.PortableRunner):
  """A runner for launching jobs on Spark, automatically starting a local
  spark master if one is not given.
  """

  # Inherits run_portable_pipeline from PortableRunner.

  def default_environment(self, options):
    spark_options = options.view_as(pipeline_options.SparkRunnerOptions)
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (re.match(LOCAL_MASTER_PATTERN, spark_options.spark_master_url) and
        not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
    spark_options = options.view_as(pipeline_options.SparkRunnerOptions)
    if spark_options.spark_submit_uber_jar:
      if not spark_options.spark_rest_url:
        raise ValueError('Option spark_rest_url must be set.')
      return spark_uber_jar_job_server.SparkUberJarJobServer(
          spark_options.spark_rest_url, options)
    # Use Java job server by default.
    # Only SparkRunnerOptions and JobServerOptions affect job server
    # configuration, so concat those as the cache key.
    job_server_options = options.view_as(pipeline_options.JobServerOptions)
    options_str = str(spark_options) + str(job_server_options)
    if not options_str in JOB_SERVER_CACHE:
      JOB_SERVER_CACHE[options_str] = job_server.StopOnExitJobServer(
          SparkJarJobServer(options))
    return JOB_SERVER_CACHE[options_str]

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service,
        options,
        retain_unknown_options=options.view_as(
            pipeline_options.SparkRunnerOptions).spark_submit_uber_jar)


class SparkJarJobServer(job_server.JavaJarJobServer):
  def __init__(self, options):
    super().__init__(options)
    options = options.view_as(pipeline_options.SparkRunnerOptions)
    self._jar = options.spark_job_server_jar
    self._master_url = options.spark_master_url
    self._spark_version = options.spark_version

  def path_to_jar(self):
    if self._jar:
      if not os.path.exists(self._jar):
        url = urllib.parse.urlparse(self._jar)
        if not url.scheme:
          raise ValueError(
              'Unable to parse jar URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file path, make sure '
              'the file exists; you may have to first build the job server '
              'using `./gradlew runners:spark:3:job-server:shadowJar`.' %
              self._jar)
      return self._jar
    else:
      if self._spark_version == '2':
        raise ValueError('Support for Spark 2 was dropped.')
      return self.path_to_beam_jar(':runners:spark:3:job-server:shadowJar')

  def java_arguments(
      self,
      job_port,
      artifact_port,
      expansion_port,
      artifacts_dir,
      jar_cache_dir):
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
        expansion_port,
        '--jar-cache-dir',
        jar_cache_dir
    ]
