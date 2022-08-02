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
import unittest

from apache_beam.options import pipeline_options
from apache_beam.runners.portability.spark_runner import SparkRunner


class SparkTestPipelineOptions(pipeline_options.PipelineOptions):
  def view_as(self, cls):
    # Ensure only SparkRunnerOptions and JobServerOptions are used when calling
    # default_job_server. If other options classes are needed, the cache key
    # must include them to prevent incorrect hits.
    assert (
        cls is pipeline_options.SparkRunnerOptions or
        cls is pipeline_options.JobServerOptions)
    return super().view_as(cls)


class SparkJavaJobServerTest(unittest.TestCase):
  def test_job_server_cache(self):
    # Multiple SparkRunner instances may be created, so we need to make sure we
    # cache job servers across runner instances.

    # Most pipeline-specific options, such as sdk_worker_parallelism, don't
    # affect job server configuration, so it is ok to ignore them for caching.
    job_server1 = SparkRunner().default_job_server(
        SparkTestPipelineOptions(['--sdk_worker_parallelism=1']))
    job_server2 = SparkRunner().default_job_server(
        SparkTestPipelineOptions(['--sdk_worker_parallelism=2']))
    self.assertIs(job_server2, job_server1)

    # JobServerOptions and SparkRunnerOptions do affect job server
    # configuration, so using different pipeline options gives us a different
    # job server.
    job_server3 = SparkRunner().default_job_server(
        SparkTestPipelineOptions(['--job_port=1234']))
    self.assertIsNot(job_server3, job_server1)

    job_server4 = SparkRunner().default_job_server(
        SparkTestPipelineOptions(['--spark_master_url=spark://localhost:5678']))
    self.assertIsNot(job_server4, job_server1)
    self.assertIsNot(job_server4, job_server3)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
