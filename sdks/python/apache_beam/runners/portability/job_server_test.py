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

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability.job_server import JavaJarJobServer


class JavaJarJobServerStub(JavaJarJobServer):
  def java_arguments(
      self,
      job_port,
      artifact_port,
      expansion_port,
      artifacts_dir,
      jar_cache_dir):
    return [
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

  def path_to_jar(self):
    return '/path/to/jar'

  @staticmethod
  def local_jar(url, jar_cache_dir):
    print(f'local_jar url({url}) jar_cache_dir({jar_cache_dir})')
    return url


class JavaJarJobServerTest(unittest.TestCase):
  def test_subprocess_cmd_and_endpoint(self):
    pipeline_options = PipelineOptions([
        '--job_port=8099',
        '--artifact_port=8098',
        '--expansion_port=8097',
        '--artifacts_dir=/path/to/artifacts/',
        '--job_server_java_launcher=/path/to/java',
        '--job_server_jvm_properties=-Dsome.property=value'
        '--jar_cache_dir=/path/to/cache_dir/'
    ])
    job_server = JavaJarJobServerStub(pipeline_options)
    subprocess_cmd, endpoint = job_server.subprocess_cmd_and_endpoint()
    self.assertEqual(
        subprocess_cmd,
        [
            '/path/to/java',
            '-jar',
            '-Dsome.property=value',
            '/path/to/jar',
            '--artifacts-dir',
            '/path/to/artifacts/',
            '--jar-cache-dir',
            '/path/to/cache_dir/',
            '--job-port',
            8099,
            '--artifact-port',
            8098,
            '--expansion-port',
            8097
        ])
    self.assertEqual(endpoint, 'localhost:8099')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
