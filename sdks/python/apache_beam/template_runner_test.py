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

"""Integration tests for templated pipelines."""

from __future__ import absolute_import

import json
import unittest

# import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.runners.dataflow_runner import DataflowPipelineRunner
from apache_beam.utils.options import PipelineOptions
from apache_beam.internal import apiclient


class TemplatingDataflowPipelineRunnerTest(unittest.TestCase):
  """TemplatingDataflow integration tests."""
  def test_full_completion(self):
    remote_runner = DataflowPipelineRunner()
    pipeline = Pipeline(remote_runner,
                        options=PipelineOptions([
                            '--dataflow_endpoint=ignored',
                            '--job_name=test-job',
                            '--project=test-project',
                            '--staging_location=ignored',
                            '--temp_location=/dev/null',
                            '--template_location=/tmp/test-file',
                            '--no_auth=True']))
    remote_runner.job = apiclient.Job(pipeline.options)
    remote_runner.run(pipeline)

    with open('/tmp/test-file') as template_file:
      saved_job_dict = json.loads(template_file.read())
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']['project'],
          'test-project')
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']['job_name'],
          'test-job')

  def test_bad_path(self):
    remote_runner = DataflowPipelineRunner()
    pipeline = Pipeline(remote_runner,
                        options=PipelineOptions([
                            '--dataflow_endpoint=ignored',
                            '--job_name=test-job',
                            '--project=test-project',
                            '--staging_location=ignored',
                            '--temp_location=/dev/null',
                            '--template_location=/bad/path',
                            '--no_auth=True']))
    remote_runner.job = apiclient.Job(pipeline.options)

    with self.assertRaises(IOError):
      remote_runner.run(pipeline)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
