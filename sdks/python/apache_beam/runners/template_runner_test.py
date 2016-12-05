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

"""Unit tests for templated pipelines."""

from __future__ import absolute_import

import json
import unittest
import tempfile

import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.runners.dataflow_runner import DataflowPipelineRunner
from apache_beam.utils.options import PipelineOptions
from apache_beam.internal import apiclient


class TemplatingDataflowPipelineRunnerTest(unittest.TestCase):
  """TemplatingDataflow tests."""
  def test_full_completion(self):
    dummy_file = tempfile.NamedTemporaryFile()
    dummy_dir = tempfile.mkdtemp()

    remote_runner = DataflowPipelineRunner()
    pipeline = Pipeline(remote_runner,
                        options=PipelineOptions([
                            '--dataflow_endpoint=ignored',
                            '--sdk_location=' + dummy_file.name,
                            '--job_name=test-job',
                            '--project=test-project',
                            '--staging_location=' + dummy_dir,
                            '--temp_location=/dev/null',
                            '--template_location=' + dummy_file.name,
                            '--no_auth=True']))

    pipeline | beam.Create([1, 2, 3]) | beam.Map(lambda x: x) # pylint: disable=expression-not-assigned
    pipeline.run()
    with open(dummy_file.name) as template_file:
      saved_job_dict = json.load(template_file)
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']
          ['options']['project'], 'test-project')
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']
          ['options']['job_name'], 'test-job')

  def test_bad_path(self):
    dummy_sdk_file = tempfile.NamedTemporaryFile()
    remote_runner = DataflowPipelineRunner()
    pipeline = Pipeline(remote_runner,
                        options=PipelineOptions([
                            '--dataflow_endpoint=ignored',
                            '--sdk_location=' + dummy_sdk_file.name,
                            '--job_name=test-job',
                            '--project=test-project',
                            '--staging_location=ignored',
                            '--temp_location=/dev/null',
                            '--template_location=/bad/path',
                            '--no_auth=True']))
    remote_runner.job = apiclient.Job(pipeline.options)

    with self.assertRaises(IOError):
      pipeline.run()


if __name__ == '__main__':
  unittest.main()
