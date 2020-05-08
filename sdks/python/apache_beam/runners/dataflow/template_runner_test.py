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

# pytype: skip-file

from __future__ import absolute_import

import json
import tempfile
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class TemplatingDataflowRunnerTest(unittest.TestCase):
  """TemplatingDataflow tests."""
  def test_full_completion(self):
    # Create dummy file and close it.  Note that we need to do this because
    # Windows does not allow NamedTemporaryFiles to be reopened elsewhere
    # before the temporary file is closed.
    dummy_file = tempfile.NamedTemporaryFile(delete=False)
    dummy_file_name = dummy_file.name
    dummy_file.close()

    dummy_dir = tempfile.mkdtemp()

    remote_runner = DataflowRunner()
    with Pipeline(remote_runner,
                  options=PipelineOptions(['--dataflow_endpoint=ignored',
                                           '--sdk_location=' + dummy_file_name,
                                           '--job_name=test-job',
                                           '--project=test-project',
                                           '--staging_location=' + dummy_dir,
                                           '--temp_location=/dev/null',
                                           '--template_location=' +
                                           dummy_file_name,
                                           '--no_auth'])) as pipeline:

      pipeline | beam.Create([1, 2, 3]) | beam.Map(lambda x: x)  # pylint: disable=expression-not-assigned

    with open(dummy_file_name) as template_file:
      saved_job_dict = json.load(template_file)
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']['options']
          ['project'],
          'test-project')
      self.assertEqual(
          saved_job_dict['environment']['sdkPipelineOptions']['options']
          ['job_name'],
          'test-job')

  def test_bad_path(self):
    dummy_sdk_file = tempfile.NamedTemporaryFile()
    remote_runner = DataflowRunner()
    pipeline = Pipeline(
        remote_runner,
        options=PipelineOptions([
            '--dataflow_endpoint=ignored',
            '--sdk_location=' + dummy_sdk_file.name,
            '--job_name=test-job',
            '--project=test-project',
            '--staging_location=ignored',
            '--temp_location=/dev/null',
            '--template_location=/bad/path',
            '--no_auth'
        ]))
    remote_runner.job = apiclient.Job(
        pipeline._options, pipeline.to_runner_api())

    with self.assertRaises(IOError):
      pipeline.run().wait_until_finish()


if __name__ == '__main__':
  unittest.main()
