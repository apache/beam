# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the PipelineRunner and DirectPipelineRunner classes.

Note that PipelineRunner and DirectPipelineRunner functionality is tested in all
the other unit tests. In this file we choose to test only aspects related to
caching and clearing values that are not tested elsewhere.
"""

import unittest

from google.cloud.dataflow.internal import apiclient
from google.cloud.dataflow.pipeline import Pipeline
from google.cloud.dataflow.runners import create_runner
from google.cloud.dataflow.runners import DataflowPipelineRunner
from google.cloud.dataflow.runners import DirectPipelineRunner
from google.cloud.dataflow.runners.runner import PValueCache
import google.cloud.dataflow.transforms as ptransform
from google.cloud.dataflow.utils.options import get_options


class RunnerTest(unittest.TestCase):

  def test_value_cached_and_cleared(self):
    cache = PValueCache()
    pipeline = Pipeline(DirectPipelineRunner(cache=cache))
    pcoll = pipeline | ptransform.Create('start', [1, 2, 3])
    self.assertEqual([1, 2, 3], list(pcoll.get()))
    self.assertTrue(cache.is_cached(pcoll))
    pipeline.runner.clear(pipeline, pcoll)
    self.assertFalse(cache.is_cached(pcoll))
    self.assertEqual([1, 2, 3], list(pcoll.get()))

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectPipelineRunner'), DirectPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('DataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('BlockingDataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertRaises(RuntimeError, create_runner, 'xyz')

  def test_remote_runner_translation(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(
        remote_runner,
        options=get_options([
            '--dataflow_endpoint=ignored',
            '--job_name=test-job',
            '--project=test-project',
            '--staging_location=ignored',
            '--temp_location=/dev/null',
            '--no_auth=True']))

    res = (p | ptransform.Create('create', [1, 2, 3])
           | ptransform.FlatMap('do', lambda x: [(x, x)])
           | ptransform.GroupByKey('gbk'))
    remote_runner.job = apiclient.Job(p.options)
    super(DataflowPipelineRunner, remote_runner).run(p)


if __name__ == '__main__':
  unittest.main()
