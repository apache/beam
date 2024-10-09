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

"""ValidatesRunner tests for CombineFn lifecycle and bundle methods."""

# pytype: skip-file

import unittest

import pytest
from parameterized import parameterized_class

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.portability import fn_api_runner
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.combinefn_lifecycle_pipeline import CallSequenceEnforcingCombineFn
from apache_beam.transforms.combinefn_lifecycle_pipeline import run_combine
from apache_beam.transforms.combinefn_lifecycle_pipeline import run_combine_uncopyable_attr
from apache_beam.transforms.combinefn_lifecycle_pipeline import run_pardo


@pytest.mark.it_validatesrunner
class CombineFnLifecycleTest(unittest.TestCase):
  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)

  def test_combine(self):
    run_combine(self.pipeline)

  def test_non_liftable_combine(self):
    run_combine(self.pipeline, lift_combiners=False)

  def test_combining_value_state(self):
    if ('DataflowRunner' in self.pipeline.get_pipeline_options().view_as(
        StandardOptions).runner):
      self.skipTest('https://github.com/apache/beam/issues/20722')
    run_pardo(self.pipeline)


@parameterized_class([
    {'runner': direct_runner.BundleBasedDirectRunner, 'pickler': 'dill'},
    {'runner': direct_runner.BundleBasedDirectRunner, 'pickler': 'cloudpickle'},
    {'runner': fn_api_runner.FnApiRunner, 'pickler': 'dill'},
    {'runner': fn_api_runner.FnApiRunner, 'pickler': 'cloudpickle'},
    ])  # yapf: disable
class LocalCombineFnLifecycleTest(unittest.TestCase):
  def tearDown(self):
    CallSequenceEnforcingCombineFn.instances.clear()

  def test_combine(self):
    test_options = PipelineOptions(flags=[f"--pickle_library={self.pickler}"])
    run_combine(TestPipeline(runner=self.runner(), options=test_options))
    self._assert_teardown_called()

  def test_combine_deepcopy_fails(self):
    test_options = PipelineOptions(flags=[f"--pickle_library={self.pickler}"])
    run_combine_uncopyable_attr(
        TestPipeline(runner=self.runner(), options=test_options))
    self._assert_teardown_called()

  def test_non_liftable_combine(self):
    test_options = PipelineOptions(flags=['--allow_unsafe_triggers'])
    run_combine(
        TestPipeline(runner=self.runner(), options=test_options),
        lift_combiners=False)
    self._assert_teardown_called()

  def test_combining_value_state(self):
    run_pardo(TestPipeline(runner=self.runner()))
    self._assert_teardown_called()

  def _assert_teardown_called(self):
    """Ensures that teardown has been invoked for all CombineFns."""
    for instance in CallSequenceEnforcingCombineFn.instances:
      self.assertTrue(instance._teardown_called)


if __name__ == '__main__':
  unittest.main()
