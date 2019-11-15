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

"""Tests for apache_beam.runners.interactive.interactive_environment."""
from __future__ import absolute_import

import importlib
import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import interactive_environment as ie

# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_environment_test'


class InteractiveEnvironmentTest(unittest.TestCase):

  def setUp(self):
    self._p = beam.Pipeline()
    self._var_in_class_instance = 'a var in class instance'
    ie.new_env()

  def assertVariableWatched(self, variable_name, variable_val):
    self.assertTrue(self._is_variable_watched(variable_name, variable_val))

  def assertVariableNotWatched(self, variable_name, variable_val):
    self.assertFalse(self._is_variable_watched(variable_name, variable_val))

  def _is_variable_watched(self, variable_name, variable_val):
    return any([(variable_name, variable_val) in watching for watching in
                ie.current_env().watching()])

  def _a_function_with_local_watched(self):
    local_var_watched = 123  # pylint: disable=possibly-unused-variable
    ie.current_env().watch(locals())

  def _a_function_not_watching_local(self):
    local_var_not_watched = 456  # pylint: disable=unused-variable

  def test_watch_main_by_default(self):
    self.assertTrue('__main__' in ie.current_env()._watching_set)
    # __main__ module has variable __name__ with value '__main__'
    self.assertVariableWatched('__name__', '__main__')

  def test_watch_a_module_by_name(self):
    self.assertFalse(
        _module_name in ie.current_env()._watching_set)
    self.assertVariableNotWatched('_module_name', _module_name)
    ie.current_env().watch(_module_name)
    self.assertTrue(
        _module_name in
        ie.current_env()._watching_set)
    self.assertVariableWatched('_module_name', _module_name)

  def test_watch_a_module_by_module_object(self):
    module = importlib.import_module(_module_name)
    self.assertFalse(module in ie.current_env()._watching_set)
    self.assertVariableNotWatched('_module_name', _module_name)
    ie.current_env().watch(module)
    self.assertTrue(module in ie.current_env()._watching_set)
    self.assertVariableWatched('_module_name', _module_name)

  def test_watch_locals(self):
    self.assertVariableNotWatched('local_var_watched', 123)
    self.assertVariableNotWatched('local_var_not_watched', 456)
    self._a_function_with_local_watched()
    self.assertVariableWatched('local_var_watched', 123)
    self._a_function_not_watching_local()
    self.assertVariableNotWatched('local_var_not_watched', 456)

  def test_watch_class_instance(self):
    self.assertVariableNotWatched('_var_in_class_instance',
                                  self._var_in_class_instance)
    ie.current_env().watch(self)
    self.assertVariableWatched('_var_in_class_instance',
                               self._var_in_class_instance)

  def test_fail_to_set_pipeline_result_key_not_pipeline(self):
    class NotPipeline(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      ie.current_env().set_pipeline_result(NotPipeline(),
                                           runner.PipelineResult(
                                               runner.PipelineState.RUNNING))
      self.assertTrue('pipeline must be an instance of apache_beam.Pipeline '
                      'or its subclass' in ctx.exception)

  def test_fail_to_set_pipeline_result_value_not_pipeline_result(self):
    class NotResult(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      ie.current_env().set_pipeline_result(self._p, NotResult())
      self.assertTrue('result must be an instance of '
                      'apache_beam.runners.runner.PipelineResult or its '
                      'subclass' in ctx.exception)

  def test_set_pipeline_result_successfully(self):
    class PipelineSubClass(beam.Pipeline):
      pass

    class PipelineResultSubClass(runner.PipelineResult):
      pass

    pipeline = PipelineSubClass()
    pipeline_result = PipelineResultSubClass(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(pipeline, pipeline_result)
    self.assertIs(ie.current_env().pipeline_result(pipeline), pipeline_result)

  def test_determine_terminal_state(self):
    for state in (runner.PipelineState.DONE,
                  runner.PipelineState.FAILED,
                  runner.PipelineState.CANCELLED,
                  runner.PipelineState.UPDATED,
                  runner.PipelineState.DRAINED):
      ie.current_env().set_pipeline_result(self._p, runner.PipelineResult(
          state))
      self.assertTrue(ie.current_env().is_terminated(self._p))
    for state in (runner.PipelineState.UNKNOWN,
                  runner.PipelineState.STARTING,
                  runner.PipelineState.STOPPED,
                  runner.PipelineState.RUNNING,
                  runner.PipelineState.DRAINING,
                  runner.PipelineState.PENDING,
                  runner.PipelineState.CANCELLING,
                  runner.PipelineState.UNRECOGNIZED):
      ie.current_env().set_pipeline_result(self._p, runner.PipelineResult(
          state))
      self.assertFalse(ie.current_env().is_terminated(self._p))

  def test_evict_pipeline_result(self):
    pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(self._p, pipeline_result)
    self.assertIs(ie.current_env().evict_pipeline_result(self._p),
                  pipeline_result)
    self.assertIs(ie.current_env().pipeline_result(self._p), None)

  def test_is_none_when_pipeline_absent(self):
    self.assertIs(ie.current_env().pipeline_result(self._p), None)
    self.assertIs(ie.current_env().is_terminated(self._p), True)
    self.assertIs(ie.current_env().evict_pipeline_result(self._p), None)


if __name__ == '__main__':
  unittest.main()
