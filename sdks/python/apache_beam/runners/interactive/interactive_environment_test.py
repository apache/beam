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
import sys
import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_environment as ie

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import call, patch
except ImportError:
  from mock import call, patch

# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_environment_test'


@unittest.skipIf(sys.version_info < (3, 6),
                 'The tests require at least Python 3.6 to work.')
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
                                               runner.PipelineState.RUNNING),
                                           is_main_job=True)
      self.assertTrue('pipeline must be an instance of apache_beam.Pipeline '
                      'or its subclass' in ctx.exception)

  def test_fail_to_set_pipeline_result_value_not_pipeline_result(self):
    class NotResult(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      ie.current_env().set_pipeline_result(
          self._p,
          NotResult(),
          is_main_job=True)
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
    ie.current_env().set_pipeline_result(
        pipeline,
        pipeline_result,
        is_main_job=True)
    self.assertIs(ie.current_env().pipeline_result(pipeline), pipeline_result)

  def test_determine_terminal_state(self):
    for state in (runner.PipelineState.DONE,
                  runner.PipelineState.FAILED,
                  runner.PipelineState.CANCELLED,
                  runner.PipelineState.UPDATED,
                  runner.PipelineState.DRAINED):
      ie.current_env().set_pipeline_result(
          self._p,
          runner.PipelineResult(state),
          is_main_job=True)
      self.assertTrue(ie.current_env().is_terminated(self._p))
    for state in (runner.PipelineState.UNKNOWN,
                  runner.PipelineState.STARTING,
                  runner.PipelineState.STOPPED,
                  runner.PipelineState.RUNNING,
                  runner.PipelineState.DRAINING,
                  runner.PipelineState.PENDING,
                  runner.PipelineState.CANCELLING,
                  runner.PipelineState.UNRECOGNIZED):
      ie.current_env().set_pipeline_result(
          self._p,
          runner.PipelineResult(state),
          is_main_job=True)
      self.assertFalse(ie.current_env().is_terminated(self._p))

  def test_evict_pipeline_result(self):
    pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(
        self._p,
        pipeline_result,
        is_main_job=True)
    self.assertIs(ie.current_env().evict_pipeline_result(self._p),
                  pipeline_result)
    self.assertIs(ie.current_env().pipeline_result(self._p), None)

  def test_pipeline_result_is_none_when_pipeline_absent(self):
    self.assertIs(ie.current_env().pipeline_result(self._p), None)
    self.assertIs(ie.current_env().is_terminated(self._p), True)
    self.assertIs(ie.current_env().evict_pipeline_result(self._p), None)

  @patch('atexit.register')
  def test_no_cleanup_when_cm_none(self,
                                   mocked_atexit):
    ie.new_env(None)
    mocked_atexit.assert_not_called()

  @patch('atexit.register')
  def test_cleanup_when_cm_not_none(self,
                                    mocked_atexit):
    ie.new_env(cache.FileBasedCacheManager())
    mocked_atexit.assert_called_once()

  @patch('atexit.register')
  @patch('atexit.unregister')
  def test_cleanup_unregistered_when_not_none_cm_cleared(self,
                                                         mocked_unreg,
                                                         mocked_reg):
    ie.new_env(cache.FileBasedCacheManager())
    mocked_reg.assert_called_once()
    mocked_unreg.assert_not_called()
    ie.current_env().set_cache_manager(None)
    mocked_reg.assert_called_once()
    mocked_unreg.assert_called_once()

  @patch('atexit.register')
  @patch('atexit.unregister')
  def test_cleanup_reregistered_when_cm_changed(self,
                                                mocked_unreg,
                                                mocked_reg):
    ie.new_env(cache.FileBasedCacheManager())
    mocked_unreg.assert_not_called()
    ie.current_env().set_cache_manager(cache.FileBasedCacheManager())
    mocked_unreg.assert_called_once()
    mocked_reg.assert_has_calls([call(ie.current_env().cleanup),
                                 call(ie.current_env().cleanup)])

  @patch('apache_beam.runners.interactive.interactive_environment'
         '.InteractiveEnvironment.cleanup')
  def test_cleanup_invoked_when_new_env_replace_not_none_env(self,
                                                             mocked_cleanup):
    ie._interactive_beam_env = None
    ie.new_env(cache.FileBasedCacheManager())
    mocked_cleanup.assert_not_called()
    ie.new_env(cache.FileBasedCacheManager())
    mocked_cleanup.assert_called_once()

  @patch('apache_beam.runners.interactive.interactive_environment'
         '.InteractiveEnvironment.cleanup')
  def test_cleanup_invoked_when_cm_changed(self,
                                           mocked_cleanup):
    ie._interactive_beam_env = None
    ie.new_env(cache.FileBasedCacheManager())
    ie.current_env().set_cache_manager(cache.FileBasedCacheManager())
    mocked_cleanup.assert_called_once()

  @patch('atexit.register')
  @patch('atexit.unregister')
  def test_cleanup_registered_when_none_cm_changed(self,
                                                   mocked_unreg,
                                                   mocked_reg):
    ie.new_env(None)
    mocked_reg.assert_not_called()
    mocked_unreg.assert_not_called()
    ie.current_env().set_cache_manager(cache.FileBasedCacheManager())
    mocked_reg.assert_called_once()
    mocked_unreg.assert_not_called()

  @patch('atexit.register')
  @patch('atexit.unregister')
  def test_noop_when_cm_is_not_changed(self,
                                       mocked_unreg,
                                       mocked_reg):
    cache_manager = cache.FileBasedCacheManager()
    ie.new_env(cache_manager)
    mocked_unreg.assert_not_called()
    mocked_reg.assert_called_once()
    ie.current_env().set_cache_manager(cache_manager)
    mocked_unreg.assert_not_called()
    mocked_reg.assert_called_once()


if __name__ == '__main__':
  unittest.main()
