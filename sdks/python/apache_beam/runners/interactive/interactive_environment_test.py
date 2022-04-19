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
# pytype: skip-file

import importlib
import unittest
from unittest.mock import patch

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.runners.interactive.sql.sql_chain import SqlNode

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
    return any((variable_name, variable_val) in watching
               for watching in ie.current_env().watching())

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
    self.assertFalse(_module_name in ie.current_env()._watching_set)
    self.assertVariableNotWatched('_module_name', _module_name)
    ie.current_env().watch(_module_name)
    self.assertTrue(_module_name in ie.current_env()._watching_set)
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
    self.assertVariableNotWatched(
        '_var_in_class_instance', self._var_in_class_instance)
    ie.current_env().watch(self)
    self.assertVariableWatched(
        '_var_in_class_instance', self._var_in_class_instance)

  def test_fail_to_set_pipeline_result_key_not_pipeline(self):
    class NotPipeline(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      ie.current_env().set_pipeline_result(
          NotPipeline(), runner.PipelineResult(runner.PipelineState.RUNNING))
      self.assertTrue(
          'pipeline must be an instance of apache_beam.Pipeline '
          'or its subclass' in ctx.exception)

  def test_fail_to_set_pipeline_result_value_not_pipeline_result(self):
    class NotResult(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      ie.current_env().set_pipeline_result(self._p, NotResult())
      self.assertTrue(
          'result must be an instance of '
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
      ie.current_env().set_pipeline_result(
          self._p, runner.PipelineResult(state))
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
          self._p, runner.PipelineResult(state))
      self.assertFalse(ie.current_env().is_terminated(self._p))

  def test_evict_pipeline_result(self):
    pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(self._p, pipeline_result)
    self.assertIs(
        ie.current_env().evict_pipeline_result(self._p), pipeline_result)
    self.assertIs(ie.current_env().pipeline_result(self._p), None)

  def test_pipeline_result_is_none_when_pipeline_absent(self):
    self.assertIs(ie.current_env().pipeline_result(self._p), None)
    self.assertIs(ie.current_env().is_terminated(self._p), True)
    self.assertIs(ie.current_env().evict_pipeline_result(self._p), None)

  def test_cleanup_registered_when_creating_new_env(self):
    with patch('atexit.register') as mocked_atexit:
      _ = ie.InteractiveEnvironment()
      mocked_atexit.assert_called_once()

  def test_cleanup_invoked_when_new_env_replace_not_none_env(self):
    ie._interactive_beam_env = None
    ie.new_env()
    with patch('apache_beam.runners.interactive.interactive_environment'
               '.InteractiveEnvironment.cleanup') as mocked_cleanup:
      ie.new_env()
      mocked_cleanup.assert_called_once()

  def test_cleanup_not_invoked_when_cm_changed_from_none(self):
    env = ie.InteractiveEnvironment()
    with patch('apache_beam.runners.interactive.interactive_environment'
               '.InteractiveEnvironment.cleanup') as mocked_cleanup:
      dummy_pipeline = 'dummy'
      self.assertIsNone(env.get_cache_manager(dummy_pipeline))
      cache_manager = cache.FileBasedCacheManager()
      env.set_cache_manager(cache_manager, dummy_pipeline)
      mocked_cleanup.assert_not_called()
      self.assertIs(env.get_cache_manager(dummy_pipeline), cache_manager)

  def test_cleanup_invoked_when_not_none_cm_changed(self):
    env = ie.InteractiveEnvironment()
    with patch('apache_beam.runners.interactive.interactive_environment'
               '.InteractiveEnvironment.cleanup') as mocked_cleanup:
      dummy_pipeline = 'dummy'
      env.set_cache_manager(cache.FileBasedCacheManager(), dummy_pipeline)
      mocked_cleanup.assert_not_called()
      env.set_cache_manager(cache.FileBasedCacheManager(), dummy_pipeline)
      mocked_cleanup.assert_called_once()

  def test_noop_when_cm_is_not_changed(self):
    cache_manager = cache.FileBasedCacheManager()
    dummy_pipeline = 'dummy'
    env = ie.InteractiveEnvironment()
    with patch('apache_beam.runners.interactive.interactive_environment'
               '.InteractiveEnvironment.cleanup') as mocked_cleanup:
      env._cache_managers[str(id(dummy_pipeline))] = cache_manager
      mocked_cleanup.assert_not_called()
      env.set_cache_manager(cache_manager, dummy_pipeline)
      mocked_cleanup.assert_not_called()

  def test_get_cache_manager_creates_cache_manager_if_absent(self):
    env = ie.InteractiveEnvironment()
    dummy_pipeline = beam.Pipeline()
    self.assertIsNone(env.get_cache_manager(dummy_pipeline))
    self.assertIsNotNone(
        env.get_cache_manager(dummy_pipeline, create_if_absent=True))

  def test_track_user_pipeline_cleanup_non_inspectable_pipeline(self):
    ie.new_env()
    dummy_pipeline_1 = beam.Pipeline()
    dummy_pipeline_2 = beam.Pipeline()
    dummy_pipeline_3 = beam.Pipeline()
    dummy_pipeline_4 = beam.Pipeline()
    dummy_pcoll = dummy_pipeline_4 | beam.Create([1])
    dummy_pipeline_5 = beam.Pipeline()
    dummy_non_inspectable_pipeline = 'dummy'
    ie.current_env().watch(locals())
    from apache_beam.runners.interactive.background_caching_job import BackgroundCachingJob
    ie.current_env().set_background_caching_job(
        dummy_pipeline_1,
        BackgroundCachingJob(
            runner.PipelineResult(runner.PipelineState.DONE), limiters=[]))
    ie.current_env().set_test_stream_service_controller(dummy_pipeline_2, None)
    ie.current_env().set_cache_manager(
        cache.FileBasedCacheManager(), dummy_pipeline_3)
    ie.current_env().mark_pcollection_computed([dummy_pcoll])
    ie.current_env().set_cached_source_signature(
        dummy_non_inspectable_pipeline, None)
    ie.current_env().set_pipeline_result(
        dummy_pipeline_5, runner.PipelineResult(runner.PipelineState.RUNNING))
    with patch('apache_beam.runners.interactive.interactive_environment'
               '.InteractiveEnvironment.cleanup') as mocked_cleanup:
      ie.current_env().track_user_pipelines()
      mocked_cleanup.assert_called_once()

  def test_evict_pcollections(self):
    """Tests the evicton logic in the InteractiveEnvironment."""

    # Create two PCollection, one that will be evicted and another that won't.
    p_to_evict = beam.Pipeline()
    to_evict = p_to_evict | beam.Create([])

    p_not_evicted = beam.Pipeline()
    not_evicted = p_not_evicted | beam.Create([])

    # Mark the PCollections as computed because the eviction logic only works
    # on computed PCollections.
    ie.current_env().mark_pcollection_computed([to_evict, not_evicted])
    self.assertSetEqual(
        ie.current_env().computed_pcollections, {to_evict, not_evicted})

    # Evict the PCollection and then check that the other PCollection is safe.
    ie.current_env().evict_computed_pcollections(p_to_evict)
    self.assertSetEqual(ie.current_env().computed_pcollections, {not_evicted})

  def test_set_get_recording_manager(self):
    ie._interactive_beam_env = None
    ie.new_env()

    p = beam.Pipeline()
    rm = RecordingManager(p)
    ie.current_env().set_recording_manager(rm, p)
    self.assertIs(rm, ie.current_env().get_recording_manager(p))

  def test_recording_manager_create_if_absent(self):
    ie._interactive_beam_env = None
    ie.new_env()

    p = beam.Pipeline()
    self.assertFalse(ie.current_env().get_recording_manager(p))
    self.assertTrue(
        ie.current_env().get_recording_manager(p, create_if_absent=True))

  def test_evict_recording_manager(self):
    ie._interactive_beam_env = None
    ie.new_env()

    p = beam.Pipeline()
    self.assertFalse(ie.current_env().get_recording_manager(p))
    self.assertTrue(
        ie.current_env().get_recording_manager(p, create_if_absent=True))

  def test_describe_all_recordings(self):
    ie._interactive_beam_env = None
    ie.new_env()

    self.assertFalse(ie.current_env().describe_all_recordings())

    p1 = beam.Pipeline()
    p2 = beam.Pipeline()
    ie.current_env().watch(locals())
    ie.current_env().track_user_pipelines()
    rm1 = ie.current_env().get_recording_manager(p1, create_if_absent=True)
    rm2 = ie.current_env().get_recording_manager(p2, create_if_absent=True)

    description = ie.current_env().describe_all_recordings()
    self.assertTrue(description)

    expected_description = {p1: rm1.describe(), p2: rm2.describe()}
    self.assertDictEqual(description, expected_description)

  def test_get_empty_sql_chain(self):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    chain = env.get_sql_chain(p)
    self.assertIsNotNone(chain)
    self.assertEqual(chain.nodes, {})

  def test_get_sql_chain_with_nodes(self):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    chain_with_node = env.get_sql_chain(p).append(
        SqlNode(output_name='name', source=p, query="query"))
    chain_got = env.get_sql_chain(p)
    self.assertIs(chain_with_node, chain_got)

  def test_get_sql_chain_setting_user_pipeline(self):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    chain = env.get_sql_chain(p, set_user_pipeline=True)
    self.assertIs(chain.user_pipeline, p)

  def test_get_sql_chain_None_when_setting_multiple_user_pipelines(self):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    chain = env.get_sql_chain(p, set_user_pipeline=True)
    p2 = beam.Pipeline()
    # Set the chain for a different pipeline.
    env.sql_chain[p2] = chain
    with self.assertRaises(ValueError):
      env.get_sql_chain(p2, set_user_pipeline=True)

  @patch(
      'apache_beam.runners.interactive.interactive_environment.'
      'assert_bucket_exists',
      return_value=None)
  def test_get_gcs_cache_dir_valid_path(self, mock_assert_bucket_exists):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    cache_root = 'gs://test-cache-dir/'
    actual_cache_dir = env._get_gcs_cache_dir(p, cache_root)
    expected_cache_dir = 'gs://test-cache-dir/{}'.format(id(p))
    self.assertEqual(actual_cache_dir, expected_cache_dir)

  def test_get_gcs_cache_dir_invalid_path(self):
    env = ie.InteractiveEnvironment()
    p = beam.Pipeline()
    cache_root = 'gs://'
    with self.assertRaises(ValueError):
      env._get_gcs_cache_dir(p, cache_root)


if __name__ == '__main__':
  unittest.main()
