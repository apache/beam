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

"""Tests for apache_beam.runners.interactive.interactive_beam."""
# pytype: skip-file

from __future__ import absolute_import

import importlib
import sys
import time
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.options.capture_limiters import Limiter
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_stream import TestStream

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch

# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_beam_test'


def _get_watched_pcollections_with_variable_names():
  watched_pcollections = {}
  for watching in ie.current_env().watching():
    for key, val in watching:
      if hasattr(val, '__class__') and isinstance(val, beam.pvalue.PCollection):
        watched_pcollections[val] = key
  return watched_pcollections


class InteractiveBeamTest(unittest.TestCase):
  def setUp(self):
    self._var_in_class_instance = 'a var in class instance, not directly used'
    ie.new_env()

  def tearDown(self):
    ib.options.capture_control.set_limiters_for_test([])

  def test_watch_main_by_default(self):
    test_env = ie.InteractiveEnvironment()
    # Current Interactive Beam env fetched and the test env are 2 instances.
    self.assertNotEqual(id(ie.current_env()), id(test_env))
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_name(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(_module_name)
    test_env.watch(_module_name)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_module_object(self):
    test_env = ie.InteractiveEnvironment()
    module = importlib.import_module(_module_name)
    ib.watch(module)
    test_env.watch(module)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_locals(self):
    # test_env serves as local var too.
    test_env = ie.InteractiveEnvironment()
    ib.watch(locals())
    test_env.watch(locals())
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_class_instance(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(self)
    test_env.watch(self)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_always_watch_given_pcolls(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    # The pcoll is not watched since watch(locals()) is not explicitly called.
    self.assertFalse(pcoll in _get_watched_pcollections_with_variable_names())
    # The call of show watches pcoll.
    ib.watch({'p': p})
    ie.current_env().track_user_pipelines()
    ib.show(pcoll)
    self.assertTrue(pcoll in _get_watched_pcollections_with_variable_names())

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_mark_pcolls_computed_when_done(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    self.assertFalse(pcoll in ie.current_env().computed_pcollections)
    # The call of show marks pcoll computed.
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ib.show(pcoll)
    self.assertTrue(pcoll in ie.current_env().computed_pcollections)

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_handles_dict_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show({'pcoll': pcoll})
    mocked_visualize.assert_called_once()

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_handles_iterable_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show([pcoll])
    mocked_visualize.assert_called_once()

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_noop_when_pcoll_container_is_invalid(self, mocked_visualize):
    class SomeRandomClass:
      def __init__(self, pcoll):
        self._pcoll = pcoll

    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    self.assertRaises(ValueError, ib.show, SomeRandomClass(pcoll))
    mocked_visualize.assert_not_called()

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  def test_recordings_describe(self):
    """Tests that getting the description works."""

    # Create the pipelines to test.
    p1 = beam.Pipeline(ir.InteractiveRunner())
    p2 = beam.Pipeline(ir.InteractiveRunner())

    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Get the descriptions. This test is simple as there isn't much logic in the
    # method.
    self.assertEqual(ib.recordings.describe(p1)['size'], 0)
    self.assertEqual(ib.recordings.describe(p2)['size'], 0)

    all_descriptions = ib.recordings.describe()
    self.assertEqual(all_descriptions[p1]['size'], 0)
    self.assertEqual(all_descriptions[p2]['size'], 0)

    # Ensure that the variable name for the pipeline is set correctly.
    self.assertEqual(all_descriptions[p1]['pipeline_var'], 'p1')
    self.assertEqual(all_descriptions[p2]['pipeline_var'], 'p2')

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  def test_recordings_clear(self):
    """Tests that clearing the pipeline is correctly forwarded."""

    # Create a basic pipeline to store something in the cache.
    p = beam.Pipeline(ir.InteractiveRunner())
    elem = p | beam.Create([1])
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # This records the pipeline so that the cache size is > 0.
    ib.collect(elem)
    self.assertGreater(ib.recordings.describe(p)['size'], 0)

    # After clearing, the cache should be empty.
    ib.recordings.clear(p)
    self.assertEqual(ib.recordings.describe(p)['size'], 0)

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  def test_recordings_record(self):
    """Tests that recording pipeline succeeds."""

    # Add the TestStream so that it can be cached.
    ib.options.capturable_sources.add(TestStream)

    # Create a pipeline with an arbitrary amonunt of elements.
    p = beam.Pipeline(
        ir.InteractiveRunner(), options=PipelineOptions(streaming=True))
    # pylint: disable=unused-variable
    _ = (p
         | TestStream()
             .advance_watermark_to(0)
             .advance_processing_time(1)
             .add_elements(list(range(10)))
             .advance_processing_time(1))  # yapf: disable
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Assert that the pipeline starts in a good state.
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.STOPPED)
    self.assertEqual(ib.recordings.describe(p)['size'], 0)

    # Create a lmiter that stops the background caching job when something is
    # written to cache. This is used to make ensure that the pipeline is
    # functioning properly and that there are no data races with the test.
    class SizeLimiter(Limiter):
      def __init__(self, pipeline):
        self.pipeline = pipeline

      def is_triggered(self):
        return ib.recordings.describe(self.pipeline)['size'] > 0

    limiter = SizeLimiter(p)
    ib.options.capture_control.set_limiters_for_test([limiter])

    # Assert that a recording can be started only once.
    self.assertTrue(ib.recordings.record(p))
    self.assertFalse(ib.recordings.record(p))
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.RUNNING)

    # Wait for the pipeline to start and write something to cache.
    for _ in range(60):
      if limiter.is_triggered():
        break
      time.sleep(1)
    self.assertTrue(
        limiter.is_triggered(),
        'Test timed out waiting for limiter to be triggered. This indicates '
        'that the BackgroundCachingJob did not cache anything.')

    # Assert that a recording can be stopped and can't be started again until
    # after the cache is cleared.
    ib.recordings.stop(p)
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.STOPPED)
    self.assertFalse(ib.recordings.record(p))
    ib.recordings.clear(p)
    self.assertTrue(ib.recordings.record(p))
    ib.recordings.stop(p)


if __name__ == '__main__':
  unittest.main()
