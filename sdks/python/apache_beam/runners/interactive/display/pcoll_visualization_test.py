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

"""Tests for apache_beam.runners.interactive.display.pcoll_visualization."""
from __future__ import absolute_import

import sys
import time
import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.display import pcoll_visualization as pv

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch

try:
  import timeloop
except ImportError:
  pass


@unittest.skipIf(not ie.current_env().is_interactive_ready,
                 '[interactive] dependency is not installed.')
@unittest.skipIf(sys.version_info < (3, 6),
                 'The tests require at least Python 3.6 to work.')
class PCollectionVisualizationTest(unittest.TestCase):

  def setUp(self):
    ie.new_env()
    # Allow unit test to run outside of ipython kernel since we don't test the
    # frontend rendering in unit tests.
    pv._pcoll_visualization_ready = True
    # Generally test the logic where notebook is connected to the assumed
    # ipython kernel by forcefully setting notebook check to True.
    ie.current_env()._is_in_notebook = True

    self._p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    self._pcoll = self._p | 'Create' >> beam.Create(range(5))
    ib.watch(self)
    self._p.run()

  def test_raise_error_for_non_pcoll_input(self):
    class Foo(object):
      pass

    with self.assertRaises(AssertionError) as ctx:
      pv.PCollectionVisualization(Foo())
      self.assertTrue('pcoll should be apache_beam.pvalue.PCollection' in
                      ctx.exception)

  def test_pcoll_visualization_generate_unique_display_id(self):
    pv_1 = pv.PCollectionVisualization(self._pcoll)
    pv_2 = pv.PCollectionVisualization(self._pcoll)
    self.assertNotEqual(pv_1._dive_display_id, pv_2._dive_display_id)
    self.assertNotEqual(pv_1._overview_display_id, pv_2._overview_display_id)
    self.assertNotEqual(pv_1._df_display_id, pv_2._df_display_id)

  def test_one_shot_visualization_not_return_handle(self):
    self.assertIsNone(pv.visualize(self._pcoll))

  def test_dynamic_plotting_return_handle(self):
    h = pv.visualize(self._pcoll, dynamic_plotting_interval=1)
    self.assertIsInstance(h, timeloop.Timeloop)
    h.stop()

  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollectionVisualization.display_facets')
  def test_dynamic_plotting_update_same_display(self,
                                                mocked_display_facets):
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(
        self._p,
        fake_pipeline_result,
        is_main_job=True)
    # Starts async dynamic plotting that never ends in this test.
    h = pv.visualize(self._pcoll, dynamic_plotting_interval=0.001)
    # Blocking so the above async task can execute some iterations.
    time.sleep(1)
    # The first iteration doesn't provide updating_pv to display_facets.
    _, first_kwargs = mocked_display_facets.call_args_list[0]
    self.assertEqual(first_kwargs, {})
    # The following iterations use the same updating_pv to display_facets and so
    # on.
    _, second_kwargs = mocked_display_facets.call_args_list[1]
    updating_pv = second_kwargs['updating_pv']
    for call in mocked_display_facets.call_args_list[2:]:
      _, kwargs = call
      self.assertIs(kwargs['updating_pv'], updating_pv)
    h.stop()

  @patch('timeloop.Timeloop.stop')
  def test_auto_stop_dynamic_plotting_when_job_is_terminated(
      self,
      mocked_timeloop):
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(
        self._p,
        fake_pipeline_result,
        is_main_job=True)
    # Starts non-stopping async dynamic plotting until the job is terminated.
    pv.visualize(self._pcoll, dynamic_plotting_interval=0.001)
    # Blocking so the above async task can execute some iterations.
    time.sleep(1)
    mocked_timeloop.assert_not_called()
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(
        self._p,
        fake_pipeline_result,
        is_main_job=True)
    # Blocking so the above async task can execute some iterations.
    time.sleep(1)
    # "assert_called" is new in Python 3.6.
    mocked_timeloop.assert_called()

  @patch('pandas.DataFrame.sample')
  def test_display_plain_text_when_kernel_has_no_frontend(self,
                                                          _mocked_sample):
    # Resets the notebook check to False.
    ie.current_env()._is_in_notebook = False
    self.assertIsNone(pv.visualize(self._pcoll))
    _mocked_sample.assert_called_once()


if __name__ == '__main__':
  unittest.main()
