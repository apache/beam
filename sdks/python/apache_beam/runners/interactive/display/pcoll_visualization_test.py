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
# pytype: skip-file

from __future__ import absolute_import

import sys
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
  from unittest.mock import patch, ANY
except ImportError:
  from mock import patch, ANY

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
         '.PCollectionVisualization._display_dive')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollectionVisualization._display_overview')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollectionVisualization._display_dataframe')
  def test_dynamic_plotting_updates_same_display(self,
                                                 mocked_display_dataframe,
                                                 mocked_display_overview,
                                                 mocked_display_dive):
    original_pcollection_visualization = pv.PCollectionVisualization(
        self._pcoll)
    # Dynamic plotting always creates a new PCollectionVisualization.
    new_pcollection_visualization = pv.PCollectionVisualization(self._pcoll)
    # The display uses ANY data the moment display is invoked, and updates
    # web elements with ids fetched from the given updating_pv.
    new_pcollection_visualization.display_facets(
        updating_pv=original_pcollection_visualization)
    mocked_display_dataframe.assert_called_once_with(
        ANY, original_pcollection_visualization._df_display_id)
    mocked_display_overview.assert_called_once_with(
        ANY, original_pcollection_visualization._overview_display_id)
    mocked_display_dive.assert_called_once_with(
        ANY, original_pcollection_visualization._dive_display_id)

  def test_auto_stop_dynamic_plotting_when_job_is_terminated(self):
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(
        self._p,
        fake_pipeline_result,
        is_main_job=True)
    # When job is running, the dynamic plotting will not be stopped.
    self.assertFalse(ie.current_env().is_terminated(self._p))

    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(
        self._p,
        fake_pipeline_result,
        is_main_job=True)
    # When job is done, the dynamic plotting will be stopped.
    self.assertTrue(ie.current_env().is_terminated(self._p))

  @patch('pandas.DataFrame.sample')
  def test_display_plain_text_when_kernel_has_no_frontend(self,
                                                          _mocked_sample):
    # Resets the notebook check to False.
    ie.current_env()._is_in_notebook = False
    self.assertIsNone(pv.visualize(self._pcoll))
    _mocked_sample.assert_called_once()


if __name__ == '__main__':
  unittest.main()
