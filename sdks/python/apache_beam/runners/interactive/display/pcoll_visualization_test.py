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

import pytz

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.display import pcoll_visualization as pv
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.windowed_value import PaneInfo
from apache_beam.utils.windowed_value import PaneInfoTiming

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch, ANY
except ImportError:
  from mock import patch, ANY  # type: ignore[misc]

try:
  import timeloop
except ImportError:
  pass


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@unittest.skipIf(
    sys.version_info < (3, 6), 'The tests require at least Python 3.6 to work.')
class PCollectionVisualizationTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()
    # Allow unit test to run outside of ipython kernel since we don't test the
    # frontend rendering in unit tests.
    pv._pcoll_visualization_ready = True
    # Generally test the logic where notebook is connected to the assumed
    # ipython kernel by forcefully setting notebook check to True.
    ie.current_env()._is_in_notebook = True
    ib.options.display_timezone = pytz.timezone('US/Pacific')

    self._p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    self._pcoll = self._p | 'Create' >> beam.Create(range(5))

    ib.watch(self)
    ie.current_env().track_user_pipelines()

    recording_manager = RecordingManager(self._p)
    recording = recording_manager.record([self._pcoll], 5, 5)
    self._stream = recording.stream(self._pcoll)

  def test_pcoll_visualization_generate_unique_display_id(self):
    pv_1 = pv.PCollectionVisualization(self._stream)
    pv_2 = pv.PCollectionVisualization(self._stream)
    self.assertNotEqual(pv_1._dive_display_id, pv_2._dive_display_id)
    self.assertNotEqual(pv_1._overview_display_id, pv_2._overview_display_id)
    self.assertNotEqual(pv_1._df_display_id, pv_2._df_display_id)

  def test_one_shot_visualization_not_return_handle(self):
    self.assertIsNone(pv.visualize(self._stream, display_facets=True))

  def test_dynamic_plotting_return_handle(self):
    h = pv.visualize(
        self._stream, dynamic_plotting_interval=1, display_facets=True)
    self.assertIsInstance(h, timeloop.Timeloop)
    h.stop()

  @patch(
      'apache_beam.runners.interactive.display.pcoll_visualization'
      '.PCollectionVisualization._display_dive')
  @patch(
      'apache_beam.runners.interactive.display.pcoll_visualization'
      '.PCollectionVisualization._display_overview')
  @patch(
      'apache_beam.runners.interactive.display.pcoll_visualization'
      '.PCollectionVisualization._display_dataframe')
  def test_dynamic_plotting_updates_same_display(
      self,
      mocked_display_dataframe,
      mocked_display_overview,
      mocked_display_dive):
    original_pcollection_visualization = pv.PCollectionVisualization(
        self._stream, display_facets=True)
    # Dynamic plotting always creates a new PCollectionVisualization.
    new_pcollection_visualization = pv.PCollectionVisualization(
        self._stream, display_facets=True)
    # The display uses ANY data the moment display is invoked, and updates
    # web elements with ids fetched from the given updating_pv.
    new_pcollection_visualization.display(
        updating_pv=original_pcollection_visualization)
    mocked_display_dataframe.assert_called_once_with(
        ANY, original_pcollection_visualization)
    # Below assertions are still true without newer calls.
    mocked_display_overview.assert_called_once_with(
        ANY, original_pcollection_visualization)
    mocked_display_dive.assert_called_once_with(
        ANY, original_pcollection_visualization)

  def test_auto_stop_dynamic_plotting_when_job_is_terminated(self):
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(self._p, fake_pipeline_result)
    # When job is running, the dynamic plotting will not be stopped.
    self.assertFalse(ie.current_env().is_terminated(self._p))

    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(self._p, fake_pipeline_result)
    # When job is done, the dynamic plotting will be stopped.
    self.assertTrue(ie.current_env().is_terminated(self._p))

  @patch('pandas.DataFrame.head')
  def test_display_plain_text_when_kernel_has_no_frontend(self, _mocked_head):
    # Resets the notebook check to False.
    ie.current_env()._is_in_notebook = False
    self.assertIsNone(pv.visualize(self._stream, display_facets=True))
    _mocked_head.assert_called_once()

  def test_event_time_formatter(self):
    # In microseconds: Monday, March 2, 2020 3:14:54 PM GMT-08:00
    event_time_us = 1583190894000000
    self.assertEqual(
        '2020-03-02 15:14:54.000000-0800',
        pv.event_time_formatter(event_time_us))

  def test_event_time_formatter_overflow_lower_bound(self):
    # A relatively small negative event time, which could be valid in Beam but
    # has no meaning when visualized.
    event_time_us = -100000000000000000
    self.assertEqual('Min Timestamp', pv.event_time_formatter(event_time_us))

  def test_event_time_formatter_overflow_upper_bound(self):
    # A relatively large event time, which exceeds the upper bound of unix time
    # Year 2038. It could mean infinite future in Beam but has no meaning
    # when visualized.
    # The value in test is supposed to be year 10000.
    event_time_us = 253402300800000000
    self.assertEqual('Max Timestamp', pv.event_time_formatter(event_time_us))

  def test_windows_formatter_global(self):
    gw = GlobalWindow()
    self.assertEqual(str(gw), pv.windows_formatter([gw]))

  def test_windows_formatter_interval(self):
    # The unit is second.
    iw = IntervalWindow(start=1583190894, end=1583200000)
    self.assertEqual(
        '2020-03-02 15:14:54.000000-0800 (2h 31m 46s)',
        pv.windows_formatter([iw]))

  def test_pane_info_formatter(self):
    self.assertEqual(
        'Pane 0: Final Early',
        pv.pane_info_formatter(
            PaneInfo(
                is_first=False,
                is_last=True,
                timing=PaneInfoTiming.EARLY,
                index=0,
                nonspeculative_index=0)))


if __name__ == '__main__':
  unittest.main()
