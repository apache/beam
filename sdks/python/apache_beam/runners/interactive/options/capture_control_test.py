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

"""Tests for apache_beam.runners.interactive.options.capture_control."""

# pytype: skip-file

from __future__ import absolute_import

import sys
import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive.options import capture_control
from apache_beam.testing.test_stream_service import TestStreamServiceController

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
  from unittest.mock import MagicMock
except ImportError:
  from mock import patch
  from mock import MagicMock


def _build_an_empty_streaming_pipeline():
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import StandardOptions
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(
      interactive_runner.InteractiveRunner(), options=pipeline_options)
  ib.watch({'pipeline': p})
  return p


def _fake_a_running_background_caching_job(pipeline):
  background_caching_job = bcj.BackgroundCachingJob(
      runner.PipelineResult(runner.PipelineState.RUNNING),
      # Do not start multithreaded checkers in tests.
      start_limit_checkers=False)
  ie.current_env().set_background_caching_job(pipeline, background_caching_job)
  return background_caching_job


def _fake_a_running_test_stream_service(pipeline):
  class FakeReader:
    def read_multiple(self):
      yield 1

  test_stream_service = TestStreamServiceController(FakeReader())
  test_stream_service.start()
  ie.current_env().set_test_stream_service_controller(
      pipeline, test_stream_service)


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@unittest.skipIf(
    sys.version_info < (3, 6), 'The tests require at least Python 3.6 to work.')
class CaptureControlTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @patch(
      'apache_beam.runners.interactive.background_caching_job'
      '.BackgroundCachingJob.cancel')
  @patch(
      'apache_beam.testing.test_stream_service.TestStreamServiceController'
      '.stop')
  def test_capture_control_evict_captured_data(
      self,
      mocked_test_stream_service_stop,
      mocked_background_caching_job_cancel):
    p = _build_an_empty_streaming_pipeline()
    ie.current_env().track_user_pipelines()
    self.assertFalse(ie.current_env().tracked_user_pipelines == set())
    background_caching_job = _fake_a_running_background_caching_job(p)
    # Makes sure the background caching job is tracked.
    self.assertIsNotNone(ie.current_env().get_background_caching_job(p))
    _fake_a_running_test_stream_service(p)
    # Fake the canceling state of the main job.
    background_caching_job._pipeline_result = runner.PipelineResult(
        runner.PipelineState.CANCELLING)
    self.assertIsNotNone(ie.current_env().get_test_stream_service_controller(p))
    ie.current_env().set_cached_source_signature(p, 'a signature')
    ie.current_env().mark_pcollection_computed(['fake_pcoll'])
    capture_control.evict_captured_data()
    mocked_background_caching_job_cancel.assert_called_once()
    mocked_test_stream_service_stop.assert_called_once()
    # Neither timer nor capture size limit is reached, thus, the cancelling
    # main job's background caching job is not considered as done.
    self.assertFalse(background_caching_job.is_done())
    self.assertIsNone(ie.current_env().get_test_stream_service_controller(p))
    self.assertTrue(ie.current_env().computed_pcollections == set())
    self.assertTrue(ie.current_env().get_cached_source_signature(p) == set())

  def test_capture_size_not_reached_when_no_cache(self):
    self.assertIsNone(ie.current_env().cache_manager())
    self.assertFalse(
        ie.current_env().options.capture_control.is_capture_size_reached())

  def test_capture_size_not_reached_when_no_file(self):
    _ = _build_an_empty_streaming_pipeline()
    self.assertIsNotNone(ie.current_env().cache_manager())
    self.assertFalse(
        ie.current_env().options.capture_control.is_capture_size_reached())

  # TODO(BEAM-8335): add more capture_size tests when the property is
  # implemented in streaming_cache.

  def test_timer_terminates_capture_size_checker(self):
    p = _build_an_empty_streaming_pipeline()
    background_caching_job = _fake_a_running_background_caching_job(p)
    background_caching_job._timer = MagicMock()
    background_caching_job._timer.is_alive.return_value = True
    self.assertFalse(background_caching_job._should_end_condition_checker())
    background_caching_job._timer.is_alive.return_value = False
    self.assertFalse(background_caching_job._timer.is_alive())
    self.assertTrue(background_caching_job._should_end_condition_checker())


if __name__ == '__main__':
  unittest.main()
