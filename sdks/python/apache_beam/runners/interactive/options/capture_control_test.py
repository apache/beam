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

import unittest
from unittest.mock import patch

import apache_beam as beam
from apache_beam import coders
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners import runner
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.runners.interactive.options import capture_control
from apache_beam.runners.interactive.options import capture_limiters
from apache_beam.testing.test_stream_service import TestStreamServiceController


def _build_an_empty_streaming_pipeline():
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import StandardOptions
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(
      interactive_runner.InteractiveRunner(), options=pipeline_options)
  ib.watch({'pipeline': p})
  return p


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

    background_caching_job = bcj.BackgroundCachingJob(
        runner.PipelineResult(runner.PipelineState.RUNNING), limiters=[])
    ie.current_env().set_background_caching_job(p, background_caching_job)

    _fake_a_running_test_stream_service(p)
    # Fake the canceling state of the main job.
    background_caching_job._pipeline_result = runner.PipelineResult(
        runner.PipelineState.CANCELLING)
    self.assertIsNotNone(ie.current_env().get_test_stream_service_controller(p))
    ie.current_env().set_cached_source_signature(p, 'a signature')
    ie.current_env().mark_pcollection_computed(['fake_pcoll'])
    capture_control.evict_captured_data()
    mocked_background_caching_job_cancel.assert_called()
    mocked_test_stream_service_stop.assert_called_once()
    # Neither timer nor capture size limit is reached, thus, the cancelling
    # main job's background caching job is not considered as done.
    self.assertFalse(background_caching_job.is_done())
    self.assertIsNone(ie.current_env().get_test_stream_service_controller(p))
    self.assertTrue(ie.current_env().computed_pcollections == set())
    self.assertTrue(ie.current_env().get_cached_source_signature(p) == set())

  def test_capture_size_limit_not_reached_when_no_cache(self):
    self.assertEqual(len(ie.current_env()._cache_managers), 0)
    limiter = capture_limiters.SizeLimiter(1)
    self.assertFalse(limiter.is_triggered())

  def test_capture_size_limit_not_reached_when_no_file(self):
    cache = StreamingCache(cache_dir=None)
    self.assertFalse(cache.exists('my_label'))
    ie.current_env().set_cache_manager(cache, 'dummy pipeline')

    limiter = capture_limiters.SizeLimiter(1)
    self.assertFalse(limiter.is_triggered())

  def test_capture_size_limit_not_reached_when_file_size_under_limit(self):
    ib.options.capture_size_limit = 100
    cache = StreamingCache(cache_dir=None)
    # Build a sink object to track the label as a capture in the test.
    cache.sink(['my_label'], is_capture=True)
    cache.write([TestStreamFileRecord()], 'my_label')
    self.assertTrue(cache.exists('my_label'))
    ie.current_env().set_cache_manager(cache, 'dummy pipeline')

    limiter = capture_limiters.SizeLimiter(ib.options.capture_size_limit)
    self.assertFalse(limiter.is_triggered())

  def test_capture_size_limit_reached_when_file_size_above_limit(self):
    ib.options.capture_size_limit = 1
    cache = StreamingCache(cache_dir=None)
    cache.sink(['my_label'], is_capture=True)
    cache.write([
        TestStreamFileRecord(
            recorded_event=TestStreamPayload.Event(
                element_event=TestStreamPayload.Event.AddElements(
                    elements=[
                        TestStreamPayload.TimestampedElement(
                            encoded_element=coders.FastPrimitivesCoder().encode(
                                'a'),
                            timestamp=0)
                    ])))
    ],
                'my_label')
    self.assertTrue(cache.exists('my_label'))
    p = _build_an_empty_streaming_pipeline()
    ie.current_env().set_cache_manager(cache, p)

    limiter = capture_limiters.SizeLimiter(1)
    self.assertTrue(limiter.is_triggered())

  def test_timer_terminates_capture_size_checker(self):
    p = _build_an_empty_streaming_pipeline()

    class FakeLimiter(capture_limiters.Limiter):
      def __init__(self):
        self.trigger = False

      def is_triggered(self):
        return self.trigger

    limiter = FakeLimiter()
    background_caching_job = bcj.BackgroundCachingJob(
        runner.PipelineResult(runner.PipelineState.CANCELLING),
        limiters=[limiter])
    ie.current_env().set_background_caching_job(p, background_caching_job)

    self.assertFalse(background_caching_job.is_done())

    limiter.trigger = True
    self.assertTrue(background_caching_job.is_done())


if __name__ == '__main__':
  unittest.main()
