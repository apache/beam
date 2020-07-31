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

"""Tests for apache_beam.runners.interactive.background_caching_job."""
# pytype: skip-file

from __future__ import absolute_import

import sys
import unittest

import apache_beam as beam
from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners import runner
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython
from apache_beam.runners.interactive.testing.test_cache_manager import FileRecordsBuilder
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream_service import TestStreamServiceController
from apache_beam.transforms.window import TimestampedValue

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch  # type: ignore[misc]

_FOO_PUBSUB_SUB = 'projects/test-project/subscriptions/foo'
_BAR_PUBSUB_SUB = 'projects/test-project/subscriptions/bar'
_TEST_CACHE_KEY = 'test'


def _build_a_test_stream_pipeline():
  test_stream = (
      TestStream().advance_watermark_to(0).add_elements([
          TimestampedValue('a', 1)
      ]).advance_processing_time(5).advance_watermark_to_infinity())
  p = beam.Pipeline(runner=interactive_runner.InteractiveRunner())
  events = p | test_stream  # pylint: disable=possibly-unused-variable
  ib.watch(locals())
  return p


def _build_an_empty_stream_pipeline():
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import StandardOptions
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(
      interactive_runner.InteractiveRunner(), options=pipeline_options)
  ib.watch({'pipeline': p})
  return p


def _setup_test_streaming_cache(pipeline):
  cache_manager = StreamingCache(cache_dir=None)
  ie.current_env().set_cache_manager(cache_manager, pipeline)
  builder = FileRecordsBuilder(tag=_TEST_CACHE_KEY)
  (builder
      .advance_watermark(watermark_secs=0)
      .advance_processing_time(5)
      .add_element(element='a', event_time_secs=1)
      .advance_watermark(watermark_secs=100)
      .advance_processing_time(10)) # yapf: disable
  cache_manager.write(builder.build(), _TEST_CACHE_KEY)


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@unittest.skipIf(
    sys.version_info < (3, 6), 'The tests require at least Python 3.6 to work.')
class BackgroundCachingJobTest(unittest.TestCase):
  def tearDown(self):
    for _, job in ie.current_env()._background_caching_jobs.items():
      job.cancel()
    ie.new_env()

  # TODO(BEAM-8335): remove the patches when there are appropriate test sources
  # that meet the boundedness checks.
  @patch(
      'apache_beam.runners.interactive.background_caching_job'
      '.has_source_to_cache',
      lambda x: True)
  # Disable the clean up so that we can keep the test streaming cache.
  @patch(
      'apache_beam.runners.interactive.interactive_environment'
      '.InteractiveEnvironment.cleanup',
      lambda x: None)
  def test_background_caching_job_starts_when_none_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    _setup_test_streaming_cache(p)
    p.run()
    self.assertIsNotNone(ie.current_env().get_background_caching_job(p))
    expected_cached_source_signature = bcj.extract_source_to_cache_signature(p)
    # This is to check whether the cached source signature is set correctly
    # when the background caching job is started.
    self.assertEqual(
        expected_cached_source_signature,
        ie.current_env().get_cached_source_signature(p))

  @patch(
      'apache_beam.runners.interactive.background_caching_job'
      '.has_source_to_cache',
      lambda x: False)
  def test_background_caching_job_not_start_for_batch_pipeline(self):
    p = _build_a_test_stream_pipeline()
    p.run()
    self.assertIsNone(ie.current_env().get_background_caching_job(p))

  @patch(
      'apache_beam.runners.interactive.background_caching_job'
      '.has_source_to_cache',
      lambda x: True)
  # Disable the clean up so that we can keep the test streaming cache.
  @patch(
      'apache_beam.runners.interactive.interactive_environment'
      '.InteractiveEnvironment.cleanup',
      lambda x: None)
  def test_background_caching_job_not_start_when_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    _setup_test_streaming_cache(p)
    a_running_background_caching_job = bcj.BackgroundCachingJob(
        runner.PipelineResult(runner.PipelineState.RUNNING), limiters=[])
    ie.current_env().set_background_caching_job(
        p, a_running_background_caching_job)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(
        a_running_background_caching_job,
        ie.current_env().get_background_caching_job(p))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result, ie.current_env().pipeline_result(p))

  @patch(
      'apache_beam.runners.interactive.background_caching_job'
      '.has_source_to_cache',
      lambda x: True)
  # Disable the clean up so that we can keep the test streaming cache.
  @patch(
      'apache_beam.runners.interactive.interactive_environment'
      '.InteractiveEnvironment.cleanup',
      lambda x: None)
  def test_background_caching_job_not_start_when_such_job_is_done(self):
    p = _build_a_test_stream_pipeline()
    _setup_test_streaming_cache(p)
    a_done_background_caching_job = bcj.BackgroundCachingJob(
        runner.PipelineResult(runner.PipelineState.DONE), limiters=[])
    ie.current_env().set_background_caching_job(
        p, a_done_background_caching_job)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(
        a_done_background_caching_job,
        ie.current_env().get_background_caching_job(p))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result, ie.current_env().pipeline_result(p))

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_source_to_cache_changed_when_pipeline_is_first_time_seen(self, cell):
    with cell:  # Cell 1
      pipeline = _build_an_empty_stream_pipeline()

    with cell:  # Cell 2
      read_foo = pipeline | 'Read' >> beam.io.ReadFromPubSub(
          subscription=_FOO_PUBSUB_SUB)
      ib.watch({'read_foo': read_foo})

    self.assertTrue(bcj.is_source_to_cache_changed(pipeline))

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_source_to_cache_changed_when_new_source_is_added(self, cell):
    with cell:  # Cell 1
      pipeline = _build_an_empty_stream_pipeline()
      read_foo = pipeline | 'Read' >> beam.io.ReadFromPubSub(
          subscription=_FOO_PUBSUB_SUB)
      ib.watch({'read_foo': read_foo})

    # Sets the signature for current pipeline state.
    ie.current_env().set_cached_source_signature(
        pipeline, bcj.extract_source_to_cache_signature(pipeline))

    with cell:  # Cell 2
      read_bar = pipeline | 'Read' >> beam.io.ReadFromPubSub(
          subscription=_BAR_PUBSUB_SUB)
      ib.watch({'read_bar': read_bar})

    self.assertTrue(bcj.is_source_to_cache_changed(pipeline))

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_source_to_cache_changed_when_source_is_altered(self, cell):
    with cell:  # Cell 1
      pipeline = _build_an_empty_stream_pipeline()
      transform = beam.io.ReadFromPubSub(subscription=_FOO_PUBSUB_SUB)
      read_foo = pipeline | 'Read' >> transform
      ib.watch({'read_foo': read_foo})

    # Sets the signature for current pipeline state.
    ie.current_env().set_cached_source_signature(
        pipeline, bcj.extract_source_to_cache_signature(pipeline))

    with cell:  # Cell 2
      from apache_beam.io.gcp.pubsub import _PubSubSource
      # Alter the transform.
      transform._source = _PubSubSource(subscription=_BAR_PUBSUB_SUB)

    self.assertTrue(bcj.is_source_to_cache_changed(pipeline))

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_source_to_cache_not_changed_for_same_source(self, cell):
    with cell:  # Cell 1
      pipeline = _build_an_empty_stream_pipeline()
      transform = beam.io.ReadFromPubSub(subscription=_FOO_PUBSUB_SUB)

    with cell:  # Cell 2
      read_foo_1 = pipeline | 'Read' >> transform
      ib.watch({'read_foo_1': read_foo_1})

    # Sets the signature for current pipeline state.
    ie.current_env().set_cached_source_signature(
        pipeline, bcj.extract_source_to_cache_signature(pipeline))

    with cell:  # Cell 3
      # Apply exactly the same transform and the same instance.
      read_foo_2 = pipeline | 'Read' >> transform
      ib.watch({'read_foo_2': read_foo_2})

    self.assertFalse(bcj.is_source_to_cache_changed(pipeline))

    with cell:  # Cell 4
      # Apply the same transform but represented in a different instance.
      # The signature representing the urn and payload is still the same, so it
      # is not treated as a new unbounded source.
      read_foo_3 = pipeline | 'Read' >> beam.io.ReadFromPubSub(
          subscription=_FOO_PUBSUB_SUB)
      ib.watch({'read_foo_3': read_foo_3})

    self.assertFalse(bcj.is_source_to_cache_changed(pipeline))

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_source_to_cache_not_changed_when_source_is_removed(self, cell):
    with cell:  # Cell 1
      pipeline = _build_an_empty_stream_pipeline()
      foo_transform = beam.io.ReadFromPubSub(subscription=_FOO_PUBSUB_SUB)
      bar_transform = beam.io.ReadFromPubSub(subscription=_BAR_PUBSUB_SUB)

    with cell:  # Cell 2
      read_foo = pipeline | 'Read' >> foo_transform
      ib.watch({'read_foo': read_foo})

    signature_with_only_foo = bcj.extract_source_to_cache_signature(pipeline)

    with cell:  # Cell 3
      read_bar = pipeline | 'Read' >> bar_transform
      ib.watch({'read_bar': read_bar})

    self.assertTrue(bcj.is_source_to_cache_changed(pipeline))
    signature_with_foo_bar = ie.current_env().get_cached_source_signature(
        pipeline)
    self.assertNotEqual(signature_with_only_foo, signature_with_foo_bar)

    class BarPruneVisitor(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        pruned_parts = list(transform_node.parts)
        for part in transform_node.parts:
          if part.transform is bar_transform:
            pruned_parts.remove(part)
        transform_node.parts = tuple(pruned_parts)
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node.transform is bar_transform:
          transform_node.parent = None

    v = BarPruneVisitor()
    pipeline.visit(v)

    signature_after_pruning_bar = bcj.extract_source_to_cache_signature(
        pipeline)
    self.assertEqual(signature_with_only_foo, signature_after_pruning_bar)
    self.assertFalse(bcj.is_source_to_cache_changed(pipeline))

  def test_determine_a_test_stream_service_running(self):
    pipeline = _build_an_empty_stream_pipeline()
    test_stream_service = TestStreamServiceController(reader=None)
    ie.current_env().set_test_stream_service_controller(
        pipeline, test_stream_service)
    self.assertTrue(bcj.is_a_test_stream_service_running(pipeline))

  def test_stop_a_running_test_stream_service(self):
    pipeline = _build_an_empty_stream_pipeline()
    test_stream_service = TestStreamServiceController(reader=None)
    test_stream_service.start()
    ie.current_env().set_test_stream_service_controller(
        pipeline, test_stream_service)
    bcj.attempt_to_stop_test_stream_service(pipeline)
    self.assertFalse(bcj.is_a_test_stream_service_running(pipeline))

  @patch(
      'apache_beam.testing.test_stream_service.TestStreamServiceController'
      '.stop')
  def test_noop_when_no_test_stream_service_running(self, _mocked_stop):
    pipeline = _build_an_empty_stream_pipeline()
    self.assertFalse(bcj.is_a_test_stream_service_running(pipeline))
    bcj.attempt_to_stop_test_stream_service(pipeline)
    _mocked_stop.assert_not_called()


if __name__ == '__main__':
  unittest.main()
