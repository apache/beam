#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import sys
import time
import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.interactive.options.capture_limiters import Limiter
from apache_beam.runners.interactive.recording_manager import ElementStream
from apache_beam.runners.interactive.recording_manager import Recording
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.runners.interactive.testing.test_cache_manager import FileRecordsBuilder
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream import WindowedValueHolder
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.windowed_value import WindowedValue

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import MagicMock
except ImportError:
  from mock import MagicMock  # type: ignore[misc]


class MockPipelineResult(beam.runners.runner.PipelineResult):
  """Mock class for controlling a PipelineResult."""
  def __init__(self):
    self._state = PipelineState.RUNNING

  def wait_until_finish(self):
    pass

  def set_state(self, state):
    self._state = state

  @property
  def state(self):
    return self._state

  def cancel(self):
    self._state = PipelineState.CANCELLED


class ElementStreamTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

    self.cache = InMemoryCache()
    self.p = beam.Pipeline()
    self.pcoll = self.p | beam.Create([])
    self.cache_key = str(pi.CacheKey('pcoll', '', '', ''))

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    self.mock_result = MockPipelineResult()
    ie.current_env().track_user_pipelines()
    ie.current_env().set_pipeline_result(self.p, self.mock_result)
    ie.current_env().set_cache_manager(self.cache, self.p)

  def test_read(self):
    """Test reading and if a stream is done no more elements are returned."""

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(['expected'], 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=1, max_duration_secs=1)

    self.assertFalse(stream.is_done())
    self.assertEqual(list(stream.read())[0], 'expected')
    self.assertTrue(stream.is_done())

  def test_done_if_terminated(self):
    """Test that terminating the job sets the stream as done."""

    self.cache.write(['expected'], 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=10)

    self.assertFalse(stream.is_done())
    self.assertEqual(list(stream.read(tail=False))[0], 'expected')

    # The limiters were not reached, so the stream is not done yet.
    self.assertFalse(stream.is_done())

    self.mock_result.set_state(PipelineState.DONE)
    self.assertEqual(list(stream.read(tail=False))[0], 'expected')

    # The underlying pipeline is terminated, so the stream won't yield new
    # elements.
    self.assertTrue(stream.is_done())

  def test_read_n(self):
    """Test that the stream only reads 'n' elements."""

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(list(range(5)), 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=1, max_duration_secs=1)
    self.assertEqual(list(stream.read()), [0])
    self.assertTrue(stream.is_done())

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=2, max_duration_secs=1)
    self.assertEqual(list(stream.read()), [0, 1])
    self.assertTrue(stream.is_done())

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=5, max_duration_secs=1)
    self.assertEqual(list(stream.read()), list(range(5)))
    self.assertTrue(stream.is_done())

    # Test that if the user asks for more than in the cache it still returns.
    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=10, max_duration_secs=1)
    self.assertEqual(list(stream.read()), list(range(5)))
    self.assertTrue(stream.is_done())

  def test_read_duration(self):
    """Test that the stream only reads a 'duration' of elements."""
    def as_windowed_value(element):
      return WindowedValueHolder(WindowedValue(element, 0, []))

    values = (FileRecordsBuilder(tag=self.cache_key)
              .advance_processing_time(1)
              .add_element(element=as_windowed_value(0), event_time_secs=0)
              .advance_processing_time(1)
              .add_element(element=as_windowed_value(1), event_time_secs=1)
              .advance_processing_time(1)
              .add_element(element=as_windowed_value(2), event_time_secs=3)
              .advance_processing_time(1)
              .add_element(element=as_windowed_value(3), event_time_secs=4)
              .advance_processing_time(1)
              .add_element(element=as_windowed_value(4), event_time_secs=5)
              .build()) # yapf: disable

    values = [
        v.recorded_event for v in values if isinstance(v, TestStreamFileRecord)
    ]

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(values, 'full', self.cache_key)
    self.cache.save_pcoder(coders.FastPrimitivesCoder(), 'full', self.cache_key)

    # The following tests a progression of reading different durations from the
    # cache.

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=1)
    self.assertSequenceEqual([e.value for e in stream.read()], [0])

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=2)
    self.assertSequenceEqual([e.value for e in stream.read()], [0, 1])

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=10)
    self.assertSequenceEqual([e.value for e in stream.read()], [0, 1, 2, 3, 4])


class RecordingTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_computed(self):
    """Tests that a PCollection is marked as computed only in a complete state.

    Because the background caching job is now long-lived, repeated runs of a
    PipelineFragment may yield different results for the same PCollection.
    """

    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    ib.watch(locals())

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    mock_result = MockPipelineResult()
    ie.current_env().track_user_pipelines()
    ie.current_env().set_pipeline_result(p, mock_result)

    # Create a mock BackgroundCachingJob that will control whether to set the
    # PCollections as computed or not.
    bcj_mock_result = MockPipelineResult()
    background_caching_job = bcj.BackgroundCachingJob(bcj_mock_result, [])

    # Create a recording.
    recording = Recording(
        p, [elems],
        mock_result,
        pi.PipelineInstrument(p),
        max_n=10,
        max_duration_secs=60)

    # The background caching job and the recording isn't done yet so there may
    # be more elements to be recorded.
    self.assertFalse(recording.is_computed())
    self.assertFalse(recording.computed())
    self.assertTrue(recording.uncomputed())

    # The recording is finished but the background caching job is not. There
    # may still be more elements to record, or the intermediate PCollection may
    # have stopped caching in an incomplete state, e.g. before a window could
    # fire.
    mock_result.set_state(PipelineState.DONE)
    recording.wait_until_finish()

    self.assertFalse(recording.is_computed())
    self.assertFalse(recording.computed())
    self.assertTrue(recording.uncomputed())

    # The background caching job finished before we started a recording which
    # is a sure signal that there will be no more elements.
    bcj_mock_result.set_state(PipelineState.DONE)
    ie.current_env().set_background_caching_job(p, background_caching_job)
    recording = Recording(
        p, [elems],
        mock_result,
        pi.PipelineInstrument(p),
        max_n=10,
        max_duration_secs=60)
    recording.wait_until_finish()

    # There are no more elements and the recording finished, meaning that the
    # intermediate PCollections are in a complete state. They can now be marked
    # as computed.
    self.assertTrue(recording.is_computed())
    self.assertTrue(recording.computed())
    self.assertFalse(recording.uncomputed())

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_describe(self):
    p = beam.Pipeline(InteractiveRunner())
    numbers = p | 'numbers' >> beam.Create([0, 1, 2])
    letters = p | 'letters' >> beam.Create(['a', 'b', 'c'])

    ib.watch(locals())

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    mock_result = MockPipelineResult()
    ie.current_env().track_user_pipelines()
    ie.current_env().set_pipeline_result(p, mock_result)

    cache_manager = InMemoryCache()
    ie.current_env().set_cache_manager(cache_manager, p)

    # Create a recording with an arbitrary start time.
    recording = Recording(
        p, [numbers, letters],
        mock_result,
        pi.PipelineInstrument(p),
        max_n=10,
        max_duration_secs=60)

    # Get the cache key of the stream and write something to cache. This is
    # so that a pipeline doesn't have to run in the test.
    numbers_stream = recording.stream(numbers)
    cache_manager.write([0, 1, 2], 'full', numbers_stream.cache_key)
    cache_manager.save_pcoder(None, 'full', numbers_stream.cache_key)

    letters_stream = recording.stream(letters)
    cache_manager.write(['a', 'b', 'c'], 'full', letters_stream.cache_key)
    cache_manager.save_pcoder(None, 'full', letters_stream.cache_key)

    # Get the description.
    description = recording.describe()
    size = description['size']

    self.assertEqual(
        size,
        cache_manager.size('full', numbers_stream.cache_key) +
        cache_manager.size('full', letters_stream.cache_key))


class RecordingManagerTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  def tearDown(self):
    ib.options.capture_control.set_limiters_for_test([])

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_basic_execution(self):
    """A basic pipeline to be used as a smoke test."""

    # Create the pipeline that will emit 0, 1, 2.
    p = beam.Pipeline(InteractiveRunner())
    numbers = p | 'numbers' >> beam.Create([0, 1, 2])
    letters = p | 'letters' >> beam.Create(['a', 'b', 'c'])

    # Watch the pipeline and PCollections. This is normally done in a notebook
    # environment automatically, but we have to do it manually here.
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Create the recording objects. By calling `record` a new PipelineFragment
    # is started to compute the given PCollections and cache to disk.
    rm = RecordingManager(p)
    numbers_recording = rm.record([numbers], max_n=3, max_duration=500)
    numbers_stream = numbers_recording.stream(numbers)
    numbers_recording.wait_until_finish()

    # Once the pipeline fragment completes, we can read from the stream and know
    # that all elements were written to cache.
    elems = list(numbers_stream.read())
    expected_elems = [
        WindowedValue(i, MIN_TIMESTAMP, [GlobalWindow()]) for i in range(3)
    ]
    self.assertListEqual(elems, expected_elems)

    # Make an extra recording and test the description.
    letters_recording = rm.record([letters], max_n=3, max_duration=500)
    letters_recording.wait_until_finish()

    self.assertEqual(
        rm.describe()['size'],
        numbers_recording.describe()['size'] +
        letters_recording.describe()['size'])

    rm.cancel()

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_duration_parsing(self):
    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    # Watch the pipeline and PCollections. This is normally done in a notebook
    # environment automatically, but we have to do it manually here.
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Create the recording objects.
    rm = RecordingManager(p)
    recording = rm.record([elems], max_n=3, max_duration='500s')
    recording.wait_until_finish()

    # Assert that the duration was parsed correctly to integer seconds.
    self.assertEqual(recording.describe()['duration'], 500)

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_cancel_stops_recording(self):
    # Add the TestStream so that it can be cached.
    ib.options.capturable_sources.add(TestStream)

    p = beam.Pipeline(
        InteractiveRunner(), options=PipelineOptions(streaming=True))
    elems = (
        p
        | TestStream().advance_watermark_to(0).advance_processing_time(
            1).add_elements(list(range(10))).advance_processing_time(1))
    squares = elems | beam.Map(lambda x: x**2)

    # Watch the local scope for Interactive Beam so that referenced PCollections
    # will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    # Get the recording then the BackgroundCachingJob.
    rm = RecordingManager(p)
    recording = rm.record([squares], max_n=10, max_duration=30)

    # The BackgroundCachingJob is still waiting for more elements, so it isn't
    # done yet.
    bcj = ie.current_env().get_background_caching_job(p)
    self.assertFalse(bcj.is_done())

    # Assert that something was read and that the BackgroundCachingJob was
    # sucessfully stopped.
    self.assertTrue(list(recording.stream(squares).read()))
    rm.cancel()
    self.assertTrue(bcj.is_done())

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_recording_manager_clears_cache(self):
    """Tests that the RecordingManager clears the cache before recording.

    A job may have incomplete PCollections when the job terminates. Clearing the
    cache ensures that correct results are computed every run.
    """
    # Add the TestStream so that it can be cached.
    ib.options.capturable_sources.add(TestStream)
    p = beam.Pipeline(
        InteractiveRunner(), options=PipelineOptions(streaming=True))
    elems = (
        p
        | TestStream().advance_watermark_to(0).advance_processing_time(
            1).add_elements(list(range(10))).advance_processing_time(1))
    squares = elems | beam.Map(lambda x: x**2)

    # Watch the local scope for Interactive Beam so that referenced PCollections
    # will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    # Do the first recording to get the timestamp of the first time the fragment
    # was run.
    rm = RecordingManager(p)
    rm.record([squares], max_n=10, max_duration=2)
    first_recording_start = rm.describe()['start']
    rm.cancel()

    # Get the cache, key, and coder to read the PCollection from the cache.
    pipeline_instrument = pi.PipelineInstrument(p)
    cache = ie.current_env().get_cache_manager(p)
    cache_key = pipeline_instrument.cache_key(squares)

    # Set up a mock for the Cache's clear function which will be used to clear
    # uncomputed PCollections.
    cache.clear = MagicMock()

    # Rerun the fragment. If the cache was cleared correctly then the starting
    # time of the second recording will be later than the first. This is because
    # the PCollection wasn't considered to be computedand was cleared from
    # cache. Thus the pipeline fragment was rerun for that PCollection at a
    # later time.
    rm.record([squares], max_n=10, max_duration=1)
    second_recording_start = rm.describe()['start']
    rm.cancel()
    self.assertGreater(second_recording_start, first_recording_start)

    # Assert that the cache cleared the PCollection.
    cache.clear.assert_called_with('full', cache_key)

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_clear(self):
    """Tests that clear can empty the cache for a specific pipeline."""

    # Create two pipelines so we can check that clearing the cache won't clear
    # all defined pipelines.
    p1 = beam.Pipeline(InteractiveRunner())
    elems_1 = p1 | 'elems 1' >> beam.Create([0, 1, 2])

    p2 = beam.Pipeline(InteractiveRunner())
    elems_2 = p2 | 'elems 2' >> beam.Create([0, 1, 2])

    # Watch the pipeline and PCollections. This is normally done in a notebook
    # environment automatically, but we have to do it manually here.
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Create the recording objects. By calling `record` a new PipelineFragment
    # is started to compute the given PCollections and cache to disk.
    rm_1 = RecordingManager(p1)
    recording = rm_1.record([elems_1], max_n=3, max_duration=500)
    recording.wait_until_finish()

    rm_2 = RecordingManager(p2)
    recording = rm_2.record([elems_2], max_n=3, max_duration=500)
    recording.wait_until_finish()

    # Assert that clearing only one recording clears that recording.
    self.assertGreater(rm_1.describe()['size'], 0)
    self.assertGreater(rm_2.describe()['size'], 0)
    rm_1.clear()
    self.assertEqual(rm_1.describe()['size'], 0)
    self.assertGreater(rm_2.describe()['size'], 0)

    rm_2.clear()
    self.assertEqual(rm_2.describe()['size'], 0)

  @unittest.skipIf(
      sys.version_info < (3, 6, 0),
      'This test requires at least Python 3.6 to work.')
  def test_record_pipeline(self):
    # Add the TestStream so that it can be cached.
    ib.options.capturable_sources.add(TestStream)
    p = beam.Pipeline(
        InteractiveRunner(), options=PipelineOptions(streaming=True))
    # pylint: disable=unused-variable
    _ = (p
         | TestStream()
             .advance_watermark_to(0)
             .advance_processing_time(1)
             .add_elements(list(range(10)))
             .advance_processing_time(1))  # yapf: disable

    # Watch the local scope for Interactive Beam so that referenced PCollections
    # will be cached.
    ib.watch(locals())

    # This is normally done in the interactive_utils when a transform is
    # applied but needs an IPython environment. So we manually run this here.
    ie.current_env().track_user_pipelines()

    # Create a lmiter that stops the background caching job when something is
    # written to cache. This is used to make ensure that the pipeline is
    # functioning properly and that there are no data races with the test.
    class SizeLimiter(Limiter):
      def __init__(self, recording_manager):
        self.recording_manager = recording_manager

      def is_triggered(self):
        return self.recording_manager.describe()['size'] > 0

    # Do the first recording to get the timestamp of the first time the fragment
    # was run.
    rm = RecordingManager(p)

    ib.options.capture_control.set_limiters_for_test([SizeLimiter(rm)])

    self.assertEqual(rm.describe()['state'], PipelineState.STOPPED)
    self.assertTrue(rm.record_pipeline())
    self.assertFalse(rm.record_pipeline())

    for _ in range(60):
      if rm.describe()['state'] == PipelineState.CANCELLED:
        break
      time.sleep(1)
    self.assertTrue(
        rm.describe()['state'] == PipelineState.CANCELLED,
        'Test timed out waiting for pipeline to be cancelled. This indicates '
        'that the BackgroundCachingJob did not cache anything.')


if __name__ == '__main__':
  unittest.main()
