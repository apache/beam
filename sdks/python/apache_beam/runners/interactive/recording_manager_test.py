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

import time
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.caching.cacheable import CacheKey
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.interactive.options.capture_limiters import Limiter
from apache_beam.runners.interactive.recording_manager import _LOGGER
from apache_beam.runners.interactive.recording_manager import AsyncComputationResult
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


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
class AsyncComputationResultTest(unittest.TestCase):
  def setUp(self):
    self.mock_future = MagicMock(spec=Future)
    self.pcolls = {MagicMock(spec=beam.pvalue.PCollection)}
    self.user_pipeline = MagicMock(spec=beam.Pipeline)
    self.recording_manager = MagicMock(spec=RecordingManager)
    self.recording_manager._async_computations = {}
    self.env = ie.InteractiveEnvironment()
    patch.object(ie, 'current_env', return_value=self.env).start()

    self.mock_button = patch('ipywidgets.Button', autospec=True).start()
    self.mock_float_progress = patch(
        'ipywidgets.FloatProgress', autospec=True).start()
    self.mock_output = patch('ipywidgets.Output', autospec=True).start()
    self.mock_hbox = patch('ipywidgets.HBox', autospec=True).start()
    self.mock_vbox = patch('ipywidgets.VBox', autospec=True).start()
    self.mock_display = patch(
        'apache_beam.runners.interactive.recording_manager.display',
        autospec=True).start()
    self.mock_clear_output = patch(
        'apache_beam.runners.interactive.recording_manager.clear_output',
        autospec=True).start()
    self.mock_html = patch(
        'apache_beam.runners.interactive.recording_manager.HTML',
        autospec=True).start()

    self.addCleanup(patch.stopall)

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_async_result_init_non_ipython(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.assertIsNotNone(async_res)
    self.mock_future.add_done_callback.assert_called_once()
    self.assertIsNone(async_res._cancel_button)

  def test_on_done_success(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.DONE
    self.mock_future.result.return_value = mock_pipeline_result
    self.mock_future.exception.return_value = None
    self.mock_future.cancelled.return_value = False
    async_res._display_id = 'test_id'
    self.recording_manager._async_computations['test_id'] = async_res

    with patch.object(
        self.env, 'unmark_pcollection_computing'
    ) as mock_unmark, patch.object(
        self.env, 'mark_pcollection_computed'
    ) as mock_mark_computed, patch.object(
        async_res, 'update_display'
    ) as mock_update:
      async_res._on_done(self.mock_future)
      mock_unmark.assert_called_once_with(self.pcolls)
      mock_mark_computed.assert_called_once_with(self.pcolls)
      self.assertNotIn('test_id', self.recording_manager._async_computations)
      mock_update.assert_called_with('Computation Finished Successfully.', 1.0)

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_on_done_failure(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    test_exception = ValueError('Test')
    self.mock_future.exception.return_value = test_exception
    self.mock_future.cancelled.return_value = False

    with patch.object(
        self.env, 'unmark_pcollection_computing'
    ) as mock_unmark, patch.object(
        self.env, 'mark_pcollection_computed'
    ) as mock_mark_computed:
      async_res._on_done(self.mock_future)
      mock_unmark.assert_called_once_with(self.pcolls)
      mock_mark_computed.assert_not_called()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_on_done_cancelled(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.cancelled.return_value = True

    with patch.object(self.env, 'unmark_pcollection_computing') as mock_unmark:
      async_res._on_done(self.mock_future)
      mock_unmark.assert_called_once_with(self.pcolls)

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  def test_cancel(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.RUNNING
    async_res.set_pipeline_result(mock_pipeline_result)
    self.mock_future.done.return_value = False

    self.assertTrue(async_res.cancel())
    mock_pipeline_result.cancel.assert_called_once()
    self.assertTrue(async_res._cancel_requested)
    self.assertTrue(async_res._cancel_button.disabled)

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_cancel_already_done(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.done.return_value = True
    self.assertFalse(async_res.cancel())

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  @patch('apache_beam.runners.interactive.recording_manager.display')
  @patch('ipywidgets.Button')
  @patch('ipywidgets.FloatProgress')
  @patch('ipywidgets.Output')
  @patch('ipywidgets.HBox')
  @patch('ipywidgets.VBox')
  def test_async_result_init_ipython(
      self,
      mock_vbox,
      mock_hbox,
      mock_output,
      mock_progress,
      mock_button,
      mock_display,
  ):
    mock_btn_instance = mock_button.return_value
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.assertIsNotNone(async_res)
    mock_button.assert_called_once_with(description='Cancel')
    mock_progress.assert_called_once()
    mock_output.assert_called_once()
    mock_hbox.assert_called_once()
    mock_vbox.assert_called_once()
    mock_display.assert_called()
    mock_btn_instance.on_click.assert_called_once_with(
        async_res._cancel_clicked)
    self.mock_future.add_done_callback.assert_called_once()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  @patch(
      'apache_beam.runners.interactive.recording_manager.display', MagicMock())
  @patch('ipywidgets.Button', MagicMock())
  @patch('ipywidgets.FloatProgress', MagicMock())
  @patch('ipywidgets.Output', MagicMock())
  def test_cancel_clicked(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    with patch.object(async_res, 'cancel') as mock_cancel, patch.object(
      async_res, 'update_display'
    ) as mock_update:
      async_res._cancel_clicked(None)
      self.assertTrue(async_res._cancel_requested)
      self.assertTrue(async_res._cancel_button.disabled)
      mock_update.assert_called_once_with('Cancel requested...')
      mock_cancel.assert_called_once()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_update_display_non_ipython(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    with patch('builtins.print') as mock_print:
      async_res.update_display('Test Message')
      mock_print.assert_called_once_with('AsyncCompute: Test Message')

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  def test_update_display_ipython(self):
    mock_prog_instance = self.mock_float_progress.return_value
    mock_btn_instance = self.mock_button.return_value
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )

    update_call_count = 1
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)

    # State: Running
    self.mock_future.done.return_value = False
    async_res._cancel_requested = False
    async_res.update_display('Running Test', 0.5)
    update_call_count += 1
    self.mock_display.assert_called()
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)
    self.assertEqual(mock_prog_instance.value, 0.5)
    self.assertFalse(mock_btn_instance.disabled)
    self.mock_html.assert_called_with('<p>Running Test</p>')

    # State: Done Success
    self.mock_future.done.return_value = True
    self.mock_future.exception.return_value = None
    self.mock_future.cancelled.return_value = False
    async_res.update_display('Done')
    update_call_count += 1
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)
    self.assertTrue(mock_btn_instance.disabled)
    self.assertEqual(mock_prog_instance.bar_style, 'success')
    self.assertEqual(mock_prog_instance.description, 'Done')

    # State: Done Failed
    self.mock_future.exception.return_value = Exception()
    async_res.update_display('Failed')
    update_call_count += 1
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)
    self.assertEqual(mock_prog_instance.bar_style, 'danger')
    self.assertEqual(mock_prog_instance.description, 'Failed')

    # State: Done Cancelled
    self.mock_future.exception.return_value = None
    self.mock_future.cancelled.return_value = True
    async_res.update_display('Cancelled')
    update_call_count += 1
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)
    self.assertEqual(mock_prog_instance.bar_style, 'warning')
    self.assertEqual(mock_prog_instance.description, 'Cancelled')

    # State: Cancelling
    self.mock_future.done.return_value = False
    async_res._cancel_requested = True
    async_res.update_display('Cancelling')
    update_call_count += 1
    self.assertEqual(self.mock_clear_output.call_count, update_call_count)
    self.assertTrue(mock_btn_instance.disabled)
    self.assertEqual(mock_prog_instance.description, 'Cancelling...')

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_set_pipeline_result_cancel_requested(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    async_res._cancel_requested = True
    mock_pipeline_result = MagicMock()
    with patch.object(async_res, 'cancel') as mock_cancel:
      async_res.set_pipeline_result(mock_pipeline_result)
      self.assertIs(async_res._pipeline_result, mock_pipeline_result)
      mock_cancel.assert_called_once()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  def test_exception_timeout(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.exception.side_effect = TimeoutError
    self.assertIsNone(async_res.exception(timeout=0.1))

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', False)
  @patch.object(_LOGGER, 'warning')
  def test_on_done_not_done_state(self, mock_logger_warning):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.FAILED
    self.mock_future.result.return_value = mock_pipeline_result
    self.mock_future.exception.return_value = None
    self.mock_future.cancelled.return_value = False

    with patch.object(self.env,
                      'mark_pcollection_computed') as mock_mark_computed:
      async_res._on_done(self.mock_future)
      mock_mark_computed.assert_not_called()
      mock_logger_warning.assert_called_once()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  def test_cancel_no_pipeline_result(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.done.return_value = False
    self.mock_future.cancel.return_value = True
    with patch.object(async_res, 'update_display') as mock_update:
      self.assertTrue(async_res.cancel())
      mock_update.assert_any_call(
          'Pipeline not yet fully started, cancelling future.')
      self.mock_future.cancel.assert_called_once()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  def test_cancel_pipeline_terminal_state(self):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.done.return_value = False
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.DONE
    async_res.set_pipeline_result(mock_pipeline_result)

    with patch.object(async_res, 'update_display') as mock_update:
      self.assertFalse(async_res.cancel())
      mock_update.assert_any_call(
          'Cannot cancel: Pipeline already in terminal state DONE.')
      mock_pipeline_result.cancel.assert_not_called()

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  @patch.object(_LOGGER, 'warning')
  @patch.object(AsyncComputationResult, 'update_display')
  def test_cancel_pipeline_exception(
      self, mock_update_display, mock_logger_warning):
    async_res = AsyncComputationResult(
        self.mock_future,
        self.pcolls,
        self.user_pipeline,
        self.recording_manager,
    )
    self.mock_future.done.return_value = False
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.RUNNING
    test_exception = RuntimeError('Cancel Failed')
    mock_pipeline_result.cancel.side_effect = test_exception
    async_res.set_pipeline_result(mock_pipeline_result)
    self.mock_future.cancel.return_value = False

    self.assertFalse(async_res.cancel())

    expected_calls = [
        call('Initializing...'),  # From __init__
        call('Attempting to cancel...'),  # From cancel() start
        call('Error sending cancel signal: %s',
             test_exception)  # From except block
    ]
    mock_update_display.assert_has_calls(expected_calls, any_order=False)

    mock_logger_warning.assert_called_once()
    self.mock_future.cancel.assert_called_once()


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
    self.cache = InMemoryCache()
    self.p = beam.Pipeline()
    self.pcoll = self.p | beam.Create([])
    self.cache_key = str(CacheKey('pcoll', '', '', ''))

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    self.mock_result = MockPipelineResult()
    ie.current_env().add_user_pipeline(self.p)
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
        v.recorded_event for v in values
        if isinstance(v, beam_interactive_api_pb2.TestStreamFileRecord)
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
        p, [elems], mock_result, max_n=10, max_duration_secs=60)

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
        p, [elems], mock_result, max_n=10, max_duration_secs=60)
    recording.wait_until_finish()

    # There are no more elements and the recording finished, meaning that the
    # intermediate PCollections are in a complete state. They can now be marked
    # as computed.
    self.assertTrue(recording.is_computed())
    self.assertTrue(recording.computed())
    self.assertFalse(recording.uncomputed())

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
        p, [numbers, letters], mock_result, max_n=10, max_duration_secs=60)

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


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
class RecordingManagerTest(unittest.TestCase):
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
        WindowedValue(i, MIN_TIMESTAMP, (GlobalWindow(), )) for i in range(3)
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

  def test_cancel_stops_recording(self):
    # Add the TestStream so that it can be cached.
    ib.options.recordable_sources.add(TestStream)

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

    class SemaphoreLimiter(Limiter):
      def __init__(self):
        self.triggered = False

      def is_triggered(self):
        return self.triggered

    # Get the recording then the BackgroundCachingJob.
    semaphore_limiter = SemaphoreLimiter()
    rm = RecordingManager(p, test_limiters=[semaphore_limiter])
    rm.record([squares], max_n=10, max_duration=500)

    # The BackgroundCachingJob is still waiting for more elements, so it isn't
    # done yet.
    bcj = ie.current_env().get_background_caching_job(p)
    self.assertFalse(bcj.is_done())

    # Assert that something was read and that the BackgroundCachingJob was
    # sucessfully stopped.
    # self.assertTrue(list(recording.stream(squares).read()))
    semaphore_limiter.triggered = True
    rm.cancel()
    self.assertTrue(bcj.is_done())

  def test_recording_manager_clears_cache(self):
    """Tests that the RecordingManager clears the cache before recording.

    A job may have incomplete PCollections when the job terminates. Clearing the
    cache ensures that correct results are computed every run.
    """
    # Add the TestStream so that it can be cached.
    ib.options.recordable_sources.add(TestStream)
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

    # Set up a mock for the Cache's clear function which will be used to clear
    # uncomputed PCollections.
    rm._clear_pcolls = MagicMock()
    rm.record([squares], max_n=1, max_duration=500)
    rm.cancel()

    # Assert that the cache cleared the PCollection.
    rm._clear_pcolls.assert_any_call(
        unittest.mock.ANY,
        # elems is unbounded source populated by the background job, thus not
        # cleared.
        {CacheKey.from_pcoll('squares', squares).to_str()})

  def test_clear(self):
    p1 = beam.Pipeline(InteractiveRunner())
    elems_1 = p1 | 'elems 1' >> beam.Create([0, 1, 2])

    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    recording_manager = RecordingManager(p1)
    recording = recording_manager.record([elems_1], max_n=3, max_duration=500)
    recording.wait_until_finish()
    record_describe = recording_manager.describe()
    self.assertGreater(record_describe['size'], 0)
    recording_manager.clear()
    self.assertEqual(recording_manager.describe()['size'], 0)

  def test_clear_specific_pipeline(self):
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
    if rm_1.describe()['state'] == PipelineState.STOPPED \
            and rm_2.describe()['state'] == PipelineState.STOPPED:

      self.assertGreater(rm_1.describe()['size'], 0)
      self.assertGreater(rm_2.describe()['size'], 0)
      rm_1.clear()
      self.assertEqual(rm_1.describe()['size'], 0)
      self.assertGreater(rm_2.describe()['size'], 0)

      rm_2.clear()
      self.assertEqual(rm_2.describe()['size'], 0)

  def test_record_pipeline(self):
    # Add the TestStream so that it can be cached.
    ib.options.recordable_sources.add(TestStream)
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
      def __init__(self, p):
        self.pipeline = p
        self._rm = None

      def set_recording_manager(self, rm):
        self._rm = rm

      def is_triggered(self):
        return self._rm.describe()['size'] > 0 if self._rm else False

    # Do the first recording to get the timestamp of the first time the fragment
    # was run.
    size_limiter = SizeLimiter(p)
    rm = RecordingManager(p, test_limiters=[size_limiter])
    size_limiter.set_recording_manager(rm)
    self.assertEqual(rm.describe()['state'], PipelineState.STOPPED)
    self.assertTrue(rm.record_pipeline())

    # A recording is in progress, no need to start another one.
    self.assertFalse(rm.record_pipeline())

    for _ in range(60):
      if rm.describe()['state'] == PipelineState.CANCELLED:
        break
      time.sleep(1)
    self.assertTrue(
        rm.describe()['state'] == PipelineState.CANCELLED,
        'Test timed out waiting for pipeline to be cancelled. This indicates '
        'that the BackgroundCachingJob did not cache anything.')

  @patch(
      'apache_beam.runners.interactive.recording_manager.'
      'RecordingManager._clear_pcolls',
      return_value=None)
  @patch(
      'apache_beam.runners.interactive.pipeline_fragment.'
      'PipelineFragment.run',
      return_value=None)
  def test_record_detects_remote_runner(
      self, mock_pipeline_fragment, mock_clear_pcolls):
    """Tests that a remote runner is detected, resulting in the
    PipelineFragment instance to have blocking enabled."""

    # Create the pipeline that will emit 0, 1, 2.
    p = beam.Pipeline(InteractiveRunner())
    numbers = p | 'numbers' >> beam.Create([0, 1, 2])

    # Set the cache directory for Interactive Beam to be in a GCS bucket.
    ib.options.cache_root = 'gs://test-bucket/'

    # Create the recording objects. By calling `record` a new PipelineFragment
    # is started to compute the given PCollections and cache to disk.
    rm = RecordingManager(p)

    # Run record() and check if the PipelineFragment.run had blocking set to
    # True due to the GCS cache_root value.
    rm.record([numbers], max_n=3, max_duration=500)
    mock_pipeline_fragment.assert_called_with(blocking=True)

    # Reset cache_root value.
    ib.options.cache_root = None

  def test_compute_async_blocking(self):
    p = beam.Pipeline(InteractiveRunner())
    pcoll = p | beam.Create([1, 2, 3])
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    rm = RecordingManager(p)

    with patch.object(rm, '_execute_pipeline_fragment') as mock_execute:
      mock_result = MagicMock()
      mock_result.state = PipelineState.DONE
      mock_execute.return_value = mock_result
      res = rm.compute_async({pcoll}, blocking=True)
      self.assertIsNone(res)
      mock_execute.assert_called_once()
      self.assertTrue(pcoll in ie.current_env().computed_pcollections)

  @patch(
      'apache_beam.runners.interactive.recording_manager.AsyncComputationResult'
  )
  @patch(
      'apache_beam.runners.interactive.recording_manager.ThreadPoolExecutor.'
      'submit')
  def test_compute_async_non_blocking(self, mock_submit, mock_async_result_cls):
    p = beam.Pipeline(InteractiveRunner())
    pcoll = p | beam.Create([1, 2, 3])
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    rm = RecordingManager(p)
    mock_async_res_instance = mock_async_result_cls.return_value

    # Capture the task
    task_submitted = None

    def capture_task(task):
      nonlocal task_submitted
      task_submitted = task
      # Return a mock future
      return MagicMock()

    mock_submit.side_effect = capture_task

    with patch.object(
        rm, '_wait_for_dependencies', return_value=True
    ), patch.object(
        rm, '_execute_pipeline_fragment'
    ) as _, patch.object(
        ie.current_env(),
        'mark_pcollection_computing',
        wraps=ie.current_env().mark_pcollection_computing,
    ) as wrapped_mark:

      res = rm.compute_async({pcoll}, blocking=False)
      wrapped_mark.assert_called_once_with({pcoll})

    # Run the task to trigger the marks
    self.assertIs(res, mock_async_res_instance)
    mock_submit.assert_called_once()
    self.assertIsNotNone(task_submitted)

    with patch.object(
        rm, '_wait_for_dependencies', return_value=True
    ), patch.object(
        rm, '_execute_pipeline_fragment'
    ) as _:
      task_submitted()

    self.assertTrue(pcoll in ie.current_env().computing_pcollections)

  def test_get_all_dependencies(self):
    p = beam.Pipeline(InteractiveRunner())
    p1 = p | 'C1' >> beam.Create([1])
    p2 = p | 'C2' >> beam.Create([2])
    p3 = p1 | 'M1' >> beam.Map(lambda x: x)
    p4 = (p2, p3) | 'F1' >> beam.Flatten()
    p5 = p3 | 'M2' >> beam.Map(lambda x: x)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    rm = RecordingManager(p)
    rm.record_pipeline()  # Analyze pipeline

    self.assertEqual(rm._get_all_dependencies({p1}), set())
    self.assertEqual(rm._get_all_dependencies({p3}), {p1})
    self.assertEqual(rm._get_all_dependencies({p4}), {p1, p2, p3})
    self.assertEqual(rm._get_all_dependencies({p5}), {p1, p3})
    self.assertEqual(rm._get_all_dependencies({p4, p5}), {p1, p2, p3})

  @patch(
      'apache_beam.runners.interactive.recording_manager.AsyncComputationResult'
  )
  def test_wait_for_dependencies(self, mock_async_result_cls):
    p = beam.Pipeline(InteractiveRunner())
    p1 = p | 'C1' >> beam.Create([1])
    p2 = p1 | 'M1' >> beam.Map(lambda x: x)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    rm = RecordingManager(p)
    rm.record_pipeline()

    # Scenario 1: No dependencies computing
    self.assertTrue(rm._wait_for_dependencies({p2}))

    # Scenario 2: Dependency is computing
    mock_future = MagicMock(spec=Future)
    mock_async_res = MagicMock(spec=AsyncComputationResult)
    mock_async_res._future = mock_future
    mock_async_res._pcolls = {p1}
    rm._async_computations['dep_id'] = mock_async_res
    ie.current_env().mark_pcollection_computing({p1})

    self.assertTrue(rm._wait_for_dependencies({p2}))
    mock_future.result.assert_called_once()
    ie.current_env().unmark_pcollection_computing({p1})


if __name__ == '__main__':
  unittest.main()
