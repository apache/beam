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
import logging
import time
import unittest

from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import TooManyRequests

  from apache_beam.io.requestresponse import Caller
  from apache_beam.io.requestresponse import DefaultThrottler
  from apache_beam.io.requestresponse import RequestResponseIO
  from apache_beam.io.requestresponse import retry_on_exception
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')

_LOGGER = logging.getLogger()

MAX_TEST_RETRIES = 3


class AckCaller(Caller[str, str]):
  """AckCaller acknowledges the incoming request by returning a
  request with ACK."""
  def __enter__(self):
    pass

  def __call__(self, request: str):
    return f"ACK: {request}"

  def __exit__(self, exc_type, exc_val, exc_tb):
    return None


class CallerWithTimeout(AckCaller):
  """CallerWithTimeout sleeps for 2 seconds before responding.
  Used to test timeout in RequestResponseIO."""
  def __call__(self, request: str, *args, **kwargs):
    time.sleep(2)
    return f"ACK: {request}"


class CallerWithRuntimeError(AckCaller):
  """CallerWithRuntimeError raises a `RuntimeError` for RequestResponseIO
  to raise a UserCodeExecutionException."""
  def __call__(self, request: str, *args, **kwargs):
    if not request:
      raise RuntimeError("Exception expected, not an error.")


class CallerThatRetries(AckCaller):
  def __init__(self):
    self.count = -1

  def __call__(self, request: str, *args, **kwargs):
    try:
      pass
    except Exception as e:
      raise e
    finally:
      self.count += 1
      raise TooManyRequests('retries = %d' % self.count)


class TestCaller(unittest.TestCase):
  def test_valid_call(self):
    caller = AckCaller()
    with TestPipeline() as test_pipeline:
      output = (
          test_pipeline
          | beam.Create(["sample_request"])
          | RequestResponseIO(caller=caller))

    self.assertIsNotNone(output)

  def test_valid_call_without_throttler(self):
    caller = AckCaller()
    with TestPipeline() as test_pipeline:
      output = (
          test_pipeline
          | beam.Create(["sample_request"])
          | RequestResponseIO(caller=caller, throttler=None))

    self.assertIsNotNone(output)

  def test_call_timeout(self):
    caller = CallerWithTimeout()
    with self.assertRaisesRegex(Exception, "Timeout"):
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["timeout_request"])
            | RequestResponseIO(caller=caller, timeout=1))

  def test_call_runtime_error(self):
    caller = CallerWithRuntimeError()
    with self.assertRaisesRegex(Exception, "could not complete request"):
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create([""])
            | RequestResponseIO(caller=caller))

  def test_retry_on_exception(self):
    self.assertFalse(retry_on_exception(RuntimeError()))
    self.assertTrue(retry_on_exception(TooManyRequests("HTTP 429")))

  def test_caller_backoff_retry_strategy(self):
    caller = CallerThatRetries()
    with self.assertRaises(Exception) as cm:
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["sample_request"])
            | RequestResponseIO(caller=caller))
    self.assertRegex(str(cm.exception), 'retries = 2')

  def test_caller_no_retry_strategy(self):
    caller = CallerThatRetries()
    with self.assertRaises(Exception) as cm:
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["sample_request"])
            | RequestResponseIO(caller=caller, repeater=None))
    self.assertRegex(str(cm.exception), 'retries = 0')

  @retry(
      retry=retry_if_exception_type(IndexError),
      reraise=True,
      stop=stop_after_attempt(MAX_TEST_RETRIES))
  def test_default_throttler(self):
    caller = CallerWithTimeout()
    throttler = DefaultThrottler(
        window_ms=10000, bucket_ms=5000, overload_ratio=1)
    # manually override the number of received requests for testing.
    throttler.throttler._all_requests.add(time.time() * 1000, 100)
    # TODO(https://github.com/apache/beam/issues/34549): This test relies on
    # metrics filtering which doesn't work on Prism yet because Prism renames
    # steps (e.g. "Do" becomes "ref_AppliedPTransform_Do_7").
    # https://github.com/apache/beam/blob/5f9cd73b7c9a2f37f83971ace3a399d633201dd1/sdks/python/apache_beam/runners/portability/fn_api_runner/fn_runner.py#L1590
    test_pipeline = TestPipeline('FnApiRunner')
    _ = (
        test_pipeline
        | beam.Create(['sample_request'])
        | RequestResponseIO(caller=caller, throttler=throttler))
    result = test_pipeline.run()
    result.wait_until_finish()
    metrics = result.metrics().query(
        beam.metrics.MetricsFilter().with_name('throttled_requests'))
    self.assertEqual(metrics['counters'][0].committed, 1)
    metrics = result.metrics().query(
        beam.metrics.MetricsFilter().with_name('cumulativeThrottlingSeconds'))
    self.assertGreater(metrics['counters'][0].committed, 0)
    metrics = result.metrics().query(
        beam.metrics.MetricsFilter().with_name('responses'))
    self.assertEqual(metrics['counters'][0].committed, 1)

  def test_batched_caller_preserves_event_timestamps_and_windows(self):
    from apache_beam.testing.util import assert_that
    from apache_beam.testing.util import equal_to
    from apache_beam.transforms.enrichment import Enrichment
    from apache_beam.transforms.enrichment import EnrichmentSourceHandler
    from apache_beam.transforms.window import FixedWindows
    from apache_beam.transforms.window import TimestampedValue

    class _OutOfOrderBatchedHandler(EnrichmentSourceHandler):
      def __call__(self, request, *args, **kwargs):
        # Return responses in reverse order to verify per-request timestamp
        # matching even when the handler reorders batch results.
        return [(req, beam.Row(tier=f'tier_{req.id}'))
                for req in reversed(request)]

      def batch_elements_kwargs(self):
        return {
            'min_batch_size': 2,
            'max_batch_size': 10,
            'element_size_fn': lambda _: 1,
        }

    inputs = [
        TimestampedValue(beam.Row(id=1), 1700000045.0),
        TimestampedValue(beam.Row(id=2), 1700000055.0),
        TimestampedValue(beam.Row(id=3), 1700000125.0),
    ]

    with TestPipeline() as p:
      enriched = (
          p
          | beam.Create(inputs)
          | Enrichment(_OutOfOrderBatchedHandler()))
      with_timestamps = enriched | 'CaptureTimestamps' >> beam.Map(
          lambda row, ts=beam.DoFn.TimestampParam:
          (row.id, row.tier, float(ts)))
      assert_that(
          with_timestamps,
          equal_to([
              (1, 'tier_1', 1700000045.0),
              (2, 'tier_2', 1700000055.0),
              (3, 'tier_3', 1700000125.0),
          ]),
          label='CheckTimestamps',
      )

      windowed_counts = (
          enriched
          | beam.WindowInto(FixedWindows(60))
          | beam.Map(lambda row: (row.id // 3, 1))
          | beam.CombinePerKey(sum))
      assert_that(
          windowed_counts,
          equal_to([(0, 2), (1, 1)]),
          label='CheckFixedWindows',
      )

      pre_windowed = (
          p
          | 'CreatePreWindowed' >> beam.Create(inputs)
          | 'PreWindow' >> beam.WindowInto(FixedWindows(60))
          | 'EnrichPreWindowed' >> Enrichment(_OutOfOrderBatchedHandler())
          | 'CapturePreWindowed' >> beam.Map(
              lambda row, ts=beam.DoFn.TimestampParam, w=beam.DoFn.WindowParam:
              (row.id, float(ts), float(w.start), float(w.end))))
      assert_that(
          pre_windowed,
          equal_to([
              (1, 1700000045.0, 1700000040.0, 1700000100.0),
              (2, 1700000055.0, 1700000040.0, 1700000100.0),
              (3, 1700000125.0, 1700000100.0, 1700000160.0),
          ]),
          label='CheckPreWindowed',
      )

  def test_reattach_window_metadata_equality_and_positional(self):
    from apache_beam.io.requestresponse import _reattach_window_metadata
    from apache_beam.io.requestresponse import _WindowedElement
    from apache_beam.transforms.window import GlobalWindow
    from apache_beam.utils.timestamp import Timestamp

    w1 = _WindowedElement(beam.Row(id=1), Timestamp(100), (GlobalWindow(), ))
    w2 = _WindowedElement(beam.Row(id=2), Timestamp(200), (GlobalWindow(), ))

    # Reconstructed Row objects (different object id, equal value) in reverse.
    reconstructed_responses = [
        (beam.Row(id=2), beam.Row(val='b')),
        (beam.Row(id=1), beam.Row(val='a')),
    ]
    matched = _reattach_window_metadata([w1, w2], reconstructed_responses)
    self.assertEqual([float(m.timestamp) for m in matched], [200.0, 100.0])

    # Positional non-tuple responses.
    pos_matched = _reattach_window_metadata([w1, w2], ['resp1', 'resp2'])
    self.assertEqual([float(m.timestamp) for m in pos_matched], [100.0, 200.0])

    # Scalar response fallback.
    scalar_matched = _reattach_window_metadata([w1], 'single_resp')
    self.assertEqual(float(scalar_matched[0].timestamp), 100.0)

    from apache_beam.io.requestresponse import FlattenBatch
    self.assertEqual(
        list(FlattenBatch().process(['raw_a', 'raw_b'])), ['raw_a', 'raw_b'])


if __name__ == '__main__':
  unittest.main()
