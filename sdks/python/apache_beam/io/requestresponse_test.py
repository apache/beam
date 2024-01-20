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
import time
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import TooManyRequests
  from apache_beam.io.requestresponse import Caller, DefaultThrottler
  from apache_beam.io.requestresponse import RequestResponseIO
  from apache_beam.io.requestresponse import UserCodeExecutionException
  from apache_beam.io.requestresponse import UserCodeTimeoutException
  from apache_beam.io.requestresponse import retry_on_exception
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')


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

  def test_call_timeout(self):
    caller = CallerWithTimeout()
    with self.assertRaises(UserCodeTimeoutException):
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["timeout_request"])
            | RequestResponseIO(caller=caller, timeout=1))

  def test_call_runtime_error(self):
    caller = CallerWithRuntimeError()
    with self.assertRaises(UserCodeExecutionException):
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
    with self.assertRaises(TooManyRequests) as cm:
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["sample_request"])
            | RequestResponseIO(caller=caller))
    self.assertRegex(cm.exception.message, 'retries = 2')

  def test_caller_no_retry_strategy(self):
    caller = CallerThatRetries()
    with self.assertRaises(TooManyRequests) as cm:
      with TestPipeline() as test_pipeline:
        _ = (
            test_pipeline
            | beam.Create(["sample_request"])
            | RequestResponseIO(caller=caller, repeater=None))
    self.assertRegex(cm.exception.message, 'retries = 0')

  def test_default_throttler(self):
    caller = CallerWithTimeout()
    throttler = DefaultThrottler(
        window_ms=10000, bucket_ms=5000, overload_ratio=1)
    # manually override the number of received requests for testing.
    throttler.throttler._all_requests.add(time.time() * 1000, 100)
    test_pipeline = TestPipeline()
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


if __name__ == '__main__':
  unittest.main()
