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

# pytype: skip-file

import unittest

from mock import MagicMock
from mock import patch

from apache_beam.io.components.adaptive_throttler import AdaptiveThrottler
from apache_beam.io.components.adaptive_throttler import ReactiveThrottler


class AdaptiveThrottlerTest(unittest.TestCase):

  START_TIME = 1500000000000
  SAMPLE_PERIOD = 60000
  BUCKET = 1000
  OVERLOAD_RATIO = 2

  def setUp(self):
    self._throttler = AdaptiveThrottler(
        AdaptiveThrottlerTest.SAMPLE_PERIOD,
        AdaptiveThrottlerTest.BUCKET,
        AdaptiveThrottlerTest.OVERLOAD_RATIO)

  # As far as practical, keep these tests aligned with
  # AdaptiveThrottlerTest.java.

  def test_no_initial_throttling(self):
    self.assertEqual(
        0,
        self._throttler._throttling_probability(
            AdaptiveThrottlerTest.START_TIME))

  def test_no_throttling_if_no_errors(self):
    for t in range(AdaptiveThrottlerTest.START_TIME,
                   AdaptiveThrottlerTest.START_TIME + 20):
      self.assertFalse(self._throttler.throttle_request(t))
      self._throttler.successful_request(t)
    self.assertEqual(
        0,
        self._throttler._throttling_probability(
            AdaptiveThrottlerTest.START_TIME + 20))

  def test_no_throttling_after_errors_expire(self):
    for t in range(AdaptiveThrottlerTest.START_TIME,
                   AdaptiveThrottlerTest.START_TIME +
                   AdaptiveThrottlerTest.SAMPLE_PERIOD,
                   100):
      self._throttler.throttle_request(t)
      # And no sucessful_request
    self.assertLess(
        0,
        self._throttler._throttling_probability(
            AdaptiveThrottlerTest.START_TIME +
            AdaptiveThrottlerTest.SAMPLE_PERIOD))
    for t in range(AdaptiveThrottlerTest.START_TIME +
                   AdaptiveThrottlerTest.SAMPLE_PERIOD,
                   AdaptiveThrottlerTest.START_TIME +
                   AdaptiveThrottlerTest.SAMPLE_PERIOD * 2,
                   100):
      self._throttler.throttle_request(t)
      self._throttler.successful_request(t)

    self.assertEqual(
        0,
        self._throttler._throttling_probability(
            AdaptiveThrottlerTest.START_TIME +
            AdaptiveThrottlerTest.SAMPLE_PERIOD * 2))

  @patch('random.Random')
  def test_throttling_after_errors(self, mock_random):
    mock_random().uniform.side_effect = [x / 10.0 for x in range(0, 10)] * 2
    self._throttler = AdaptiveThrottler(
        AdaptiveThrottlerTest.SAMPLE_PERIOD,
        AdaptiveThrottlerTest.BUCKET,
        AdaptiveThrottlerTest.OVERLOAD_RATIO)
    for t in range(AdaptiveThrottlerTest.START_TIME,
                   AdaptiveThrottlerTest.START_TIME + 20):
      throttled = self._throttler.throttle_request(t)
      # 1/3rd of requests succeeding.
      if t % 3 == 1:
        self._throttler.successful_request(t)

      if t > AdaptiveThrottlerTest.START_TIME + 10:
        # Roughly 1/3rd succeeding, 1/3rd failing, 1/3rd throttled.
        self.assertAlmostEqual(
            0.33, self._throttler._throttling_probability(t), delta=0.1)
        # Given the mocked random numbers, we expect 10..13 to be throttled and
        # 14+ to be unthrottled.
        self.assertEqual(t < AdaptiveThrottlerTest.START_TIME + 14, throttled)


class ReactiveThrottlerTest(unittest.TestCase):

  START_TIME_SECS = 1500000000
  THROTTLE_DELAY_SECS = 5

  def setUp(self):
    self._throttler = ReactiveThrottler(
        window_ms=60000,
        bucket_ms=1000,
        overload_ratio=2,
        namespace='ReactiveThrottlerTest',
        throttle_delay_secs=ReactiveThrottlerTest.THROTTLE_DELAY_SECS)
    self._mock_signaler = MagicMock()
    self._throttler.throttling_signaler = self._mock_signaler
    sleep_patcher = patch(
        'apache_beam.io.components.adaptive_throttler.time.sleep')
    self._mock_sleep = sleep_patcher.start()
    self.addCleanup(sleep_patcher.stop)

  def test_throttle_does_not_delay_unthrottled_request(self):
    with patch.object(self._throttler, 'throttle_request', return_value=False):
      self._throttler.throttle()
    self._mock_sleep.assert_not_called()
    self._mock_signaler.signal_throttled.assert_not_called()

  def test_throttle_delays_throttled_request_once(self):
    with patch.object(self._throttler,
                      'throttle_request',
                      side_effect=[True, False]) as mock_throttle_request:
      self._throttler.throttle()
    mock_throttle_request.assert_called_once()
    self._mock_sleep.assert_called_once_with(
        ReactiveThrottlerTest.THROTTLE_DELAY_SECS)
    self._mock_signaler.signal_throttled.assert_called_once_with(
        ReactiveThrottlerTest.THROTTLE_DELAY_SECS)

  def test_throttle_records_one_request_per_call(self):
    # Throttle whenever the throttling probability is positive. The finite
    # side_effect makes an implementation that re-checks after the delay fail
    # fast instead of looping forever.
    self._throttler._random = MagicMock()
    self._throttler._random.uniform.side_effect = [0.0] * 3
    with patch('apache_beam.io.components.adaptive_throttler.time.time',
               return_value=ReactiveThrottlerTest.START_TIME_SECS):
      # None of the requests succeed, i.e. successful_request() is not called.
      for _ in range(3):
        self._throttler.throttle()

    now_ms = ReactiveThrottlerTest.START_TIME_SECS * 1000
    self.assertEqual(3, self._throttler._all_requests.sum(now_ms))
    self.assertEqual(0.75, self._throttler._throttling_probability(now_ms))
    # The first request has no history to be throttled on.
    self.assertEqual(2, self._mock_sleep.call_count)


if __name__ == '__main__':
  unittest.main()
