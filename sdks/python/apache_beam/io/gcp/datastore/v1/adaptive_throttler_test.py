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

from __future__ import absolute_import
from __future__ import division

import os
import sys
import unittest
from builtins import range

from mock import patch

from apache_beam.io.gcp.datastore.v1.adaptive_throttler import AdaptiveThrottler


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
class AdaptiveThrottlerTest(unittest.TestCase):

  START_TIME = 1500000000000
  SAMPLE_PERIOD = 60000
  BUCKET = 1000
  OVERLOAD_RATIO = 2

  def setUp(self):
    self._throttler = AdaptiveThrottler(
        AdaptiveThrottlerTest.SAMPLE_PERIOD, AdaptiveThrottlerTest.BUCKET,
        AdaptiveThrottlerTest.OVERLOAD_RATIO)

  # As far as practical, keep these tests aligned with
  # AdaptiveThrottlerTest.java.

  def test_no_initial_throttling(self):
    self.assertEqual(0, self._throttler._throttling_probability(
        AdaptiveThrottlerTest.START_TIME))

  def test_no_throttling_if_no_errors(self):
    for t in range(AdaptiveThrottlerTest.START_TIME,
                   AdaptiveThrottlerTest.START_TIME + 20):
      self.assertFalse(self._throttler.throttle_request(t))
      self._throttler.successful_request(t)
    self.assertEqual(0, self._throttler._throttling_probability(
        AdaptiveThrottlerTest.START_TIME + 20))

  def test_no_throttling_after_errors_expire(self):
    for t in range(AdaptiveThrottlerTest.START_TIME,
                   AdaptiveThrottlerTest.START_TIME
                   + AdaptiveThrottlerTest.SAMPLE_PERIOD, 100):
      self._throttler.throttle_request(t)
      # And no sucessful_request
    self.assertLess(0, self._throttler._throttling_probability(
        AdaptiveThrottlerTest.START_TIME + AdaptiveThrottlerTest.SAMPLE_PERIOD
        ))
    for t in range(AdaptiveThrottlerTest.START_TIME
                   + AdaptiveThrottlerTest.SAMPLE_PERIOD,
                   AdaptiveThrottlerTest.START_TIME
                   + AdaptiveThrottlerTest.SAMPLE_PERIOD*2, 100):
      self._throttler.throttle_request(t)
      self._throttler.successful_request(t)

    self.assertEqual(0, self._throttler._throttling_probability(
        AdaptiveThrottlerTest.START_TIME +
        AdaptiveThrottlerTest.SAMPLE_PERIOD*2))

  @patch('random.Random')
  def test_throttling_after_errors(self, mock_random):
    mock_random().uniform.side_effect = [x/10.0 for x in range(0, 10)]*2
    self._throttler = AdaptiveThrottler(
        AdaptiveThrottlerTest.SAMPLE_PERIOD, AdaptiveThrottlerTest.BUCKET,
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


if __name__ == '__main__':
  unittest.main()
