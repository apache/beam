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

import datetime
import math
import sys
import unittest

from mock import patch

from apache_beam.io.gcp.datastore.v1new.rampup_throttling_fn import RampupThrottlingFn

DATE_ZERO = datetime.datetime(
    year=1970, month=1, day=1, tzinfo=datetime.timezone.utc)


class _RampupDelayException(Exception):
  pass


class RampupThrottlerTransformTest(unittest.TestCase):
  @patch('datetime.datetime')
  @patch('time.sleep')
  def test_rampup_throttling(self, mock_sleep, mock_datetime):
    mock_datetime.now.return_value = DATE_ZERO
    throttling_fn = RampupThrottlingFn(num_workers=1)
    rampup_schedule = [
        (DATE_ZERO + datetime.timedelta(seconds=0), 500),
        (DATE_ZERO + datetime.timedelta(milliseconds=1), 0),
        (DATE_ZERO + datetime.timedelta(seconds=1), 500),
        (DATE_ZERO + datetime.timedelta(seconds=1, milliseconds=1), 0),
        (DATE_ZERO + datetime.timedelta(minutes=5), 500),
        (DATE_ZERO + datetime.timedelta(minutes=10), 750),
        (DATE_ZERO + datetime.timedelta(minutes=15), 1125),
        (DATE_ZERO + datetime.timedelta(minutes=30), 3796),
        (DATE_ZERO + datetime.timedelta(minutes=60), 43248),
    ]

    mock_sleep.side_effect = _RampupDelayException()
    for date, expected_budget in rampup_schedule:
      mock_datetime.now.return_value = date
      for _ in range(expected_budget):
        next(throttling_fn.process(None))
      # Delay after budget is exhausted
      with self.assertRaises(_RampupDelayException):
        next(throttling_fn.process(None))

  def test_budget_overflow(self):
    throttling_fn = RampupThrottlingFn(num_workers=1)

    normal_date = DATE_ZERO + datetime.timedelta(minutes=2000)
    normal_budget = throttling_fn._calc_max_ops_budget(DATE_ZERO, normal_date)
    self.assertNotEqual(normal_budget, float('inf'))

    # This tests that a previously thrown OverflowError is caught.
    overflow_minutes = math.log(sys.float_info.max) / math.log(1.5) * 5
    overflow_date = DATE_ZERO + datetime.timedelta(minutes=overflow_minutes)
    overflow_budget = throttling_fn._calc_max_ops_budget(
        DATE_ZERO, overflow_date)
    self.assertEqual(overflow_budget, float('inf'))


if __name__ == '__main__':
  unittest.main()
