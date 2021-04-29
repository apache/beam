import datetime
import time
import logging
import unittest

from mock import patch

from apache_beam.io.gcp.datastore.v1new.rampup_throttling_fn import \
  RampupThrottlingFn


DATE_ZERO = datetime.datetime(year=1970, month=1, day=1)


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
        throttling_fn.process(None)
      # Delay after budget is exhausted
      with self.assertRaises(_RampupDelayException):
        throttling_fn.process(None)


if __name__ == '__main__':
  unittest.main()
