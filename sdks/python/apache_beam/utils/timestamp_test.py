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

"""Unit tests for time utilities."""

# pytype: skip-file

import datetime
import unittest

import pytz
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


class TimestampTest(unittest.TestCase):
  def test_of(self):
    interval = Timestamp(123)
    self.assertEqual(id(interval), id(Timestamp.of(interval)))
    self.assertEqual(interval, Timestamp.of(123.0))
    with self.assertRaises(TypeError):
      Timestamp.of(Duration(10))

  def test_precision(self):
    self.assertEqual(Timestamp(10000000) % 0.1, 0)
    self.assertEqual(Timestamp(10000000) % 0.05, 0)
    self.assertEqual(Timestamp(10000000) % 0.000005, 0)
    self.assertEqual(Timestamp(10000000) % Duration(0.1), 0)
    self.assertEqual(Timestamp(10000000) % Duration(0.05), 0)
    self.assertEqual(Timestamp(10000000) % Duration(0.000005), 0)

  def test_utc_timestamp(self):
    self.assertEqual(Timestamp(10000000).to_rfc3339(), '1970-04-26T17:46:40Z')
    self.assertEqual(
        Timestamp(10000000.000001).to_rfc3339(), '1970-04-26T17:46:40.000001Z')
    self.assertEqual(
        Timestamp(1458343379.123456).to_rfc3339(),
        '2016-03-18T23:22:59.123456Z')

  def test_from_rfc3339(self):
    test_cases = [
        (10000000, '1970-04-26T17:46:40Z'),
        (10000000.000001, '1970-04-26T17:46:40.000001Z'),
        (1458343379.123456, '2016-03-18T23:22:59.123456Z'),
    ]
    for seconds_float, rfc3339_str in test_cases:
      self.assertEqual(
          Timestamp(seconds_float), Timestamp.from_rfc3339(rfc3339_str))
      self.assertEqual(
          rfc3339_str, Timestamp.from_rfc3339(rfc3339_str).to_rfc3339())

  def test_from_rfc3339_with_timezone(self):
    test_cases = [
        (1458328979.123456, '2016-03-18T23:22:59.123456+04:00'),
        (1458357779.123456, '2016-03-18T23:22:59.123456-04:00'),
    ]
    for seconds_float, rfc3339_str in test_cases:
      self.assertEqual(
          Timestamp(seconds_float), Timestamp.from_rfc3339(rfc3339_str))

  def test_from_rfc3339_failure(self):
    with self.assertRaisesRegex(ValueError, 'parse'):
      Timestamp.from_rfc3339('not rfc3339')
    with self.assertRaisesRegex(ValueError, 'parse'):
      Timestamp.from_rfc3339('2016-03-18T23:22:59.123456Z unparseable')

  def test_from_utc_datetime(self):
    self.assertEqual(
        Timestamp.from_utc_datetime(
            datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)),
        Timestamp(0))
    with self.assertRaisesRegex(ValueError, r'UTC'):
      Timestamp.from_utc_datetime(datetime.datetime(1970, 1, 1))

  def test_arithmetic(self):
    # Supported operations.
    self.assertEqual(Timestamp(123) + 456, 579)
    self.assertEqual(Timestamp(123) + Duration(456), 579)
    self.assertEqual(456 + Timestamp(123), 579)
    self.assertEqual(Duration(456) + Timestamp(123), 579)
    self.assertEqual(Timestamp(123) - 456, -333)
    self.assertEqual(Timestamp(123) - Duration(456), -333)
    self.assertEqual(Timestamp(1230) % 456, 318)
    self.assertEqual(Timestamp(1230) % Duration(456), 318)
    self.assertEqual(Timestamp(123) - Timestamp(100), 23)

    # Check that direct comparison of Timestamp and Duration is allowed.
    self.assertTrue(Duration(123) == Timestamp(123))
    self.assertTrue(Timestamp(123) == Duration(123))
    self.assertFalse(Duration(123) == Timestamp(1230))
    self.assertFalse(Timestamp(123) == Duration(1230))

    # Check return types.
    self.assertEqual((Timestamp(123) + 456).__class__, Timestamp)
    self.assertEqual((Timestamp(123) + Duration(456)).__class__, Timestamp)
    self.assertEqual((456 + Timestamp(123)).__class__, Timestamp)
    self.assertEqual((Duration(456) + Timestamp(123)).__class__, Timestamp)
    self.assertEqual((Timestamp(123) - 456).__class__, Timestamp)
    self.assertEqual((Timestamp(123) - Duration(456)).__class__, Timestamp)
    self.assertEqual((Timestamp(1230) % 456).__class__, Duration)
    self.assertEqual((Timestamp(1230) % Duration(456)).__class__, Duration)
    self.assertEqual((Timestamp(123) - Timestamp(100)).__class__, Duration)

    # Unsupported operations.
    with self.assertRaises(TypeError):
      self.assertEqual(Timestamp(123) * 456, 56088)
    with self.assertRaises(TypeError):
      self.assertEqual(Timestamp(123) * Duration(456), 56088)
    with self.assertRaises(TypeError):
      self.assertEqual(456 * Timestamp(123), 56088)
    with self.assertRaises(TypeError):
      self.assertEqual(Duration(456) * Timestamp(123), 56088)
    with self.assertRaises(TypeError):
      self.assertEqual(456 - Timestamp(123), 333)
    with self.assertRaises(TypeError):
      self.assertEqual(Duration(456) - Timestamp(123), 333)
    with self.assertRaises(TypeError):
      self.assertEqual(-Timestamp(123), -123)
    with self.assertRaises(TypeError):
      self.assertEqual(-Timestamp(123), -Duration(123))
    with self.assertRaises(TypeError):
      self.assertEqual(1230 % Timestamp(456), 318)
    with self.assertRaises(TypeError):
      self.assertEqual(Duration(1230) % Timestamp(456), 318)

  def test_sort_order(self):
    self.assertEqual([-63, Timestamp(-3), 2, 9, Timestamp(292.3), 500],
                     sorted([9, 2, Timestamp(-3), Timestamp(292.3), -63, 500]))
    self.assertEqual([4, 5, Timestamp(6), Timestamp(7), 8, 9],
                     sorted([9, 8, Timestamp(7), Timestamp(6), 5, 4]))

  def test_str(self):
    self.assertEqual('Timestamp(1.234567)', str(Timestamp(1.234567)))
    self.assertEqual('Timestamp(-1.234567)', str(Timestamp(-1.234567)))
    self.assertEqual(
        'Timestamp(-999999999.900000)', str(Timestamp(-999999999.9)))
    self.assertEqual('Timestamp(999999999)', str(Timestamp(999999999)))
    self.assertEqual('Timestamp(-999999999)', str(Timestamp(-999999999)))

  def test_now(self):
    now = Timestamp.now()
    self.assertTrue(isinstance(now, Timestamp))

  def test_from_proto(self):
    ts_proto = timestamp_pb2.Timestamp(seconds=1234, nanos=56000)
    actual_ts = Timestamp.from_proto(ts_proto)
    expected_ts = Timestamp(seconds=1234, micros=56)
    self.assertEqual(actual_ts, expected_ts)

  def test_from_proto_fails_with_truncation(self):
    # TODO(BEAM-8738): Better define timestamps.
    with self.assertRaises(ValueError):
      Timestamp.from_proto(timestamp_pb2.Timestamp(seconds=1234, nanos=56789))

  def test_to_proto(self):
    ts = Timestamp(seconds=1234, micros=56)
    actual_ts_proto = Timestamp.to_proto(ts)
    expected_ts_proto = timestamp_pb2.Timestamp(seconds=1234, nanos=56000)
    self.assertEqual(actual_ts_proto, expected_ts_proto)

  def test_equality(self):
    for min_val in (Timestamp(1), Duration(1), 1, 1.1):
      for max_val in (Timestamp(123), Duration(123), 123, 123.4):
        self.assertTrue(min_val < max_val, "%s < %s" % (min_val, max_val))
        self.assertTrue(min_val <= max_val, "%s <= %s" % (min_val, max_val))
        self.assertTrue(max_val > min_val, "%s > %s" % (max_val, min_val))
        self.assertTrue(max_val >= min_val, "%s >= %s" % (max_val, min_val))


class DurationTest(unittest.TestCase):
  def test_of(self):
    interval = Duration(123)
    self.assertEqual(id(interval), id(Duration.of(interval)))
    self.assertEqual(interval, Duration.of(123.0))
    with self.assertRaises(TypeError):
      Duration.of(Timestamp(10))

  def test_precision(self):
    self.assertEqual(Duration(10000000) % 0.1, 0)
    self.assertEqual(Duration(10000000) % 0.05, 0)
    self.assertEqual(Duration(10000000) % 0.000005, 0)

  def test_arithmetic(self):
    self.assertEqual(Duration(123) + 456, 579)
    self.assertEqual(456 + Duration(123), 579)
    self.assertEqual(Duration(123) * 456, 56088)
    self.assertEqual(456 * Duration(123), 56088)
    self.assertEqual(Duration(123) - 456, -333)
    self.assertEqual(456 - Duration(123), 333)
    self.assertEqual(-Duration(123), -123)

  def test_sort_order(self):
    self.assertEqual([-63, Duration(-3), 2, 9, Duration(292.3), 500],
                     sorted([9, 2, Duration(-3), Duration(292.3), -63, 500]))
    self.assertEqual([4, 5, Duration(6), Duration(7), 8, 9],
                     sorted([9, 8, Duration(7), Duration(6), 5, 4]))

  def test_str(self):
    self.assertEqual('Duration(1.234567)', str(Duration(1.234567)))
    self.assertEqual('Duration(-1.234567)', str(Duration(-1.234567)))
    self.assertEqual('Duration(-999999999.900000)', str(Duration(-999999999.9)))
    self.assertEqual('Duration(999999999)', str(Duration(999999999)))
    self.assertEqual('Duration(-999999999)', str(Duration(-999999999)))

  def test_from_proto(self):
    dur_proto = duration_pb2.Duration(seconds=1234, nanos=56000)
    actual_dur = Duration.from_proto(dur_proto)
    expected_dur = Duration(seconds=1234, micros=56)
    self.assertEqual(actual_dur, expected_dur)

  def test_from_proto_fails_with_truncation(self):
    # TODO(BEAM-8738): Better define durations.
    with self.assertRaises(ValueError):
      Duration.from_proto(duration_pb2.Duration(seconds=1234, nanos=56789))

  def test_to_proto(self):
    dur = Duration(seconds=1234, micros=56)
    actual_dur_proto = Duration.to_proto(dur)
    expected_dur_proto = duration_pb2.Duration(seconds=1234, nanos=56000)
    self.assertEqual(actual_dur_proto, expected_dur_proto)


if __name__ == '__main__':
  unittest.main()
