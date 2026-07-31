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
      Timestamp.from_utc_datetime(
          datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern')))
    with self.assertRaisesRegex(ValueError, r'dt has no timezone info'):
      Timestamp.from_utc_datetime(datetime.datetime(1970, 1, 1, tzinfo=None))

  def test_from_to_utc_datetime(self):
    timestamp = Timestamp(seconds=1458343379.123456)
    dt = timestamp.to_utc_datetime(has_tz=True)
    self.assertEqual(timestamp, Timestamp.from_utc_datetime(dt))

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
      self.assertEqual(-Timestamp(123), -123)  # pylint: disable=invalid-unary-operand-type
    with self.assertRaises(TypeError):
      self.assertEqual(-Timestamp(123), -Duration(123))  # pylint: disable=invalid-unary-operand-type
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

  def test_from_proto_with_sub_micro_nanos(self):
    # Sub-microsecond protos produce a nanosecond-precision Timestamp
    # instead of losing precision (or raising, as this method used to).
    actual_ts = Timestamp.from_proto(
        timestamp_pb2.Timestamp(seconds=1234, nanos=56789))
    self.assertEqual(actual_ts.precision(), Timestamp.NANOS_PRECISION)
    self.assertEqual(actual_ts.nanos, 1234 * 10**9 + 56789)

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


class TimestampPrecisionTest(unittest.TestCase):
  def test_constructor(self):
    ts = Timestamp(seconds=1234, subseconds=123456789, precision=9)
    self.assertEqual(ts.seconds(), 1234)
    self.assertEqual(ts.subseconds(), 123456789)
    self.assertEqual(ts.precision(), 9)
    self.assertEqual(ts.nanos, 1234123456789)

    # Subseconds overflowing a second carry into seconds.
    ts = Timestamp(seconds=1, subseconds=1500, precision=3)
    self.assertEqual(ts.seconds(), 2)
    self.assertEqual(ts.subseconds(), 500)

    # Negative timestamps floor seconds so subseconds stay non-negative.
    ts = Timestamp(seconds=-2, subseconds=500000, precision=6)
    self.assertEqual(ts.seconds(), -2)
    self.assertEqual(ts.subseconds(), 500000)
    self.assertEqual(ts, Timestamp(-1.5))

  def test_constructor_validation(self):
    with self.assertRaises(ValueError):
      Timestamp(0, 0, precision=10)
    with self.assertRaises(ValueError):
      Timestamp(0, 0, precision=-1)
    with self.assertRaises(TypeError):
      Timestamp(0, 0, precision=6.0)  # type: ignore[arg-type]
    # micros is an alias for subseconds at the default precision only.
    with self.assertRaises(ValueError):
      Timestamp(0, subseconds=1, micros=1)
    with self.assertRaises(ValueError):
      Timestamp(0, precision=9, micros=1)
    self.assertEqual(Timestamp(1, micros=500000), Timestamp(1.5))

  def test_default_precision_is_micros(self):
    self.assertEqual(Timestamp(1.5).precision(), Timestamp.MICROS_PRECISION)
    self.assertEqual(Timestamp.now().precision(), Timestamp.MICROS_PRECISION)

  def test_equality_across_precisions(self):
    self.assertEqual(
        Timestamp(seconds=1, subseconds=500, precision=3),
        Timestamp(seconds=1, subseconds=500000000, precision=9))
    self.assertEqual(
        hash(Timestamp(seconds=1, subseconds=500, precision=3)),
        hash(Timestamp(seconds=1, subseconds=500000000, precision=9)))
    # An equal Duration must hash equal too.
    self.assertEqual(hash(Timestamp(micros=5)), hash(Duration(micros=5)))
    self.assertNotEqual(
        Timestamp(seconds=1, subseconds=500000001, precision=9),
        Timestamp(seconds=1, subseconds=500000, precision=6))

  def test_comparison(self):
    self.assertLess(
        Timestamp(seconds=1, subseconds=500000000, precision=9),
        Timestamp(seconds=1, subseconds=500001, precision=6))
    self.assertGreater(
        Timestamp(seconds=1, subseconds=500000001, precision=9),
        Timestamp(seconds=1, subseconds=500000, precision=6))
    self.assertLess(Timestamp(2.1), Timestamp(3))
    self.assertGreater(Timestamp(3), Timestamp(2.1))

  def test_str(self):
    self.assertEqual('Timestamp(1.500)', str(Timestamp(1, 500, precision=3)))
    self.assertEqual(
        'Timestamp(1.123456789)', str(Timestamp(1, 123456789, precision=9)))
    self.assertEqual(
        'Timestamp(-1.500000000)', str(Timestamp(-1.5, precision=9)))
    self.assertEqual('Timestamp(1)', str(Timestamp(1, precision=9)))
    self.assertEqual('Timestamp(1)', str(Timestamp(1, precision=0)))

  def test_precision_conversion(self):
    ts = Timestamp(seconds=1, subseconds=123456789, precision=9)
    self.assertIs(ts.to_precision(9), ts)
    # Lossless upward conversion.
    up = Timestamp(1.5).to_precision(9)
    self.assertEqual(up.precision(), 9)
    self.assertEqual(up, Timestamp(1.5))
    # Lossy downward conversion requires explicit permission and floors.
    with self.assertRaises(ValueError):
      ts.to_precision(6)
    truncated = ts.to_precision(6, allow_lossy_conversion=True)
    self.assertEqual(truncated.precision(), 6)
    self.assertEqual(truncated.micros, 1123456)
    # A lossless downward conversion doesn't require the flag.
    self.assertEqual(Timestamp(1.5, precision=9).to_precision(3).precision(), 3)
    # Truncation of negative timestamps floors towards negative infinity.
    negative = Timestamp(
        seconds=-1, precision=9).predecessor().to_precision(
            6, allow_lossy_conversion=True)
    self.assertEqual(negative, Timestamp(-1) - Duration(micros=1))

  def test_micros_guard_rail(self):
    ts = Timestamp(seconds=1, subseconds=123456789, precision=9)
    with self.assertRaises(ValueError):
      _ = ts.micros
    self.assertEqual(ts.nanos, 1123456789)
    self.assertEqual(Timestamp(1, 500, precision=3).micros, 1500000)

  def test_predecessor_successor(self):
    ts = Timestamp(seconds=10, precision=9)
    self.assertEqual(ts.predecessor().nanos, 10 * 10**9 - 1)
    self.assertEqual(ts.successor().nanos, 10 * 10**9 + 1)
    self.assertEqual(ts.predecessor().precision(), 9)
    self.assertEqual(ts.successor().seconds(), 10)
    self.assertEqual(ts.predecessor().seconds(), 9)
    # Micros-precision timestamps keep their historical 1-micro step.
    self.assertEqual(Timestamp(10).predecessor().micros, 10 * 10**6 - 1)

  def test_to_utc_datetime_guard_rail(self):
    ts = Timestamp(seconds=1234, subseconds=123456789, precision=9)
    with self.assertRaises(ValueError):
      ts.to_utc_datetime()
    dt = ts.to_utc_datetime(allow_lossy_conversion=True)
    self.assertEqual(dt.microsecond, 123456)
    # Timestamps at or below microsecond precision convert freely.
    Timestamp(1.5).to_utc_datetime()

  def test_to_rfc3339_is_lossless(self):
    ts = Timestamp(seconds=1458343379, subseconds=123456789, precision=9)
    self.assertEqual(ts.to_rfc3339(), '2016-03-18T23:22:59.123456789Z')
    self.assertEqual(Timestamp.from_rfc3339(ts.to_rfc3339()), ts)
    # Whole seconds have no fractional digits, as before.
    self.assertEqual(
        Timestamp(1458343379, precision=9).to_rfc3339(), '2016-03-18T23:22:59Z')

  def test_from_rfc3339_with_nanos(self):
    ts = Timestamp.from_rfc3339('2016-03-18T23:22:59.123456789Z')
    self.assertEqual(ts.precision(), 9)
    self.assertEqual(ts.subseconds(), 123456789)
    # Precision matches the number of fractional digits (above 6).
    ts = Timestamp.from_rfc3339('2016-03-18T23:22:59.1234567Z')
    self.assertEqual(ts.precision(), 7)
    self.assertEqual(ts.subseconds(), 1234567)
    self.assertEqual(Timestamp.from_rfc3339(ts.to_rfc3339()), ts)
    self.assertEqual(
        Timestamp.from_rfc3339('2016-03-18T23:22:59.123Z').precision(), 6)
    with self.assertRaises(ValueError):
      Timestamp.from_rfc3339('2016-03-18T23:22:59.1234567891Z')

  def test_from_rfc3339_with_comma_separator(self):
    # ISO 8601 also allows ',' as the decimal separator.
    ts = Timestamp.from_rfc3339('2016-03-18T23:22:59,123456789Z')
    self.assertEqual(
        ts, Timestamp.from_rfc3339('2016-03-18T23:22:59.123456789Z'))
    self.assertEqual(ts.subseconds(), 123456789)

  def test_proto_round_trip_with_nanos(self):
    ts = Timestamp(seconds=1234, subseconds=123456789, precision=9)
    self.assertEqual(Timestamp.from_proto(ts.to_proto()), ts)
    # Negative timestamps use non-negative proto nanos.
    ts = Timestamp(-1.5, precision=9).predecessor()
    proto = ts.to_proto()
    self.assertEqual(proto.seconds, -2)
    self.assertEqual(proto.nanos, 499999999)
    self.assertEqual(Timestamp.from_proto(proto), ts)

  def test_arithmetic_preserves_precision(self):
    ts = Timestamp(seconds=1, subseconds=123456789, precision=9)
    self.assertEqual((ts + 1).nanos, ts.nanos + 10**9)
    self.assertEqual((ts + 1).precision(), 9)
    self.assertEqual((ts - Duration(micros=1)).nanos, ts.nanos - 1000)
    # Sub-micros precision arithmetic results are widened to micros.
    self.assertEqual((Timestamp(1, 500, precision=3) + 0.5).precision(), 6)

  def test_duration_arithmetic_guard_rail(self):
    ts = Timestamp(seconds=1, subseconds=123456789, precision=9)
    # Exact differences are representable as a (micros) Duration.
    self.assertEqual(ts - (ts - Duration(micros=3)), Duration(micros=3))
    self.assertEqual(ts + Duration(micros=5) - ts, Duration(micros=5))
    # Sub-microsecond differences are not.
    with self.assertRaises(ValueError):
      _ = ts - ts.predecessor()
    with self.assertRaises(ValueError):
      _ = ts % Duration(seconds=1)


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
    # TODO(https://github.com/apache/beam/issues/19922): Better define
    # durations.
    with self.assertRaises(ValueError):
      Duration.from_proto(duration_pb2.Duration(seconds=1234, nanos=56789))

  def test_to_proto(self):
    dur = Duration(seconds=1234, micros=56)
    actual_dur_proto = Duration.to_proto(dur)
    expected_dur_proto = duration_pb2.Duration(seconds=1234, nanos=56000)
    self.assertEqual(actual_dur_proto, expected_dur_proto)


if __name__ == '__main__':
  unittest.main()
