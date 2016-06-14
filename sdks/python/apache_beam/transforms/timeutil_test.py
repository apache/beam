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

from __future__ import absolute_import

import unittest

from apache_beam.transforms.timeutil import Duration
from apache_beam.transforms.timeutil import Timestamp


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
    self.assertEqual(Timestamp(10000000).isoformat(),
                     '1970-04-26T17:46:40Z')
    self.assertEqual(Timestamp(10000000.000001).isoformat(),
                     '1970-04-26T17:46:40.000001Z')
    self.assertEqual(Timestamp(1458343379.123456).isoformat(),
                     '2016-03-18T23:22:59.123456Z')

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
    self.assertEqual(
        [-63, Timestamp(-3), 2, 9, Timestamp(292.3), 500],
        sorted([9, 2, Timestamp(-3), Timestamp(292.3), -63, 500]))
    self.assertEqual(
        [4, 5, Timestamp(6), Timestamp(7), 8, 9],
        sorted([9, 8, Timestamp(7), Timestamp(6), 5, 4]))

  def test_str(self):
    self.assertEqual('Timestamp(1.234567)',
                     str(Timestamp(1.234567)))
    self.assertEqual('Timestamp(-1.234567)',
                     str(Timestamp(-1.234567)))
    self.assertEqual('Timestamp(-999999999.900000)',
                     str(Timestamp(-999999999.9)))
    self.assertEqual('Timestamp(999999999)',
                     str(Timestamp(999999999)))
    self.assertEqual('Timestamp(-999999999)',
                     str(Timestamp(-999999999)))


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
    self.assertEqual(
        [-63, Duration(-3), 2, 9, Duration(292.3), 500],
        sorted([9, 2, Duration(-3), Duration(292.3), -63, 500]))
    self.assertEqual(
        [4, 5, Duration(6), Duration(7), 8, 9],
        sorted([9, 8, Duration(7), Duration(6), 5, 4]))

  def test_str(self):
    self.assertEqual('Duration(1.234567)',
                     str(Duration(1.234567)))
    self.assertEqual('Duration(-1.234567)',
                     str(Duration(-1.234567)))
    self.assertEqual('Duration(-999999999.900000)',
                     str(Duration(-999999999.9)))
    self.assertEqual('Duration(999999999)',
                     str(Duration(999999999)))
    self.assertEqual('Duration(-999999999)',
                     str(Duration(-999999999)))


if __name__ == '__main__':
  unittest.main()
