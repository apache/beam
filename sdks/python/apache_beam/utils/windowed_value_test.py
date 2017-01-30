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

"""Unit tests for the windowed_value."""

import copy
import pickle
import unittest

from apache_beam.utils import windowed_value
from apache_beam.transforms.timeutil import Timestamp


class WindowedValueTest(unittest.TestCase):

  def test_timestamps(self):
    wv = windowed_value.WindowedValue(None, 3, ())
    self.assertEqual(wv.timestamp, Timestamp.of(3))
    self.assertTrue(wv.timestamp is wv.timestamp)
    self.assertEqual(windowed_value.WindowedValue(None, -2.5, ()).timestamp,
                     Timestamp.of(-2.5))

  def test_with_value(self):
    wv = windowed_value.WindowedValue(1, 3, ())
    self.assertEqual(wv.with_value(10), windowed_value.WindowedValue(10, 3, ()))

  def test_equality(self):
    self.assertEqual(
        windowed_value.WindowedValue(1, 3, ()),
        windowed_value.WindowedValue(1, 3, ()))
    self.assertNotEqual(
        windowed_value.WindowedValue(1, 3, ()),
        windowed_value.WindowedValue(100, 3, ()))
    self.assertNotEqual(
        windowed_value.WindowedValue(1, 3, ()),
        windowed_value.WindowedValue(1, 300, ()))
    self.assertNotEqual(
        windowed_value.WindowedValue(1, 3, ()),
        windowed_value.WindowedValue(1, 300, ((),)))

    self.assertNotEqual(
        windowed_value.WindowedValue(1, 3, ()),
        object())

  def test_hash(self):
    wv = windowed_value.WindowedValue(1, 3, ())
    wv_copy = copy.copy(wv)
    self.assertFalse(wv is wv_copy)
    self.assertEqual({wv: 100}.get(wv_copy), 100)

  def test_pickle(self):
    wv = windowed_value.WindowedValue(1, 3, ())
    self.assertTrue(pickle.loads(pickle.dumps(wv)) == wv)


if __name__ == '__main__':
  unittest.main()
