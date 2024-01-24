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
import unittest

from apache_beam.yaml.yaml_provider import YamlProviders
from apache_beam.yaml.yaml_provider import parse_callable_kwargs


class WindowIntoTest(unittest.TestCase):
  def __init__(self, methodName="runWindowIntoTest"):
    unittest.TestCase.__init__(self, methodName)
    self.parse_duration = YamlProviders.WindowInto._parse_duration

  def test_parse_duration_ms(self):
    value = self.parse_duration('1000ms', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_sec(self):
    value = self.parse_duration('1s', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_min(self):
    value = self.parse_duration('1m', 'size')
    self.assertEqual(60, value)

  def test_parse_duration_hour(self):
    value = self.parse_duration('1h', 'size')
    self.assertEqual(3600, value)

  def test_parse_duration_from_decimal(self):
    value = self.parse_duration('1.5m', 'size')
    self.assertEqual(90, value)

  def test_parse_duration_to_decimal(self):
    value = self.parse_duration('1ms', 'size')
    self.assertEqual(0.001, value)

  def test_parse_duration_with_missing_suffix(self):
    value = self.parse_duration('1', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_with_invalid_suffix(self):
    with self.assertRaises(ValueError):
      self.parse_duration('1x', 'size')

  def test_parse_duration_with_missing_value(self):
    with self.assertRaises(ValueError):
      self.parse_duration('s', 'size')


class CallableKwargsTest(unittest.TestCase):
  def __init__(self, methodName="runCallableKwargsTest"):
    unittest.TestCase.__init__(self, methodName)

  def test_parse_callable_kwargs_without_callable(self):
    kwargs = {'n': 2, 'key': 'a', 'reverse': True}
    expected = {'n': 2, 'key': 'a', 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_callable(self):
    kwargs = {'n': 2, 'key': {'callable': 'len'}, 'reverse': True}
    expected = {'n': 2, 'key': len, 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_nested_value(self):
    kwargs = {'n': {'m': 2}, 'key': 'a', 'reverse': True}
    expected = {'n': {'m': 2}, 'key': 'a', 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_nested_callable(self):
    kwargs = {'n': {'m': {'callable': 'len'}}, 'key': 'a', 'reverse': True}
    expected = {'n': {'m': len}, 'key': 'a', 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_top_level_callable(self):
    kwargs = {'n': 2, 'callable': 'len', 'reverse': True}
    expected = {'n': 2, 'callable': 'len', 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_double_nested_callable(self):
    kwargs = {'n': 2, 'callable': {'callable': 'len'}, 'reverse': True}
    expected = {'n': 2, 'callable': len, 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_double_nested_callable_under_value(self):
    kwargs = {'n': 2, 'key': {'callable': {'callable': 'len'}}, 'reverse': True}
    expected = {'n': 2, 'key': {'callable': len}, 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_multiple_values(self):
    kwargs = {'n': 2, 'key': {'inner': 2, 'val': 'foo'}, 'reverse': True}
    expected = {'n': 2, 'key': {'inner': 2, 'val': 'foo'}, 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_multiple_values_callable(self):
    kwargs = {'n': 2, 'key': {'val': 2, 'callable': 'len'}, 'reverse': True}
    expected = {'n': 2, 'key': {'val': 2, 'callable': 'len'}, 'reverse': True}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_multiple_values_nested_callable(self):
    kwargs = {'n': 2, 'key': {'inner': 2, 'callable': {'callable': 'len'}}}
    expected = {'n': 2, 'key': {'inner': 2, 'callable': len}}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)

  def test_parse_callable_kwargs_with_only_callable(self):
    kwargs = {'callable': 'len'}
    expected = {'callable': 'len'}
    actual = parse_callable_kwargs(kwargs)
    self.assertEqual(expected, actual)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
