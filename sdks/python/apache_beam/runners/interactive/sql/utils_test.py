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

"""Tests for utils module."""

# pytype: skip-file

import unittest
from typing import NamedTuple
from unittest.mock import patch

import apache_beam as beam
from apache_beam.runners.interactive.sql.utils import find_pcolls
from apache_beam.runners.interactive.sql.utils import is_namedtuple
from apache_beam.runners.interactive.sql.utils import pformat_namedtuple
from apache_beam.runners.interactive.sql.utils import register_coder_for_schema
from apache_beam.runners.interactive.sql.utils import replace_single_pcoll_token


class ANamedTuple(NamedTuple):
  a: int
  b: str


class UtilsTest(unittest.TestCase):
  def test_is_namedtuple(self):
    class AType:
      pass

    a_type = AType
    a_tuple = type((1, 2, 3))

    a_namedtuple = ANamedTuple

    self.assertTrue(is_namedtuple(a_namedtuple))
    self.assertFalse(is_namedtuple(a_type))
    self.assertFalse(is_namedtuple(a_tuple))

  def test_register_coder_for_schema(self):
    self.assertNotIsInstance(
        beam.coders.registry.get_coder(ANamedTuple), beam.coders.RowCoder)
    register_coder_for_schema(ANamedTuple)
    self.assertIsInstance(
        beam.coders.registry.get_coder(ANamedTuple), beam.coders.RowCoder)

  def test_find_pcolls(self):
    with patch('apache_beam.runners.interactive.interactive_beam.collect',
               lambda _: None):
      found = find_pcolls(
          """SELECT * FROM pcoll_1 JOIN pcoll_2
          USING (common_column)""", {
              'pcoll_1': None, 'pcoll_2': None
          })
      self.assertIn('pcoll_1', found)
      self.assertIn('pcoll_2', found)

  def test_replace_single_pcoll_token(self):
    sql = 'SELECT * FROM abc WHERE a=1 AND b=2'
    replaced_sql = replace_single_pcoll_token(sql, 'wow')
    self.assertEqual(replaced_sql, sql)
    replaced_sql = replace_single_pcoll_token(sql, 'abc')
    self.assertEqual(
        replaced_sql, 'SELECT * FROM PCOLLECTION WHERE a=1 AND b=2')

  def test_pformat_namedtuple(self):
    self.assertEqual(
        'ANamedTuple(a: int, b: str)', pformat_namedtuple(ANamedTuple))


if __name__ == '__main__':
  unittest.main()
