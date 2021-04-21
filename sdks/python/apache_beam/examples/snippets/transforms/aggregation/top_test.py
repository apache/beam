# coding=utf-8
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

import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import top


def check_largest_elements(actual):
  expected = '''[START largest_elements]
[4, 3]
[END largest_elements]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_largest_elements_per_key(actual):
  expected = '''[START largest_elements_per_key]
('🥕', [3, 2])
('🍆', [1])
('🍅', [5, 4])
[END largest_elements_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_smallest_elements(actual):
  expected = '''[START smallest_elements]
[1, 2]
[END smallest_elements]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_smallest_elements_per_key(actual):
  expected = '''[START smallest_elements_per_key]
('🥕', [2, 3])
('🍆', [1])
('🍅', [3, 4])
[END smallest_elements_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_shortest_elements(actual):
  expected = '''[START shortest_elements]
['🌽 Corn', '🥕 Carrot']
[END shortest_elements]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_shortest_elements_per_key(actual):
  expected = '''[START shortest_elements_per_key]
('spring', ['🥕 Carrot', '🍓 Strawberry'])
('summer', ['🌽 Corn', '🥕 Carrot'])
('fall', ['🥕 Carrot', '🍏 Green apple'])
('winter', ['🍆 Eggplant'])
[END shortest_elements_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.top.print', str)
class TopTest(unittest.TestCase):
  def test_top_largest(self):
    top.top_largest(check_largest_elements)

  def test_top_largest_per_key(self):
    top.top_largest_per_key(check_largest_elements_per_key)

  def test_top_smallest(self):
    top.top_smallest(check_smallest_elements)

  def test_top_smallest_per_key(self):
    top.top_smallest_per_key(check_smallest_elements_per_key)

  def test_top_of(self):
    top.top_of(check_shortest_elements)

  def test_top_per_key(self):
    top.top_per_key(check_shortest_elements_per_key)


if __name__ == '__main__':
  unittest.main()
