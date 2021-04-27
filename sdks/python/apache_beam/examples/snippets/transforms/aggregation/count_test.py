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

# pytype: skip-file

import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import count


def check_total_elements(actual):
  expected = '''[START total_elements]
10
[END total_elements]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_total_elements_per_key(actual):
  expected = '''[START total_elements_per_key]
('spring', 4)
('summer', 3)
('fall', 2)
('winter', 1)
[END total_elements_per_key]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_total_unique_elements(actual):
  expected = '''[START total_unique_elements]
('🍓', 1)
('🥕', 3)
('🍆', 2)
('🍅', 3)
('🌽', 1)
[END total_unique_elements]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.count.print', str)
class CountTest(unittest.TestCase):
  def test_count_globally(self):
    count.count_globally(check_total_elements)

  def test_count_per_key(self):
    count.count_per_key(check_total_elements_per_key)

  def test_count_per_element(self):
    count.count_per_element(check_total_unique_elements)


if __name__ == '__main__':
  unittest.main()
