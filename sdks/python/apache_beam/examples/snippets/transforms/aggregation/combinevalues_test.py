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

from . import combinevalues


def check_total(actual):
  expected = '''[START total]
('🥕', 5)
('🍆', 1)
('🍅', 12)
[END total]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_saturated_total(actual):
  expected = '''[START saturated_total]
('🥕', 5)
('🍆', 1)
('🍅', 8)
[END saturated_total]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_bounded_total(actual):
  expected = '''[START bounded_total]
('🥕', 5)
('🍆', 2)
('🍅', 8)
[END bounded_total]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_percentages_per_season(actual):
  expected = '''[START percentages_per_season]
('spring', {'🥕': 0.4, '🍅': 0.4, '🍆': 0.2})
('summer', {'🥕': 0.2, '🍅': 0.6, '🌽': 0.2})
('fall', {'🥕': 0.5, '🍅': 0.5})
('winter', {'🍆': 1.0})
[END percentages_per_season]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combinevalues.print',
    str)
class CombineValuesTest(unittest.TestCase):
  def test_combinevalues_function(self):
    combinevalues.combinevalues_function(check_saturated_total)

  def test_combinevalues_lambda(self):
    combinevalues.combinevalues_lambda(check_saturated_total)

  def test_combinevalues_multiple_arguments(self):
    combinevalues.combinevalues_multiple_arguments(check_saturated_total)

  def test_combinevalues_side_inputs_singleton(self):
    combinevalues.combinevalues_side_inputs_singleton(check_saturated_total)

  def test_combinevalues_side_inputs_iter(self):
    combinevalues.combinevalues_side_inputs_iter(check_bounded_total)

  def test_combinevalues_side_inputs_dict(self):
    combinevalues.combinevalues_side_inputs_dict(check_bounded_total)

  def test_combinevalues_combinefn(self):
    combinevalues.combinevalues_combinefn(check_percentages_per_season)


if __name__ == '__main__':
  unittest.main()
