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

from . import flatmap


def check_plants(actual):
  expected = '''[START plants]
🍓Strawberry
🥕Carrot
🍆Eggplant
🍅Tomato
🥔Potato
[END plants]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_valid_plants(actual):
  expected = '''[START valid_plants]
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
[END valid_plants]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap.print', str)
class FlatMapTest(unittest.TestCase):
  def test_flatmap_simple(self):
    flatmap.flatmap_simple(check_plants)

  def test_flatmap_function(self):
    flatmap.flatmap_function(check_plants)

  def test_flatmap_lambda(self):
    flatmap.flatmap_lambda(check_plants)

  def test_flatmap_generator(self):
    flatmap.flatmap_generator(check_plants)

  def test_flatmap_multiple_arguments(self):
    flatmap.flatmap_multiple_arguments(check_plants)

  def test_flatmap_tuple(self):
    flatmap.flatmap_tuple(check_plants)

  def test_flatmap_side_inputs_singleton(self):
    flatmap.flatmap_side_inputs_singleton(check_plants)

  def test_flatmap_side_inputs_iter(self):
    flatmap.flatmap_side_inputs_iter(check_valid_plants)

  def test_flatmap_side_inputs_dict(self):
    flatmap.flatmap_side_inputs_dict(check_valid_plants)


if __name__ == '__main__':
  unittest.main()
