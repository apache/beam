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

from . import map


def check_plants(actual):
  expected = '''[START plants]
🍓Strawberry
🥕Carrot
🍆Eggplant
🍅Tomato
🥔Potato
[END plants]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_plant_details(actual):
  expected = '''[START plant_details]
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END plant_details]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map.print', str)
class MapTest(unittest.TestCase):
  def test_map_simple(self):
    map.map_simple(check_plants)

  def test_map_function(self):
    map.map_function(check_plants)

  def test_map_lambda(self):
    map.map_lambda(check_plants)

  def test_map_multiple_arguments(self):
    map.map_multiple_arguments(check_plants)

  def test_map_tuple(self):
    map.map_tuple(check_plants)

  def test_map_side_inputs_singleton(self):
    map.map_side_inputs_singleton(check_plants)

  def test_map_side_inputs_iter(self):
    map.map_side_inputs_iter(check_plants)

  def test_map_side_inputs_dict(self):
    map.map_side_inputs_dict(check_plant_details)


if __name__ == '__main__':
  unittest.main()
