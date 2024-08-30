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
# pylint:disable=line-too-long

import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from . import map_context
from . import map_function
from . import map_lambda
from . import map_multiple_arguments
from . import map_side_inputs_dict
from . import map_side_inputs_iter
from . import map_side_inputs_singleton
from . import map_simple
from . import map_tuple


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
    'apache_beam.examples.snippets.transforms.elementwise.map_simple.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_function.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_lambda.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_multiple_arguments.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_tuple.print', str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_side_inputs_singleton.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_side_inputs_iter.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_side_inputs_dict.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.map_context.print',
    str)
class MapTest(unittest.TestCase):
  def test_map_simple(self):
    map_simple.map_simple(check_plants)

  def test_map_function(self):
    map_function.map_function(check_plants)

  def test_map_lambda(self):
    map_lambda.map_lambda(check_plants)

  def test_map_multiple_arguments(self):
    map_multiple_arguments.map_multiple_arguments(check_plants)

  def test_map_tuple(self):
    map_tuple.map_tuple(check_plants)

  def test_map_side_inputs_singleton(self):
    map_side_inputs_singleton.map_side_inputs_singleton(check_plants)

  def test_map_side_inputs_iter(self):
    map_side_inputs_iter.map_side_inputs_iter(check_plants)

  def test_map_side_inputs_dict(self):
    map_side_inputs_dict.map_side_inputs_dict(check_plant_details)

  def test_map_context(self):
    import re

    def check_nonces(output):
      def shares_same_nonces(elements):
        s = set(re.search(r'\d+ \d+', e).group(0) for e in elements)
        assert len(s) == 1, s

      assert_that(output, shares_same_nonces)

    map_context.map_context(check_nonces)


if __name__ == '__main__':
  unittest.main()
