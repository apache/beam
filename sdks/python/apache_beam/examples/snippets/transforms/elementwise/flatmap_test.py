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

from . import flatmap_function
from . import flatmap_generator
from . import flatmap_lambda
from . import flatmap_multiple_arguments
from . import flatmap_nofunction
from . import flatmap_side_inputs_dict
from . import flatmap_side_inputs_iter
from . import flatmap_side_inputs_singleton
from . import flatmap_simple
from . import flatmap_tuple


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
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_simple.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_function.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_nofunction.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_lambda.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_generator.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_multiple_arguments.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_tuple.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_side_inputs_singleton.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_side_inputs_iter.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.flatmap_side_inputs_dict.print',
    str)
class FlatMapTest(unittest.TestCase):
  def test_flatmap_simple(self):
    flatmap_simple.flatmap_simple(check_plants)

  def test_flatmap_function(self):
    flatmap_function.flatmap_function(check_plants)

  def test_flatmap_nofunction(self):
    flatmap_nofunction.flatmap_nofunction(check_plants)

  def test_flatmap_lambda(self):
    flatmap_lambda.flatmap_lambda(check_plants)

  def test_flatmap_generator(self):
    flatmap_generator.flatmap_generator(check_plants)

  def test_flatmap_multiple_arguments(self):
    flatmap_multiple_arguments.flatmap_multiple_arguments(check_plants)

  def test_flatmap_tuple(self):
    flatmap_tuple.flatmap_tuple(check_plants)

  def test_flatmap_side_inputs_singleton(self):
    flatmap_side_inputs_singleton.flatmap_side_inputs_singleton(check_plants)

  def test_flatmap_side_inputs_iter(self):
    flatmap_side_inputs_iter.flatmap_side_inputs_iter(check_valid_plants)

  def test_flatmap_side_inputs_dict(self):
    flatmap_side_inputs_dict.flatmap_side_inputs_dict(check_valid_plants)


if __name__ == '__main__':
  unittest.main()
