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

from . import filter_function
from . import filter_lambda
from . import filter_multiple_arguments
from . import filter_side_inputs_dict
from . import filter_side_inputs_iter
from . import filter_side_inputs_singleton


def check_perennials(actual):
  expected = '''[START perennials]
{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END perennials]'''.splitlines()[1:-1]
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
    'apache_beam.examples.snippets.transforms.elementwise.filter_function.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.filter_lambda.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.filter_multiple_arguments.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.filter_side_inputs_singleton.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.filter_side_inputs_iter.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.filter_side_inputs_dict.print',
    str)
class FilterTest(unittest.TestCase):
  def test_filter_function(self):
    filter_function.filter_function(check_perennials)

  def test_filter_lambda(self):
    filter_lambda.filter_lambda(check_perennials)

  def test_filter_multiple_arguments(self):
    filter_multiple_arguments.filter_multiple_arguments(check_perennials)

  def test_filter_side_inputs_singleton(self):
    filter_side_inputs_singleton.filter_side_inputs_singleton(check_perennials)

  def test_filter_side_inputs_iter(self):
    filter_side_inputs_iter.filter_side_inputs_iter(check_valid_plants)

  def test_filter_side_inputs_dict(self):
    filter_side_inputs_dict.filter_side_inputs_dict(check_perennials)


if __name__ == '__main__':
  unittest.main()
