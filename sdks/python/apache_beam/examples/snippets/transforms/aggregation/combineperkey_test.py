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

from . import combineperkey_combinefn
from . import combineperkey_function
from . import combineperkey_lambda
from . import combineperkey_multiple_arguments
from . import combineperkey_side_inputs_dict
from . import combineperkey_side_inputs_iter
from . import combineperkey_side_inputs_singleton
from . import combineperkey_simple


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


def check_average(actual):
  expected = '''[START average]
('🥕', 2.5)
('🍆', 1.0)
('🍅', 4.0)
[END average]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_simple.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_function.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_lambda.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_multiple_arguments.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_multiple_arguments.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_side_inputs_singleton.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_side_inputs_iter.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_side_inputs_dict.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.combineperkey_combinefn.print',
    str)
class CombinePerKeyTest(unittest.TestCase):
  def test_combineperkey_simple(self):
    combineperkey_simple.combineperkey_simple(check_total)

  def test_combineperkey_function(self):
    combineperkey_function.combineperkey_function(check_saturated_total)

  def test_combineperkey_lambda(self):
    combineperkey_lambda.combineperkey_lambda(check_saturated_total)

  def test_combineperkey_multiple_arguments(self):
    combineperkey_multiple_arguments.combineperkey_multiple_arguments(
        check_saturated_total)

  def test_combineperkey_side_inputs_singleton(self):
    combineperkey_side_inputs_singleton.combineperkey_side_inputs_singleton(
        check_saturated_total)

  def test_combineperkey_side_inputs_iter(self):
    combineperkey_side_inputs_iter.combineperkey_side_inputs_iter(
        check_bounded_total)

  def test_combineperkey_side_inputs_dict(self):
    combineperkey_side_inputs_dict.combineperkey_side_inputs_dict(
        check_bounded_total)

  def test_combineperkey_combinefn(self):
    combineperkey_combinefn.combineperkey_combinefn(check_average)


if __name__ == '__main__':
  unittest.main()
