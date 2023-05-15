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

from . import tostring_element
from . import tostring_iterables
from . import tostring_kvs


def check_plants(actual):
  expected = '''[START plants]
🍓,Strawberry
🥕,Carrot
🍆,Eggplant
🍅,Tomato
🥔,Potato
[END plants]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_plant_lists(actual):
  expected = '''[START plant_lists]
['🍓', 'Strawberry', 'perennial']
['🥕', 'Carrot', 'biennial']
['🍆', 'Eggplant', 'perennial']
['🍅', 'Tomato', 'annual']
['🥔', 'Potato', 'perennial']
[END plant_lists]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_plants_csv(actual):
  expected = '''[START plants_csv]
🍓,Strawberry,perennial
🥕,Carrot,biennial
🍆,Eggplant,perennial
🍅,Tomato,annual
🥔,Potato,perennial
[END plants_csv]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.tostring_kvs.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.tostring_element.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.tostring_iterables.print',
    str)
class ToStringTest(unittest.TestCase):
  def test_tostring_kvs(self):
    tostring_kvs.tostring_kvs(check_plants)

  def test_tostring_element(self):
    tostring_element.tostring_element(check_plant_lists)

  def test_tostring_iterables(self):
    tostring_iterables.tostring_iterables(check_plants_csv)


if __name__ == '__main__':
  unittest.main()
