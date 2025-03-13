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

from . import regex_all_matches
from . import regex_find
from . import regex_find_all
from . import regex_find_kv
from . import regex_matches
from . import regex_matches_kv
from . import regex_replace_all
from . import regex_replace_first
from . import regex_split


def check_matches(actual):
  expected = '''[START plants_matches]
🍓, Strawberry, perennial
🥕, Carrot, biennial
🍆, Eggplant, perennial
🍅, Tomato, annual
🥔, Potato, perennial
[END plants_matches]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_all_matches(actual):
  expected = '''[START plants_all_matches]
['🍓, Strawberry, perennial', '🍓', 'Strawberry', 'perennial']
['🥕, Carrot, biennial', '🥕', 'Carrot', 'biennial']
['🍆, Eggplant, perennial', '🍆', 'Eggplant', 'perennial']
['🍅, Tomato, annual', '🍅', 'Tomato', 'annual']
['🥔, Potato, perennial', '🥔', 'Potato', 'perennial']
[END plants_all_matches]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_matches_kv(actual):
  expected = '''[START plants_matches_kv]
('🍓', '🍓, Strawberry, perennial')
('🥕', '🥕, Carrot, biennial')
('🍆', '🍆, Eggplant, perennial')
('🍅', '🍅, Tomato, annual')
('🥔', '🥔, Potato, perennial')
[END plants_matches_kv]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_find_all(actual):
  expected = '''[START plants_find_all]
['🍓, Strawberry, perennial']
['🥕, Carrot, biennial']
['🍆, Eggplant, perennial', '🍌, Banana, perennial']
['🍅, Tomato, annual', '🍉, Watermelon, annual']
['🥔, Potato, perennial']
[END plants_find_all]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_find_kv(actual):
  expected = '''[START plants_find_kv]
('🍓', '🍓, Strawberry, perennial')
('🥕', '🥕, Carrot, biennial')
('🍆', '🍆, Eggplant, perennial')
('🍌', '🍌, Banana, perennial')
('🍅', '🍅, Tomato, annual')
('🍉', '🍉, Watermelon, annual')
('🥔', '🥔, Potato, perennial')
[END plants_find_kv]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_replace_all(actual):
  expected = '''[START plants_replace_all]
🍓,Strawberry,perennial
🥕,Carrot,biennial
🍆,Eggplant,perennial
🍅,Tomato,annual
🥔,Potato,perennial
[END plants_replace_all]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_replace_first(actual):
  expected = '''[START plants_replace_first]
🍓: Strawberry, perennial
🥕: Carrot, biennial
🍆: Eggplant, perennial
🍅: Tomato, annual
🥔: Potato, perennial
[END plants_replace_first]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_split(actual):
  expected = '''[START plants_split]
['🍓', 'Strawberry', 'perennial']
['🥕', 'Carrot', 'biennial']
['🍆', 'Eggplant', 'perennial']
['🍅', 'Tomato', 'annual']
['🥔', 'Potato', 'perennial']
[END plants_split]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_matches.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_all_matches.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_matches_kv.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_find.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_find_all.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_find_kv.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_replace_all.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_replace_first.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.regex_split.print',
    str)
class RegexTest(unittest.TestCase):
  def test_matches(self):
    regex_matches.regex_matches(check_matches)

  def test_all_matches(self):
    regex_all_matches.regex_all_matches(check_all_matches)

  def test_matches_kv(self):
    regex_matches_kv.regex_matches_kv(check_matches_kv)

  def test_find(self):
    regex_find.regex_find(check_matches)

  def test_find_all(self):
    regex_find_all.regex_find_all(check_find_all)

  def test_find_kv(self):
    regex_find_kv.regex_find_kv(check_find_kv)

  def test_replace_all(self):
    regex_replace_all.regex_replace_all(check_replace_all)

  def test_replace_first(self):
    regex_replace_first.regex_replace_first(check_replace_first)

  def test_split(self):
    regex_split.regex_split(check_split)


if __name__ == '__main__':
  unittest.main()
