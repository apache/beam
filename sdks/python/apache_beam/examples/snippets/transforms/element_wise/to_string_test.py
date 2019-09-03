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

from __future__ import absolute_import
from __future__ import print_function

import sys
import unittest

import mock

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import to_string


def check_plants(actual):
  # [START plants]
  plants = [
      '🍓,Strawberry',
      '🥕,Carrot',
      '🍆,Eggplant',
      '🍅,Tomato',
      '🥔,Potato',
  ]
  # [END plants]
  assert_that(actual, equal_to(plants))


def check_plant_lists(actual):
  # [START plant_lists]
  plant_lists = [
      "['🍓', 'Strawberry', 'perennial']",
      "['🥕', 'Carrot', 'biennial']",
      "['🍆', 'Eggplant', 'perennial']",
      "['🍅', 'Tomato', 'annual']",
      "['🥔', 'Potato', 'perennial']",
  ]
  # [END plant_lists]

  # Some unicode characters become escaped with double backslashes.
  import apache_beam as beam

  def normalize_escaping(elem):
    # In Python 2 all utf-8 characters are escaped with double backslashes.
    # TODO: Remove this after Python 2 deprecation.
    # https://issues.apache.org/jira/browse/BEAM-8124
    if sys.version_info.major == 2:
      return elem.decode('string-escape')

    # In Python 3.5 some utf-8 characters are escaped with double backslashes.
    if '\\' in elem:
      return bytes(elem, 'utf-8').decode('unicode-escape')
    return elem
  actual = actual | beam.Map(normalize_escaping)
  assert_that(actual, equal_to(plant_lists))


def check_plants_csv(actual):
  # [START plants_csv]
  plants_csv = [
      '🍓,Strawberry,perennial',
      '🥕,Carrot,biennial',
      '🍆,Eggplant,perennial',
      '🍅,Tomato,annual',
      '🥔,Potato,perennial',
  ]
  # [END plants_csv]
  assert_that(actual, equal_to(plants_csv))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.to_string.print', lambda elem: elem)
# pylint: enable=line-too-long
class ToStringTest(unittest.TestCase):
  def test_to_string_kvs(self):
    to_string.to_string_kvs(check_plants)

  def test_to_string_element(self):
    to_string.to_string_element(check_plant_lists)

  def test_to_string_iterables(self):
    to_string.to_string_iterables(check_plants_csv)


if __name__ == '__main__':
  unittest.main()
