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

import unittest

import mock

from apache_beam.examples.snippets.transforms.element_wise.filter import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.filter.print', lambda elem: elem)
# pylint: enable=line-too-long
class FilterTest(unittest.TestCase):
  def __init__(self, methodName):
    super(FilterTest, self).__init__(methodName)
    # [START perennials]
    perennials = [
        {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
        {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
        {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
    ]
    # [END perennials]
    self.perennials_test = lambda actual: \
        assert_that(actual, equal_to(perennials))

    # [START valid_plants]
    valid_plants = [
        {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
        {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
        {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
        {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
    ]
    # [END valid_plants]
    self.valid_plants_test = lambda actual: \
        assert_that(actual, equal_to(valid_plants))

  def test_filter_function(self):
    filter_function(self.perennials_test)

  def test_filter_lambda(self):
    filter_lambda(self.perennials_test)

  def test_filter_multiple_arguments(self):
    filter_multiple_arguments(self.perennials_test)

  def test_filter_side_inputs_singleton(self):
    filter_side_inputs_singleton(self.perennials_test)

  def test_filter_side_inputs_iter(self):
    filter_side_inputs_iter(self.valid_plants_test)

  def test_filter_side_inputs_dict(self):
    filter_side_inputs_dict(self.perennials_test)


if __name__ == '__main__':
  unittest.main()
