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

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from ..element_wise import partition


def check_partitions(actual1, actual2, actual3):
  # [START partitions]
  annuals = [
      {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
  ]
  biennials = [
      {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
  ]
  perennials = [
      {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
      {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
      {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
  ]
  # [END partitions]
  assert_that(actual1, equal_to(annuals), label='assert annuals')
  assert_that(actual2, equal_to(biennials), label='assert biennials')
  assert_that(actual3, equal_to(perennials), label='assert perennials')


def check_split_datasets(actual1, actual2):
  # [START train_test]
  train_dataset = [
      {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
      {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
      {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
  ]
  test_dataset = [
      {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
      {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
  ]
  # [END train_test]
  assert_that(actual1, equal_to(train_dataset), label='assert train')
  assert_that(actual2, equal_to(test_dataset), label='assert test')


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.partition.print', lambda elem: elem)
# pylint: enable=line-too-long
class PartitionTest(unittest.TestCase):
  def test_partition_function(self):
    partition.partition_function(check_partitions)

  def test_partition_lambda(self):
    partition.partition_lambda(check_partitions)

  def test_partition_multiple_arguments(self):
    partition.partition_multiple_arguments(check_split_datasets)


if __name__ == '__main__':
  unittest.main()
