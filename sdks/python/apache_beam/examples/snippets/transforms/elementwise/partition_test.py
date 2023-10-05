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

from . import partition_function
from . import partition_lambda
from . import partition_multiple_arguments


def check_partitions(actual1, actual2, actual3):
  expected = '''[START partitions]
perennial: {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
biennial: {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
perennial: {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
annual: {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
perennial: {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END partitions]'''.splitlines()[1:-1]

  annuals = [
      line.split(':', 1)[1].strip() for line in expected
      if line.split(':', 1)[0] == 'annual'
  ]
  biennials = [
      line.split(':', 1)[1].strip() for line in expected
      if line.split(':', 1)[0] == 'biennial'
  ]
  perennials = [
      line.split(':', 1)[1].strip() for line in expected
      if line.split(':', 1)[0] == 'perennial'
  ]

  assert_matches_stdout(actual1, annuals, label='annuals')
  assert_matches_stdout(actual2, biennials, label='biennials')
  assert_matches_stdout(actual3, perennials, label='perennials')


def check_split_datasets(actual1, actual2):
  expected = '''[START train_test]
train: {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
train: {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
test: {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
test: {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
train: {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END train_test]'''.splitlines()[1:-1]

  train_dataset = [
      line.split(':', 1)[1].strip() for line in expected
      if line.split(':', 1)[0] == 'train'
  ]
  test_dataset = [
      line.split(':', 1)[1].strip() for line in expected
      if line.split(':', 1)[0] == 'test'
  ]

  assert_matches_stdout(actual1, train_dataset, label='train_dataset')
  assert_matches_stdout(actual2, test_dataset, label='test_dataset')


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.partition_function.print',
    lambda elem: elem)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.partition_lambda.print',
    lambda elem: elem)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.partition_multiple_arguments.print',
    lambda elem: elem)
class PartitionTest(unittest.TestCase):
  def test_partition_function(self):
    partition_function.partition_function(check_partitions)

  def test_partition_lambda(self):
    partition_lambda.partition_lambda(check_partitions)

  def test_partition_multiple_arguments(self):
    partition_multiple_arguments.partition_multiple_arguments(
        check_split_datasets)


if __name__ == '__main__':
  unittest.main()
