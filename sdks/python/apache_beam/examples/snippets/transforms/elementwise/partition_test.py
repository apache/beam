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

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import partition


def check_partitions(actual1, actual2, actual3):
  expected = '''[START partitions]
perennial: {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
biennial: {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
perennial: {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
annual: {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
perennial: {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END partitions]'''.splitlines()[1:-1]

  annuals = [
      line.split(':', 1)[1].strip()
      for line in expected
      if line.split(':', 1)[0] == 'annual'
  ]
  biennials = [
      line.split(':', 1)[1].strip()
      for line in expected
      if line.split(':', 1)[0] == 'biennial'
  ]
  perennials = [
      line.split(':', 1)[1].strip()
      for line in expected
      if line.split(':', 1)[0] == 'perennial'
  ]

  assert_that(actual1 | 'annuals str' >> beam.Map(str),
              equal_to(annuals), label='assert annuals')
  assert_that(actual2 | 'biennials str' >> beam.Map(str),
              equal_to(biennials), label='assert biennials')
  assert_that(actual3 | 'perennials str' >> beam.Map(str),
              equal_to(perennials), label='assert perennials')


def check_split_datasets(actual1, actual2):
  expected = '''[START train_test]
train: {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}
train: {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}
test: {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}
test: {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}
train: {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
[END train_test]'''.splitlines()[1:-1]

  train_dataset = [
      line.split(':', 1)[1].strip()
      for line in expected
      if line.split(':', 1)[0] == 'train'
  ]
  test_dataset = [
      line.split(':', 1)[1].strip()
      for line in expected
      if line.split(':', 1)[0] == 'test'
  ]

  assert_that(actual1 | 'train str' >> beam.Map(str),
              equal_to(train_dataset), label='assert train')
  assert_that(actual2 | 'test str' >> beam.Map(str),
              equal_to(test_dataset), label='assert test')


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.examples.snippets.transforms.elementwise.partition.print', lambda x: x)
class PartitionTest(unittest.TestCase):
  def test_partition_function(self):
    partition.partition_function(check_partitions)

  def test_partition_lambda(self):
    partition.partition_lambda(check_partitions)

  def test_partition_multiple_arguments(self):
    partition.partition_multiple_arguments(check_split_datasets)


if __name__ == '__main__':
  unittest.main()
