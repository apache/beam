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

import unittest

import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import tolist


def identity(x):
  return x


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.tolist.print',
    identity)
# pylint: enable=line-too-long
class BatchElementsTest(unittest.TestCase):
  def test_tolist(self):
    def check(result):
      assert_that(
          result
          | 'Flatten' >> beam.FlatMap(identity)
          | 'Count' >> beam.combiners.Count.PerElement(),
          equal_to([('🍓', 1), ('🥕', 1), ('🍆', 1), ('🍅', 1)]),
          label='Check counts')
      assert_that(
          result
          | beam.Map(lambda x: isinstance(x, list)),
          equal_to([True]),
          label='Check type')

    tolist.tolist(check)


if __name__ == '__main__':
  unittest.main()
