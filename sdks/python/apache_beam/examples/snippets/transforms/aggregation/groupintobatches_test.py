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

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

from . import groupintobatches


def check_batches_with_keys(actual):
  expected = '''[START batches_with_keys]
('spring', ['🍓', '🥕', '🍆'])
('summer', ['🥕', '🍅', '🌽'])
('spring', ['🍅'])
('fall', ['🥕', '🍅'])
('winter', ['🍆'])
[END batches_with_keys]'''.splitlines()[1:-1]
  assert_matches_stdout(
      actual, expected, lambda batch: (batch[0], len(batch[1])))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupintobatches.print',
    str)
# pylint: enable=line-too-long
class GroupIntoBatchesTest(unittest.TestCase):
  def test_groupintobatches(self):
    groupintobatches.groupintobatches(check_batches_with_keys)


if __name__ == '__main__':
  unittest.main()
