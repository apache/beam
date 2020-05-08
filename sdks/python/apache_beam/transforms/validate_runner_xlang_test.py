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

import logging
import os
import typing
import unittest

from nose.plugins.attrib import attr
from past.builtins import unicode

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder

TEST_PREFIX_URN = "beam:transforms:xlang:test:prefix"
TEST_MULTI_URN = "beam:transforms:xlang:test:multi"
TEST_GBK_URN = "beam:transforms:xlang:test:gbk"
TEST_CGBK_URN = "beam:transforms:xlang:test:cgbk"
TEST_COMGL_URN = "beam:transforms:xlang:test:comgl"
TEST_COMPK_URN = "beam:transforms:xlang:test:compk"
TEST_FLATTEN_URN = "beam:transforms:xlang:test:flatten"
TEST_PARTITION_URN = "beam:transforms:xlang:test:partition"


class CrossLanguageTestPipelines(object):
  def __init__(self, expansion_service=None):
    self.expansion_service = expansion_service or (
        'localhost:%s' % os.environ.get('EXPANSION_PORT'))

  def run_prefix(self, pipeline):
    with pipeline as p:
      res = (
          p
          | beam.Create(['a', 'b']).with_output_types(unicode)
          | beam.ExternalTransform(
              TEST_PREFIX_URN,
              ImplicitSchemaPayloadBuilder({'data': u'0'}),
              self.expansion_service))
      assert_that(res, equal_to(['0a', '0b']))

  def run_multi_input_output_with_sideinput(self, pipeline):
    with pipeline as p:
      main1 = p | 'Main1' >> beam.Create(
          ['a', 'bb'], reshuffle=False).with_output_types(unicode)
      main2 = p | 'Main2' >> beam.Create(
          ['x', 'yy', 'zzz'], reshuffle=False).with_output_types(unicode)
      side = p | 'Side' >> beam.Create(['s']).with_output_types(unicode)
      res = dict(
          main1=main1, main2=main2, side=side) | beam.ExternalTransform(
              TEST_MULTI_URN, None, self.expansion_service)
      assert_that(res['main'], equal_to(['as', 'bbs', 'xs', 'yys', 'zzzs']))
      assert_that(res['side'], equal_to(['ss']), label='CheckSide')

  def run_group_by_key(self, pipeline):
    with pipeline as p:
      res = (
          p
          | beam.Create([(0, "1"), (0, "2"),
                         (1, "3")], reshuffle=False).with_output_types(
                             typing.Tuple[int, unicode])
          | beam.ExternalTransform(TEST_GBK_URN, None, self.expansion_service)
          | beam.Map(lambda x: "{}:{}".format(x[0], ','.join(sorted(x[1])))))
      assert_that(res, equal_to(['0:1,2', '1:3']))

  def run_cogroup_by_key(self, pipeline):
    with pipeline as p:
      col1 = p | 'create_col1' >> beam.Create(
          [(0, "1"), (0, "2"), (1, "3")], reshuffle=False).with_output_types(
              typing.Tuple[int, unicode])
      col2 = p | 'create_col2' >> beam.Create(
          [(0, "4"), (1, "5"), (1, "6")], reshuffle=False).with_output_types(
              typing.Tuple[int, unicode])
      res = (
          dict(col1=col1, col2=col2)
          | beam.ExternalTransform(TEST_CGBK_URN, None, self.expansion_service)
          | beam.Map(lambda x: "{}:{}".format(x[0], ','.join(sorted(x[1])))))
      assert_that(res, equal_to(['0:1,2,4', '1:3,5,6']))

  def run_combine_globally(self, pipeline):
    with pipeline as p:
      res = (
          p
          | beam.Create([1, 2, 3]).with_output_types(int)
          | beam.ExternalTransform(
              TEST_COMGL_URN, None, self.expansion_service))
      assert_that(res, equal_to([6]))

  def run_combine_per_key(self, pipeline):
    with pipeline as p:
      res = (
          p
          | beam.Create([('a', 1), ('a', 2), ('b', 3)]).with_output_types(
              typing.Tuple[unicode, int])
          | beam.ExternalTransform(
              TEST_COMPK_URN, None, self.expansion_service))
      assert_that(res, equal_to([('a', 3), ('b', 3)]))

  def run_flatten(self, pipeline):
    with pipeline as p:
      col1 = p | 'col1' >> beam.Create([1, 2, 3]).with_output_types(int)
      col2 = p | 'col2' >> beam.Create([4, 5, 6]).with_output_types(int)
      res = ((col1, col2)
             | beam.ExternalTransform(
                 TEST_FLATTEN_URN, None, self.expansion_service))
      assert_that(res, equal_to([1, 2, 3, 4, 5, 6]))

  def run_partition(self, pipeline):
    with pipeline as p:
      res = (
          p
          | beam.Create([1, 2, 3, 4, 5, 6]).with_output_types(int)
          | beam.ExternalTransform(
              TEST_PARTITION_URN, None, self.expansion_service))
      assert_that(res['0'], equal_to([2, 4, 6]), label='check_even')
      assert_that(res['1'], equal_to([1, 3, 5]), label='check_odd')


@attr('UsesCrossLanguageTransforms')
@unittest.skipUnless(
    os.environ.get('EXPANSION_PORT'),
    "EXPANSION_PORT environment var is not provided.")
class ValidateRunnerXlangTest(unittest.TestCase):
  def create_pipeline(self):
    test_pipeline = TestPipeline()
    test_pipeline.not_use_test_runner_api = True
    return test_pipeline

  def test_prefix(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_prefix(
        test_pipeline or self.create_pipeline())

  def test_multi_input_output_with_sideinput(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_multi_input_output_with_sideinput(
        test_pipeline or self.create_pipeline())

  def test_group_by_key(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_group_by_key(
        test_pipeline or self.create_pipeline())

  def test_cogroup_by_key(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_cogroup_by_key(
        test_pipeline or self.create_pipeline())

  def test_combine_globally(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_combine_globally(
        test_pipeline or self.create_pipeline())

  def test_combine_per_key(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_combine_per_key(
        test_pipeline or self.create_pipeline())

  def test_flatten(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_flatten(
        test_pipeline or self.create_pipeline())

  def test_partition(self, test_pipeline=None):
    CrossLanguageTestPipelines().run_partition(
        test_pipeline or self.create_pipeline())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
