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

"""Tests for schemas."""

import unittest
from typing import NamedTuple

import future.tests.base  # pylint: disable=unused-import
# patches unittest.testcase to be python3 compatible
import pandas as pd
from past.builtins import unicode

import apache_beam as beam
from apache_beam.coders import RowCoder
from apache_beam.coders.typecoders import registry as coders_registry
from apache_beam.dataframe import schemas
from apache_beam.dataframe import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

Simple = NamedTuple(
    'Simple', [('name', unicode), ('id', int), ('height', float)])
coders_registry.register_coder(Simple, RowCoder)
Animal = NamedTuple('Animal', [('animal', unicode), ('max_speed', float)])
coders_registry.register_coder(Animal, RowCoder)


def matches_df(expected):
  def check_df_pcoll_equal(actual):
    sorted_actual = pd.concat(actual).sort_index()
    sorted_expected = expected.sort_index()
    if not sorted_actual.equals(sorted_expected):
      raise AssertionError(
          'Dataframes not equal: \n\nActual:\n%s\n\nExpected:\n%s' %
          (sorted_actual, sorted_expected))

  return check_df_pcoll_equal


class SchemasTest(unittest.TestCase):
  def test_simple_df(self):
    expected = pd.DataFrame({
        'name': list(map(unicode, range(5))),
        'id': list(range(5)),
        'height': list(map(float, range(5)))
    })

    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              Simple(name=unicode(i), id=i, height=float(i)) for i in range(5)
          ])
          | schemas.BatchRowsAsDataFrame(min_batch_size=10, max_batch_size=10))
      assert_that(res, matches_df(expected))

  def test_generate_proxy(self):
    expected = pd.DataFrame({
        'animal': pd.Series(dtype=unicode), 'max_speed': pd.Series(dtype=float)
    })

    self.assertTrue(schemas.generate_proxy(Animal).equals(expected))

  def test_batch_with_df_transform(self):
    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              Animal('Falcon', 380.0),
              Animal('Falcon', 370.0),
              Animal('Parrot', 24.0),
              Animal('Parrot', 26.0)
          ])
          | schemas.BatchRowsAsDataFrame()
          | transforms.DataframeTransform(
              lambda df: df.groupby('animal').mean(),
              proxy=schemas.generate_proxy(Animal)))
      assert_that(
          res,
          matches_df(
              pd.DataFrame({'max_speed': [375.0, 25.0]},
                           index=pd.Index(
                               data=['Falcon', 'Parrot'], name='animal'))))


if __name__ == '__main__':
  unittest.main()
