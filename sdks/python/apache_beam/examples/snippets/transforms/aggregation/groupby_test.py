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

# Wrapping hurts the readability of the docs.
# pylint: disable=line-too-long

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import typing
import unittest

import apache_beam as beam
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class UnorderedList(object):
  def __init__(self, contents):
    self._contents = list(contents)

  def __eq__(self, other):
    try:
      return sorted(self._contents) == sorted(other)
    except TypeError:
      return sorted(self._contents, key=str) == sorted(other, key=str)

  def __hash__(self):
    return hash(tuple(sorted(self._contents)))


def normalize(x):
  if isinstance(x, tuple) and hasattr(x, '_fields'):
    # A named tuple.
    return beam.Row(**dict(zip(x._fields, x)))
  elif isinstance(x, typing.Iterable) and not isinstance(x, (str, beam.Row)):
    return UnorderedList(x)
  else:
    return x


def normalize_kv(k, v):
  return normalize(k), normalize(v)


# For documentation.
NamedTuple = beam.Row

# [START groupby_table]
GROCERY_LIST = [
    beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
    beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
    beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
    beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
]
# [END groupby_table]


class GroupByTest(unittest.TestCase):
  def test_groupby_expr(self):
    # [START groupby_expr]
    with beam.Pipeline() as p:
      grouped = (
          p
          | beam.Create(['strawberry', 'raspberry', 'blueberry', 'blackberry', 'banana'])
          | beam.GroupBy(lambda s: s[0]))
      # [END groupby_expr]

      assert_that(
          grouped | beam.MapTuple(normalize_kv),
          equal_to([
              #[START groupby_expr_result]
              ('s', ['strawberry']),
              ('r', ['raspberry']),
              ('b', ['banana', 'blackberry', 'blueberry']),
              #[END groupby_expr_result]
          ]))

  def test_groupby_two_exprs(self):
    # [START groupby_two_exprs]
    with beam.Pipeline() as p:
      grouped = (
          p
          | beam.Create(['strawberry', 'raspberry', 'blueberry', 'blackberry', 'banana'])
          | beam.GroupBy(letter=lambda s: s[0], is_berry=lambda s: 'berry' in s))
      # [END groupby_two_exprs]

      expected = [
          #[START groupby_two_exprs_result]
          (NamedTuple(letter='s', is_berry=True), ['strawberry']),
          (NamedTuple(letter='r', is_berry=True), ['raspberry']),
          (NamedTuple(letter='b', is_berry=True), ['blackberry', 'blueberry']),
          (NamedTuple(letter='b', is_berry=False), ['banana']),
          #[END groupby_two_exprs_result]
      ]
      assert_that(grouped | beam.MapTuple(normalize_kv), equal_to(expected))

  def test_group_by_attr(self):
    # [START groupby_attr]
    with beam.Pipeline() as p:
      grouped = p | beam.Create(GROCERY_LIST) | beam.GroupBy('recipe')
      # [END groupby_attr]

      expected = [
          #[START groupby_attr_result]
          ('pie',
            [
                beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
                beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
                beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
                beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
            ]),
          ('muffin',
            [
                beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
                beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
            ]),
          #[END groupby_attr_result]
      ]
      assert_that(grouped | beam.MapTuple(normalize_kv), equal_to(expected))

  def test_group_by_attr_expr(self):
    # [START groupby_attr_expr]
    with beam.Pipeline() as p:
      grouped = (
          p | beam.Create(GROCERY_LIST)
          | beam.GroupBy('recipe', is_berry=lambda x: 'berry' in x.fruit))
      # [END groupby_attr_expr]

      expected = [
          #[START groupby_attr_expr_result]
          (NamedTuple(recipe='pie', is_berry=True),
            [
                beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
                beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
                beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
                beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
            ]),
          (NamedTuple(recipe='muffin', is_berry=True),
            [
                beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
            ]),
          (NamedTuple(recipe='muffin', is_berry=False),
            [
                beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
            ]),
          #[END groupby_attr_expr_result]
      ]
      assert_that(grouped | beam.MapTuple(normalize_kv), equal_to(expected))

  def test_simple_aggregate(self):
    # [START simple_aggregate]
    with beam.Pipeline() as p:
      grouped = (
          p
          | beam.Create(GROCERY_LIST)
          | beam.GroupBy('fruit')
              .aggregate_field('quantity', sum, 'total_quantity'))
      # [END simple_aggregate]

      expected = [
          #[START simple_aggregate_result]
          NamedTuple(fruit='strawberry', total_quantity=3),
          NamedTuple(fruit='raspberry', total_quantity=1),
          NamedTuple(fruit='blackberry', total_quantity=1),
          NamedTuple(fruit='blueberry', total_quantity=3),
          NamedTuple(fruit='banana', total_quantity=3),
          #[END simple_aggregate_result]
      ]
      assert_that(grouped | beam.Map(normalize), equal_to(expected))

  def test_expr_aggregate(self):
    # [START expr_aggregate]
    with beam.Pipeline() as p:
      grouped = (
          p
          | beam.Create(GROCERY_LIST)
          | beam.GroupBy('recipe')
              .aggregate_field('quantity', sum, 'total_quantity')
              .aggregate_field(lambda x: x.quantity * x.unit_price, sum, 'price'))
      # [END expr_aggregate]

      expected = [
          #[START expr_aggregate_result]
          NamedTuple(recipe='pie', total_quantity=6, price=14.00),
          NamedTuple(recipe='muffin', total_quantity=5, price=7.00),
          #[END expr_aggregate_result]
      ]
      assert_that(grouped | beam.Map(normalize), equal_to(expected))

  def test_global_aggregate(self):
    # [START global_aggregate]
    with beam.Pipeline() as p:
      grouped = (
          p
          | beam.Create(GROCERY_LIST)
          | beam.GroupBy()
              .aggregate_field('unit_price', min, 'min_price')
              .aggregate_field('unit_price', MeanCombineFn(), 'mean_price')
              .aggregate_field('unit_price', max, 'max_price'))
      # [END global_aggregate]

      expected = [
          #[START global_aggregate_result]
          NamedTuple(min_price=1.00, mean_price=7 / 3, max_price=4.00),
          #[END global_aggregate_result]
      ]
      assert_that(grouped | beam.Map(normalize), equal_to(expected))


if __name__ == '__main__':
  unittest.main()
