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

import typing
import unittest

import mock

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from .groupby_attr import groupby_attr
from .groupby_attr_expr import groupby_attr_expr
from .groupby_expr import groupby_expr
from .groupby_expr_aggregate import expr_aggregate
from .groupby_global_aggregate import global_aggregate
from .groupby_simple_aggregate import simple_aggregate
from .groupby_two_exprs import groupby_two_exprs

#
# TODO: Remove early returns in check functions
#  https://github.com/apache/beam/issues/30778
skip_due_to_30778 = True


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

  def __repr__(self):
    return 'UnorderedList(%r)' % self._contents


def normalize(x):
  if isinstance(x, tuple) and hasattr(x, '_fields'):
    # A named tuple.
    return beam.Row(**dict(zip(x._fields, x)))
  elif isinstance(x, typing.Iterable) and not isinstance(x, (str, beam.Row)):
    return UnorderedList(normalize(e) for e in x)
  else:
    return x


def normalize_kv(k, v):
  return normalize(k), normalize(v)


# For documentation.
NamedTuple = beam.Row


def check_groupby_expr_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.MapTuple(normalize_kv),
      equal_to([
          #[START groupby_expr_result]
          ('s', ['strawberry']),
          ('r', ['raspberry']),
          ('b', ['banana', 'blackberry', 'blueberry']),
          #[END groupby_expr_result]
      ]))


def check_groupby_two_exprs_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.MapTuple(normalize_kv),
      equal_to([
          #[START groupby_two_exprs_result]
          (NamedTuple(letter='s', is_berry=True), ['strawberry']),
          (NamedTuple(letter='r', is_berry=True), ['raspberry']),
          (NamedTuple(letter='b', is_berry=True), ['blackberry', 'blueberry']),
          (NamedTuple(letter='b', is_berry=False), ['banana']),
          #[END groupby_two_exprs_result]
      ]))


def check_groupby_attr_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.MapTuple(normalize_kv),
      equal_to([
          #[START groupby_attr_result]
          (
              'pie',
              [
                  beam.Row(
                      recipe='pie',
                      fruit='strawberry',
                      quantity=3,
                      unit_price=1.50),
                  beam.Row(
                      recipe='pie',
                      fruit='raspberry',
                      quantity=1,
                      unit_price=3.50),
                  beam.Row(
                      recipe='pie',
                      fruit='blackberry',
                      quantity=1,
                      unit_price=4.00),
                  beam.Row(
                      recipe='pie',
                      fruit='blueberry',
                      quantity=1,
                      unit_price=2.00),
              ]),
          (
              'muffin',
              [
                  beam.Row(
                      recipe='muffin',
                      fruit='blueberry',
                      quantity=2,
                      unit_price=2.00),
                  beam.Row(
                      recipe='muffin',
                      fruit='banana',
                      quantity=3,
                      unit_price=1.00),
              ]),  #[END groupby_attr_result]
      ]))


def check_groupby_attr_expr_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.MapTuple(normalize_kv),
      equal_to([
          #[START groupby_attr_expr_result]
          (
              NamedTuple(recipe='pie', is_berry=True),
              [
                  beam.Row(
                      recipe='pie',
                      fruit='strawberry',
                      quantity=3,
                      unit_price=1.50),
                  beam.Row(
                      recipe='pie',
                      fruit='raspberry',
                      quantity=1,
                      unit_price=3.50),
                  beam.Row(
                      recipe='pie',
                      fruit='blackberry',
                      quantity=1,
                      unit_price=4.00),
                  beam.Row(
                      recipe='pie',
                      fruit='blueberry',
                      quantity=1,
                      unit_price=2.00),
              ]),
          (
              NamedTuple(recipe='muffin', is_berry=True),
              [
                  beam.Row(
                      recipe='muffin',
                      fruit='blueberry',
                      quantity=2,
                      unit_price=2.00),
              ]),
          (
              NamedTuple(recipe='muffin', is_berry=False),
              [
                  beam.Row(
                      recipe='muffin',
                      fruit='banana',
                      quantity=3,
                      unit_price=1.00),
              ]),  #[END groupby_attr_expr_result]
      ]))


def check_simple_aggregate_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.MapTuple(normalize_kv),
      equal_to([
          #[START simple_aggregate_result]
          NamedTuple(fruit='strawberry', total_quantity=3),
          NamedTuple(fruit='raspberry', total_quantity=1),
          NamedTuple(fruit='blackberry', total_quantity=1),
          NamedTuple(fruit='blueberry', total_quantity=3),
          NamedTuple(fruit='banana', total_quantity=3),
          #[END simple_aggregate_result]
      ]))


def check_expr_aggregate_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.Map(normalize),
      equal_to([
          #[START expr_aggregate_result]
          NamedTuple(recipe='pie', total_quantity=6, price=14.00),
          NamedTuple(recipe='muffin', total_quantity=5, price=7.00),
          #[END expr_aggregate_result]
      ]))


def check_global_aggregate_result(grouped):
  if skip_due_to_30778:
    return
  assert_that(
      grouped | beam.Map(normalize),
      equal_to([
          #[START global_aggregate_result]
          NamedTuple(min_price=1.00, mean_price=7 / 3, max_price=4.00),
          #[END global_aggregate_result]
      ]))


@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_expr.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_two_exprs.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_attr.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_attr_expr.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_simple_aggregate.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_expr_aggregate.print',
    str)
@mock.patch(
    'apache_beam.examples.snippets.transforms.aggregation.groupby_global_aggregate.print',
    str)
class GroupByTest(unittest.TestCase):
  def test_groupby_expr(self):
    groupby_expr(check_groupby_expr_result)

  def test_groupby_two_exprs(self):
    groupby_two_exprs(check_groupby_two_exprs_result)

  def test_group_by_attr(self):
    groupby_attr(check_groupby_attr_result)

  def test_group_by_attr_expr(self):
    groupby_attr_expr(check_groupby_attr_expr_result)

  def test_simple_aggregate(self):
    simple_aggregate(check_simple_aggregate_result)

  def test_expr_aggregate(self):
    expr_aggregate(check_expr_aggregate_result)

  def test_global_aggregate(self):
    global_aggregate(check_global_aggregate_result)


if __name__ == '__main__':
  unittest.main()
