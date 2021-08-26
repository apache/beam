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

import unittest

import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import partitionings


class ExpressionTest(unittest.TestCase):
  def test_placeholder_expression(self):
    a = expressions.PlaceholderExpression(None)
    b = expressions.PlaceholderExpression(None)
    session = expressions.Session({a: 1, b: 2})
    self.assertEqual(session.evaluate(a), 1)
    self.assertEqual(session.evaluate(b), 2)

  def test_constant_expresion(self):
    two = expressions.ConstantExpression(2)
    session = expressions.Session({})
    self.assertEqual(session.evaluate(two), 2)

  def test_computed_expression(self):
    a = expressions.PlaceholderExpression(0)
    b = expressions.PlaceholderExpression(0)
    a_plus_b = expressions.ComputedExpression('add', lambda a, b: a + b, [a, b])
    session = expressions.Session({a: 1, b: 2})
    self.assertEqual(session.evaluate(a_plus_b), 3)

  def test_expression_proxy(self):
    a = expressions.PlaceholderExpression(1)
    b = expressions.PlaceholderExpression(2)
    a_plus_b = expressions.ComputedExpression('add', lambda a, b: a + b, [a, b])
    self.assertEqual(a_plus_b.proxy(), 3)

  def test_expression_proxy_error(self):
    a = expressions.PlaceholderExpression(1)
    b = expressions.PlaceholderExpression('s')
    with self.assertRaises(TypeError):
      expressions.ComputedExpression('add', lambda a, b: a + b, [a, b])

  def test_preserves_singleton_output_partitioning(self):
    # Empty DataFrame with one column and two index levels
    input_expr = expressions.ConstantExpression(
        pd.DataFrame(columns=["column"], index=[[], []]))

    preserves_only_singleton = expressions.ComputedExpression(
        'preserves_only_singleton',
        # index is replaced with an entirely new one, so
        # if we were partitioned by Index we're not anymore.
        lambda df: df.set_index('column'),
        [input_expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    for partitioning in (partitionings.Singleton(), ):
      self.assertEqual(
          expressions.output_partitioning(
              preserves_only_singleton, partitioning),
          partitioning,
          f"Should preserve {partitioning}")

    for partitioning in (partitionings.Index([0]),
                         partitionings.Index(),
                         partitionings.Arbitrary()):
      self.assertEqual(
          expressions.output_partitioning(
              preserves_only_singleton, partitioning),
          partitionings.Arbitrary(),
          f"Should NOT preserve {partitioning}")

  def test_preserves_index_output_partitioning(self):
    # Empty DataFrame with two columns and two index levels
    input_expr = expressions.ConstantExpression(
        pd.DataFrame(columns=["foo", "bar"], index=[[], []]))

    preserves_partial_index = expressions.ComputedExpression(
        'preserves_partial_index',
        # This adds an additional index  level, so we'd only preserve
        # partitioning on the two index levels that existed before.
        lambda df: df.set_index('foo', append=True),
        [input_expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Index([0, 1]))

    for partitioning in (
        partitionings.Singleton(),
        partitionings.Index([0]),
        partitionings.Index([1]),
        partitionings.Index([0, 1]),
    ):
      self.assertEqual(
          expressions.output_partitioning(
              preserves_partial_index, partitioning),
          partitioning,
          f"Should preserve {partitioning}")

    for partitioning in (partitionings.Index([0, 1, 2]),
                         partitionings.Index(),
                         partitionings.Arbitrary()):
      self.assertEqual(
          expressions.output_partitioning(
              preserves_partial_index, partitioning),
          partitionings.Arbitrary(),
          f"Should NOT preserve {partitioning}")


if __name__ == '__main__':
  unittest.main()
