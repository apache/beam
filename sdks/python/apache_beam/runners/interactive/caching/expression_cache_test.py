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

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.runners.interactive.caching.expression_cache import ExpressionCache


class ExpressionCacheTest(unittest.TestCase):
  def setUp(self):
    self._pcollection_cache = {}
    self._computed_cache = set()
    self._pipeline = beam.Pipeline()
    self.cache = ExpressionCache(self._pcollection_cache, self._computed_cache)

  def create_trace(self, expr):
    trace = [expr]
    for input in expr.args():
      trace += self.create_trace(input)
    return trace

  def mock_cache(self, expr):
    pcoll = beam.PCollection(self._pipeline)
    self._pcollection_cache[expr._id] = pcoll
    self._computed_cache.add(pcoll)

  def assertTraceTypes(self, expr, expected):
    actual_types = [type(e).__name__ for e in self.create_trace(expr)]
    expected_types = [e.__name__ for e in expected]
    self.assertListEqual(actual_types, expected_types)

  def test_only_replaces_cached(self):
    in_expr = expressions.ConstantExpression(0)
    comp_expr = expressions.ComputedExpression('test', lambda x: x, [in_expr])

    # Expect that no replacement of expressions is performed.
    expected_trace = [
        expressions.ComputedExpression, expressions.ConstantExpression
    ]
    self.assertTraceTypes(comp_expr, expected_trace)

    self.cache.replace_with_cached(comp_expr)

    self.assertTraceTypes(comp_expr, expected_trace)

    # Now "cache" the expression and assert that the cached expression was
    # replaced with a placeholder.
    self.mock_cache(in_expr)

    replaced = self.cache.replace_with_cached(comp_expr)

    expected_trace = [
        expressions.ComputedExpression, expressions.PlaceholderExpression
    ]
    self.assertTraceTypes(comp_expr, expected_trace)
    self.assertIn(in_expr._id, replaced)

  def test_only_replaces_inputs(self):
    arg_0_expr = expressions.ConstantExpression(0)
    ident_val = expressions.ComputedExpression(
        'ident', lambda x: x, [arg_0_expr])

    arg_1_expr = expressions.ConstantExpression(1)
    comp_expr = expressions.ComputedExpression(
        'add', lambda x, y: x + y, [ident_val, arg_1_expr])

    self.mock_cache(ident_val)

    replaced = self.cache.replace_with_cached(comp_expr)

    # Assert that ident_val was replaced and that its arguments were removed
    # from the expression tree.
    expected_trace = [
        expressions.ComputedExpression,
        expressions.PlaceholderExpression,
        expressions.ConstantExpression
    ]
    self.assertTraceTypes(comp_expr, expected_trace)
    self.assertIn(ident_val._id, replaced)
    self.assertNotIn(arg_0_expr, self.create_trace(comp_expr))

  def test_only_caches_same_input(self):
    arg_0_expr = expressions.ConstantExpression(0)
    ident_val = expressions.ComputedExpression(
        'ident', lambda x: x, [arg_0_expr])
    comp_expr = expressions.ComputedExpression(
        'add', lambda x, y: x + y, [ident_val, arg_0_expr])

    self.mock_cache(arg_0_expr)

    replaced = self.cache.replace_with_cached(comp_expr)

    # Assert that arg_0_expr, being an input to two computations, was replaced
    # with the same placeholder expression.
    expected_trace = [
        expressions.ComputedExpression,
        expressions.ComputedExpression,
        expressions.PlaceholderExpression,
        expressions.PlaceholderExpression
    ]
    actual_trace = self.create_trace(comp_expr)
    unique_placeholders = set(
        t for t in actual_trace
        if isinstance(t, expressions.PlaceholderExpression))
    self.assertTraceTypes(comp_expr, expected_trace)
    self.assertTrue(
        all(e == replaced[arg_0_expr._id] for e in unique_placeholders))
    self.assertIn(arg_0_expr._id, replaced)


if __name__ == '__main__':
  unittest.main()
