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

from __future__ import absolute_import

import unittest

from apache_beam.dataframe import expressions


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


if __name__ == '__main__':
  unittest.main()
