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
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames


class FrameBaseTest(unittest.TestCase):
  def test_elementwise_func(self):
    a = pd.Series([1, 2, 3])
    b = pd.Series([100, 200, 300])
    empty_proxy = a[:0]
    x = frames.DeferredSeries(expressions.PlaceholderExpression(empty_proxy))
    y = frames.DeferredSeries(expressions.PlaceholderExpression(empty_proxy))
    sub = frame_base._elementwise_function(lambda x, y: x - y)

    session = expressions.Session({x._expr: a, y._expr: b})
    self.assertTrue(sub(x, y)._expr.evaluate_at(session).equals(a - b))
    self.assertTrue(sub(x, 1)._expr.evaluate_at(session).equals(a - 1))
    self.assertTrue(sub(1, x)._expr.evaluate_at(session).equals(1 - a))
    self.assertTrue(sub(x, b)._expr.evaluate_at(session).equals(a - b))
    self.assertTrue(sub(a, y)._expr.evaluate_at(session).equals(a - b))

  def test_elementwise_func_kwarg(self):
    a = pd.Series([1, 2, 3])
    b = pd.Series([100, 200, 300])
    empty_proxy = a[:0]
    x = frames.DeferredSeries(expressions.PlaceholderExpression(empty_proxy))
    y = frames.DeferredSeries(expressions.PlaceholderExpression(empty_proxy))
    sub = frame_base._elementwise_function(lambda x, y=1: x - y)

    session = expressions.Session({x._expr: a, y._expr: b})
    self.assertTrue(sub(x, y=y)._expr.evaluate_at(session).equals(a - b))
    self.assertTrue(sub(x)._expr.evaluate_at(session).equals(a - 1))
    self.assertTrue(sub(1, y=x)._expr.evaluate_at(session).equals(1 - a))
    self.assertTrue(sub(x, y=b)._expr.evaluate_at(session).equals(a - b))
    self.assertTrue(sub(a, y=y)._expr.evaluate_at(session).equals(a - b))
    self.assertTrue(sub(x, y)._expr.evaluate_at(session).equals(a - b))

  def test_maybe_inplace(self):
    @frame_base.maybe_inplace
    def add_one(frame):
      return frame + 1

    frames.DeferredSeries.add_one = add_one
    original_expr = expressions.PlaceholderExpression(pd.Series([1, 2, 3]))
    x = frames.DeferredSeries(original_expr)
    x.add_one()
    self.assertIs(x._expr, original_expr)
    x.add_one(inplace=False)
    self.assertIs(x._expr, original_expr)
    x.add_one(inplace=True)
    self.assertIsNot(x._expr, original_expr)

  def test_args_to_kwargs(self):
    class Base(object):
      def func(self, a=1, b=2, c=3, *, kw_only=4):
        pass

    class Proxy(object):
      @frame_base.args_to_kwargs(Base)
      def func(self, **kwargs):
        return kwargs

    proxy = Proxy()
    # pylint: disable=too-many-function-args
    self.assertEqual(proxy.func(), {})
    self.assertEqual(proxy.func(100), {'a': 100})
    self.assertEqual(proxy.func(2, 4, 6), {'a': 2, 'b': 4, 'c': 6})
    self.assertEqual(proxy.func(2, c=6), {'a': 2, 'c': 6})
    self.assertEqual(proxy.func(c=6, a=2), {'a': 2, 'c': 6})
    self.assertEqual(proxy.func(2, kw_only=20), {'a': 2, 'kw_only': 20})
    with self.assertRaises(TypeError):  # got too many positioned arguments
      proxy.func(2, 4, 6, 8)

  def test_args_to_kwargs_populates_defaults(self):
    class Base(object):
      def func(self, a=1, b=2, c=3):
        pass

      def func_removed_args(self, a):
        pass

    class Proxy(object):
      @frame_base.args_to_kwargs(Base)
      @frame_base.populate_defaults(Base)
      def func(self, a, c=1000, **kwargs):
        return dict(kwargs, a=a, c=c)

      @frame_base.args_to_kwargs(Base, removed_method=True)
      @frame_base.populate_defaults(Base, removed_method=True)
      def func_removed_method(self, a, **kwargs):
        return dict(kwargs, a=a)

      @frame_base.args_to_kwargs(Base, removed_args=['c'])
      @frame_base.populate_defaults(Base, removed_args=['c'])
      def func_removed_args(self, a, c, **kwargs):
        return dict(kwargs, a=a)

    proxy = Proxy()
    # pylint: disable=too-many-function-args,no-value-for-parameter
    self.assertEqual(proxy.func(), {'a': 1, 'c': 1000})
    self.assertEqual(proxy.func(100), {'a': 100, 'c': 1000})
    self.assertEqual(proxy.func(2, 4, 6), {'a': 2, 'b': 4, 'c': 6})
    self.assertEqual(proxy.func(2, c=6), {'a': 2, 'c': 6})
    self.assertEqual(proxy.func(c=6, a=2), {'a': 2, 'c': 6})
    self.assertEqual(proxy.func(c=6), {'a': 1, 'c': 6})

    with self.assertRaises(TypeError):  # missing 1 required positional argument
      proxy.func_removed_method()
    self.assertEqual(proxy.func_removed_method(12, c=100), {'a': 12, 'c': 100})

    with self.assertRaises(TypeError):  # missing 1 required positional argument
      proxy.func_removed_args()
    self.assertEqual(proxy.func_removed_args(12, d=100), {'a': 12, 'd': 100})

  def test_args_to_kwargs_populates_default_handles_kw_only(self):
    class Base(object):
      def func(self, a, b=2, c=3, *, kw_only=4):
        pass

    class ProxyUsesKwOnly(object):
      @frame_base.args_to_kwargs(Base)
      @frame_base.populate_defaults(Base)
      def func(self, a, kw_only, **kwargs):
        return dict(kwargs, a=a, kw_only=kw_only)

    proxy = ProxyUsesKwOnly()

    # pylint: disable=too-many-function-args,no-value-for-parameter
    with self.assertRaises(TypeError):  # missing 1 required positional argument
      proxy.func()

    self.assertEqual(proxy.func(100), {'a': 100, 'kw_only': 4})
    self.assertEqual(
        proxy.func(2, 4, 6, kw_only=8), {
            'a': 2, 'b': 4, 'c': 6, 'kw_only': 8
        })
    with self.assertRaises(TypeError):
      proxy.func(2, 4, 6, 8)  # got too many positioned arguments

    class ProxyDoesntUseKwOnly(object):
      @frame_base.args_to_kwargs(Base)
      @frame_base.populate_defaults(Base)
      def func(self, a, **kwargs):
        return dict(kwargs, a=a)

    proxy = ProxyDoesntUseKwOnly()

    # pylint: disable=too-many-function-args,no-value-for-parameter
    with self.assertRaises(TypeError):  # missing 1 required positional argument
      proxy.func()
    self.assertEqual(proxy.func(100), {'a': 100})
    self.assertEqual(
        proxy.func(2, 4, 6, kw_only=8), {
            'a': 2, 'b': 4, 'c': 6, 'kw_only': 8
        })

  def test_populate_defaults_overwrites_copy(self):
    class Base(object):
      def func(self, a=1, b=2, c=3, *, copy=None):
        pass

    class Proxy(object):
      @frame_base.args_to_kwargs(Base)
      @frame_base.populate_defaults(Base)
      def func(self, a, copy, **kwargs):
        return dict(kwargs, a=a, copy=copy)

    proxy = Proxy()
    self.assertEqual(proxy.func(), {'a': 1, 'copy': True})
    self.assertEqual(proxy.func(copy=False), {'a': 1, 'copy': False})


if __name__ == '__main__':
  unittest.main()
