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


if __name__ == '__main__':
  unittest.main()
