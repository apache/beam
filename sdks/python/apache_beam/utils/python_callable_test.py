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

import os
import unittest

from apache_beam.utils.python_callable import PythonCallableWithSource


class PythonCallableWithSourceTest(unittest.TestCase):
  def test_builtin(self):
    self.assertEqual(PythonCallableWithSource.load_from_source('str'), str)

  def test_builtin_attribute(self):
    self.assertEqual(
        PythonCallableWithSource.load_from_source('str.lower'), str.lower)

  def test_fully_qualified_name(self):
    self.assertEqual(
        PythonCallableWithSource.load_from_source('os.path.abspath'),
        os.path.abspath)

  def test_expression(self):
    self.assertEqual(PythonCallableWithSource('lambda x: x*x')(10), 100)

  def test_expression_with_dependency(self):
    self.assertEqual(
        PythonCallableWithSource('import math\nlambda x: math.sqrt(x) + x')(
            100),
        110)

  def test_def(self):
    self.assertEqual(
        PythonCallableWithSource(
            """
            def foo(x):
                return x * x
        """)(10),
        100)

  def test_def_with_preamble(self):
    self.assertEqual(
        PythonCallableWithSource(
            """
            def bar(x):
                return x + 1

            def foo(x):
                return bar(x) * x
        """)(10),
        110)

  def test_class(self):
    self.assertEqual(
        PythonCallableWithSource(
            """
            class BareClass:
              def __init__(self, x):
                self.x = x
        """)(10).x,
        10)

    self.assertEqual(
        PythonCallableWithSource(
            """
            class SubClass(object):
              def __init__(self, x):
                self.x = x
        """)(10).x,
        10)


if __name__ == '__main__':
  unittest.main()
