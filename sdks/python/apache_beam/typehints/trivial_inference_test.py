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

"""Tests for apache_beam.typehints.trivial_inference."""

from __future__ import absolute_import

import os
import sys
import unittest

from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

global_int = 1


@unittest.skipIf(sys.version_info >= (3, 6, 0) and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3.6. '
                 'See BEAM-6877')
class TrivialInferenceTest(unittest.TestCase):

  def assertReturnType(self, expected, f, inputs=()):
    self.assertEquals(expected, trivial_inference.infer_return_type(f, inputs))

  def testIdentity(self):
    self.assertReturnType(int, lambda x: x, [int])

  def testIndexing(self):
    self.assertReturnType(int, lambda x: x[0], [typehints.Tuple[int, str]])
    self.assertReturnType(str, lambda x: x[1], [typehints.Tuple[int, str]])
    self.assertReturnType(str, lambda x: x[1], [typehints.List[str]])

  def testTuples(self):
    self.assertReturnType(
        typehints.Tuple[typehints.Tuple[()], int], lambda x: ((), x), [int])
    self.assertReturnType(
        typehints.Tuple[str, int, float], lambda x: (x, 0, 1.0), [str])

  def testGetItem(self):
    def reverse(ab):
      return ab[-1], ab[0]
    self.assertReturnType(
        typehints.Tuple[typehints.Any, typehints.Any], reverse, [typehints.Any])
    self.assertReturnType(
        typehints.Tuple[int, float], reverse, [typehints.Tuple[float, int]])
    self.assertReturnType(
        typehints.Tuple[int, str], reverse, [typehints.Tuple[str, float, int]])
    self.assertReturnType(
        typehints.Tuple[int, int], reverse, [typehints.List[int]])
    self.assertReturnType(
        typehints.List[int], lambda v: v[::-1], [typehints.List[int]])

  def testUnpack(self):
    def reverse(a_b):
      (a, b) = a_b
      return b, a
    any_tuple = typehints.Tuple[typehints.Any, typehints.Any]
    self.assertReturnType(
        typehints.Tuple[int, float], reverse, [typehints.Tuple[float, int]])
    self.assertReturnType(
        typehints.Tuple[int, int], reverse, [typehints.Tuple[int, ...]])
    self.assertReturnType(
        typehints.Tuple[int, int], reverse, [typehints.List[int]])
    self.assertReturnType(
        typehints.Tuple[typehints.Union[int, float, str],
                        typehints.Union[int, float, str]],
        reverse, [typehints.Tuple[int, float, str]])
    self.assertReturnType(any_tuple, reverse, [typehints.Any])

    self.assertReturnType(typehints.Tuple[int, float],
                          reverse, [trivial_inference.Const((1.0, 1))])
    self.assertReturnType(any_tuple,
                          reverse, [trivial_inference.Const((1, 2, 3))])

  def testNoneReturn(self):
    def func(a):
      if a == 5:
        return a
      return None
    self.assertReturnType(typehints.Union[int, type(None)], func, [int])

  def testSimpleList(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [1, 2],
        [typehints.Tuple[int, ...]])

    self.assertReturnType(
        typehints.List[typehints.Any],
        lambda xs: list(xs), # List is a disallowed builtin
        [typehints.Tuple[int, ...]])

  def testListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, ...]])

  @unittest.skipIf(sys.version_info[0] == 3 and
                   os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                   'This test still needs to be fixed on Python 3. '
                   'See BEAM-6877')
  def testTupleListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, int, int]])
    self.assertReturnType(
        typehints.List[typehints.Union[int, float]],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, float]])
    # TODO(luke-zhu): This test fails in Python 3
    self.assertReturnType(
        typehints.List[typehints.Tuple[str, int]],
        lambda kvs: [(kvs[0], v) for v in kvs[1]],
        [typehints.Tuple[str, typehints.Iterable[int]]])
    self.assertReturnType(
        typehints.List[typehints.Tuple[str, typehints.Union[str, int], int]],
        lambda L: [(a, a or b, b) for a, b in L],
        [typehints.Iterable[typehints.Tuple[str, int]]])

  def testGenerator(self):

    def foo(x, y):
      yield x
      yield y

    self.assertReturnType(typehints.Iterable[int], foo, [int, int])
    self.assertReturnType(
        typehints.Iterable[typehints.Union[int, float]], foo, [int, float])

  def testGeneratorComprehension(self):
    self.assertReturnType(
        typehints.Iterable[int],
        lambda xs: (x for x in xs),
        [typehints.Tuple[int, ...]])

  def testBinOp(self):
    self.assertReturnType(int, lambda a, b: a + b, [int, int])
    self.assertReturnType(
        typehints.Any, lambda a, b: a + b, [int, typehints.Any])
    self.assertReturnType(
        typehints.List[typehints.Union[int, str]], lambda a, b: a + b,
        [typehints.List[int], typehints.List[str]])

  def testCall(self):
    f = lambda x, *args: x
    self.assertReturnType(
        typehints.Tuple[int, float], lambda: (f(1), f(2.0, 3)))
    # We could do better here, but this is at least correct.
    self.assertReturnType(
        typehints.Tuple[int, typehints.Any], lambda: (1, f(x=1.0)))

  def testClosure(self):
    x = 1
    y = 1.0
    self.assertReturnType(typehints.Tuple[int, float], lambda: (x, y))

  def testGlobals(self):
    self.assertReturnType(int, lambda: global_int)

  def testBuiltins(self):
    self.assertReturnType(int, lambda x: len(x), [typehints.Any])

  def testGetAttr(self):
    self.assertReturnType(
        typehints.Tuple[str, typehints.Any],
        lambda: (typehints.__doc__, typehints.fake))

  def testMethod(self):

    class A(object):
      def m(self, x):
        return x

    self.assertReturnType(int, lambda: A().m(3))
    self.assertReturnType(float, lambda: A.m(A(), 3.0))

  def testAlwaysReturnsEarly(self):

    def some_fn(v):
      if v:
        return 1
      return 2

    self.assertReturnType(int, some_fn)

  def testDict(self):
    self.assertReturnType(
        typehints.Dict[typehints.Any, typehints.Any], lambda: {})

  def testDictComprehension(self):
    # Just ensure it doesn't crash.
    fields = []
    self.assertReturnType(
        typehints.Any,
        lambda row: {f: row[f] for f in fields}, [typehints.Any])


if __name__ == '__main__':
  unittest.main()
