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
import unittest


from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

global_int = 1


class TrivialInferenceTest(unittest.TestCase):

  def assertReturnType(self, expected, f, inputs=()):
    self.assertEquals(expected, trivial_inference.infer_return_type(f, inputs))

  def testIdentity(self):
    self.assertReturnType(int, lambda x: x, [int])

  def testTuples(self):
    self.assertReturnType(
        typehints.Tuple[typehints.Tuple[()], int], lambda x: ((), x), [int])
    self.assertReturnType(
        typehints.Tuple[str, int, float], lambda x: (x, 0, 1.0), [str])

  def testUnpack(self):
    def reverse((a, b)):
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

  def testListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, ...]])

  def testTupleListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, int, int]])
    self.assertReturnType(
        typehints.List[typehints.Union[int, float]],
        lambda xs: [x for x in xs],
        [typehints.Tuple[int, float]])

  def testGenerator(self):

    def foo(x, y):
      yield x
      yield y

    self.assertReturnType(typehints.Iterable[int], foo, [int, int])
    self.assertReturnType(
        typehints.Iterable[typehints.Union[int, float]], foo, [int, float])

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
      else:
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
