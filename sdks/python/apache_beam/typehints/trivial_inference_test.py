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

# pytype: skip-file

from __future__ import absolute_import

import sys
import types
import unittest

import apache_beam as beam
from apache_beam.typehints import row_type
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

global_int = 1


class TrivialInferenceTest(unittest.TestCase):
  def assertReturnType(self, expected, f, inputs=(), depth=5):
    self.assertEqual(
        expected,
        trivial_inference.infer_return_type(f, inputs, debug=True, depth=depth))

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

  def testGetItemSlice(self):
    self.assertReturnType(
        typehints.List[int], lambda v: v[::-1], [typehints.List[int]])
    self.assertReturnType(
        typehints.Tuple[int], lambda v: v[::-1], [typehints.Tuple[int]])
    self.assertReturnType(str, lambda v: v[::-1], [str])
    self.assertReturnType(typehints.Any, lambda v: v[::-1], [typehints.Any])
    self.assertReturnType(typehints.Any, lambda v: v[::-1], [object])
    if sys.version_info >= (3, ):
      # Test binary_subscr on a slice of a Const. On Py2.7 this will use the
      # unsupported opcode SLICE+0.
      test_list = ['a', 'b']
      self.assertReturnType(typehints.List[str], lambda: test_list[:], [])

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

    self.assertReturnType(
        typehints.Tuple[int, float],
        reverse, [trivial_inference.Const((1.0, 1))])
    self.assertReturnType(
        any_tuple, reverse, [trivial_inference.Const((1, 2, 3))])

  @unittest.skipIf(sys.version_info < (3, ), 'BUILD_MAP better in Python 3')
  def testBuildMap(self):
    self.assertReturnType(
        typehints.Dict[typehints.Any, typehints.Any],
        lambda k,
        v: {}, [int, float])
    self.assertReturnType(
        typehints.Dict[int, float], lambda k, v: {k: v}, [int, float])
    self.assertReturnType(
        typehints.Dict[int, typehints.Union[float, str]],
        lambda k1,
        v1,
        k2,
        v2: {
            k1: v1, k2: v2
        }, [int, float, int, str])

    # Constant map.
    self.assertReturnType(
        typehints.Dict[str, typehints.Union[int, float]],
        lambda a,
        b: {
            'a': a, 'b': b
        }, [int, float])

  def testNoneReturn(self):
    def func(a):
      if a == 5:
        return a
      return None

    self.assertReturnType(typehints.Union[int, type(None)], func, [int])

  def testSimpleList(self):
    self.assertReturnType(
        typehints.List[int], lambda xs: [1, 2], [typehints.Tuple[int, ...]])

    self.assertReturnType(
        typehints.List[typehints.Any],
        lambda xs: list(xs),  # List is a disallowed builtin
        [typehints.Tuple[int, ...]])

  def testListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs], [typehints.Tuple[int, ...]])

  def testTupleListComprehension(self):
    self.assertReturnType(
        typehints.List[int],
        lambda xs: [x for x in xs], [typehints.Tuple[int, int, int]])
    self.assertReturnType(
        typehints.List[typehints.Union[int, float]],
        lambda xs: [x for x in xs], [typehints.Tuple[int, float]])
    if sys.version_info[:2] == (3, 5):
      # A better result requires implementing the MAKE_CLOSURE opcode.
      expected = typehints.Any
    else:
      expected = typehints.List[typehints.Tuple[str, int]]
    self.assertReturnType(
        expected,
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
        lambda xs: (x for x in xs), [typehints.Tuple[int, ...]])

  def testBinOp(self):
    self.assertReturnType(int, lambda a, b: a + b, [int, int])
    self.assertReturnType(int, lambda a: a + 1, [int])
    self.assertReturnType(
        typehints.Any, lambda a, b: a + b, [int, typehints.Any])
    self.assertReturnType(
        typehints.List[typehints.Union[int, str]],
        lambda a,
        b: a + b, [typehints.List[int], typehints.List[str]])

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

  def testCallFunctionOnAny(self):
    # Tests inference when CALL_FUNCTION/CALL_METHOD's function argument is Any.
    # The function cannot be called but inference should continue. Also tests
    # that LOAD_ATTR/LOAD_METHOD implementations don't load builtin functions,
    # which also break inference since they don't disassemble.
    def call_function_on_any(s):
      # str.split is a builtin so opcodes.load_attr (load_method in Py3.7+)
      # should put Any on the stack.
      # If infer_return_type_func raises while trying to simulate CALL_FUNCTION
      # on Any, the result will be Any instead of int.
      s.split()
      return 0

    self.assertReturnType(int, call_function_on_any, [str])

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
    fields = []
    if sys.version_info >= (3, 6):
      expected_type = typehints.Dict[typehints.Any, typehints.Any]
    else:
      # For Python 2, just ensure it doesn't crash.
      expected_type = typehints.Any
    self.assertReturnType(
        expected_type, lambda row: {f: row[f]
                                    for f in fields}, [typehints.Any])

  def testDictComprehensionSimple(self):
    self.assertReturnType(
        typehints.Dict[str, int], lambda _list: {'a': 1
                                                 for _ in _list}, [])

  def testDepthFunction(self):
    def f(i):
      return i

    self.assertReturnType(typehints.Any, lambda i: f(i), [int], depth=0)
    self.assertReturnType(int, lambda i: f(i), [int], depth=1)

  def testDepthMethod(self):
    class A(object):
      def m(self, x):
        return x

    self.assertReturnType(typehints.Any, lambda: A().m(3), depth=0)
    self.assertReturnType(int, lambda: A().m(3), depth=1)

    self.assertReturnType(typehints.Any, lambda: A.m(A(), 3.0), depth=0)
    self.assertReturnType(float, lambda: A.m(A(), 3.0), depth=1)

  def testBuildTupleUnpackWithCall(self):
    # Lambda uses BUILD_TUPLE_UNPACK_WITH_CALL opcode in Python 3.6, 3.7.
    def fn(x1, x2, *unused_args):
      return x1, x2

    self.assertReturnType(
        typehints.Tuple[str, float],
        lambda x1,
        x2,
        _list: fn(x1, x2, *_list), [str, float, typehints.List[int]])
    # No *args
    self.assertReturnType(
        typehints.Tuple[str, typehints.List[int]],
        lambda x1,
        x2,
        _list: fn(x1, x2, *_list), [str, typehints.List[int]])

  @unittest.skipIf(sys.version_info < (3, 6), 'CALL_FUNCTION_EX is new in 3.6')
  def testCallFunctionEx(self):
    # Test when fn arguments are built using BUiLD_LIST.
    def fn(*args):
      return args

    self.assertReturnType(
        typehints.List[typehints.Union[str, float]],
        lambda x1,
        x2: fn(*[x1, x2]), [str, float])

  @unittest.skipIf(sys.version_info < (3, 6), 'CALL_FUNCTION_EX is new in 3.6')
  def testCallFunctionExKwargs(self):
    def fn(x1, x2, **unused_kwargs):
      return x1, x2

    # Keyword args are currently unsupported for CALL_FUNCTION_EX.
    self.assertReturnType(
        typehints.Any,
        lambda x1,
        x2,
        _dict: fn(x1, x2, **_dict), [str, float, typehints.List[int]])

  def testInstanceToType(self):
    class MyClass(object):
      def method(self):
        pass

    test_cases = [
        (typehints.Dict[str, int], {
            'a': 1
        }),
        (typehints.Dict[str, typehints.Union[str, int]], {
            'a': 1, 'b': 'c'
        }),
        (typehints.Dict[typehints.Any, typehints.Any], {}),
        (typehints.Set[str], {'a'}),
        (typehints.Set[typehints.Union[str, float]], {'a', 0.4}),
        (typehints.Set[typehints.Any], set()),
        (typehints.FrozenSet[str], frozenset(['a'])),
        (
            typehints.FrozenSet[typehints.Union[str, float]],
            frozenset(['a', 0.4])),
        (typehints.FrozenSet[typehints.Any], frozenset()),
        (typehints.Tuple[int], (1, )),
        (typehints.Tuple[int, int, str], (1, 2, '3')),
        (typehints.Tuple[()], ()),
        (typehints.List[int], [1]),
        (typehints.List[typehints.Union[int, str]], [1, 'a']),
        (typehints.List[typehints.Any], []),
        (type(None), None),
        (type(MyClass), MyClass),
        (MyClass, MyClass()),
        (type(MyClass.method), MyClass.method),
        (types.MethodType, MyClass().method),
    ]
    for expected_type, instance in test_cases:
      self.assertEqual(
          expected_type,
          trivial_inference.instance_to_type(instance),
          msg=instance)

  def testRow(self):
    self.assertReturnType(
        row_type.RowTypeConstraint([('x', int), ('y', str)]),
        lambda x,
        y: beam.Row(x=x + 1, y=y), [int, str])
    self.assertReturnType(
        row_type.RowTypeConstraint([('x', int), ('y', str)]),
        lambda x: beam.Row(x=x, y=str(x)), [int])


if __name__ == '__main__':
  unittest.main()
