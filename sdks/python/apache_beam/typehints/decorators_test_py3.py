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

"""Tests for decorators module with Python 3 syntax not supported by 2.7."""

# pytype: skip-file

from __future__ import absolute_import

import functools
import typing
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from apache_beam import Map
from apache_beam.typehints import Any
from apache_beam.typehints import Dict
from apache_beam.typehints import List
from apache_beam.typehints import Tuple
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints import TypeVariable
from apache_beam.typehints import decorators

T = TypeVariable('T')
# Name is 'T' so it converts to a beam type with the same name.
# mypy requires that the name of the variable match, so we must ignore this.
T_typing = typing.TypeVar('T')  # type: ignore


class IOTypeHintsTest(unittest.TestCase):
  def test_from_callable(self):
    def fn(
        a: int,
        b: str = '',
        *args: Tuple[T],
        foo: List[int],
        **kwargs: Dict[str, str]) -> Tuple[Any, ...]:
      return a, b, args, foo, kwargs

    th = decorators.IOTypeHints.from_callable(fn)
    self.assertEqual(
        th.input_types, ((int, str, Tuple[T]), {
            'foo': List[int], 'kwargs': Dict[str, str]
        }))
    self.assertEqual(th.output_types, ((Tuple[Any, ...], ), {}))

  def test_from_callable_partial_annotations(self):
    def fn(a: int, b=None, *args, foo: List[int], **kwargs):
      return a, b, args, foo, kwargs

    th = decorators.IOTypeHints.from_callable(fn)
    self.assertEqual(
        th.input_types,
        ((int, Any, Tuple[Any, ...]), {
            'foo': List[int], 'kwargs': Dict[Any, Any]
        }))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_from_callable_class(self):
    class Class(object):
      def __init__(self, unused_arg: int):
        pass

    th = decorators.IOTypeHints.from_callable(Class)
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((Class, ), {}))

  def test_from_callable_method(self):
    class Class(object):
      def method(self, arg: T = None) -> None:
        pass

    th = decorators.IOTypeHints.from_callable(Class.method)
    self.assertEqual(th.input_types, ((Any, T), {}))
    self.assertEqual(th.output_types, ((None, ), {}))

    th = decorators.IOTypeHints.from_callable(Class().method)
    self.assertEqual(th.input_types, ((T, ), {}))
    self.assertEqual(th.output_types, ((None, ), {}))

  def test_from_callable_convert_to_beam_types(self):
    def fn(
        a: typing.List[int],
        b: str = '',
        *args: typing.Tuple[T_typing],
        foo: typing.List[int],
        **kwargs: typing.Dict[str, str]) -> typing.Tuple[typing.Any, ...]:
      return a, b, args, foo, kwargs

    th = decorators.IOTypeHints.from_callable(fn)
    self.assertEqual(
        th.input_types,
        ((List[int], str, Tuple[T]), {
            'foo': List[int], 'kwargs': Dict[str, str]
        }))
    self.assertEqual(th.output_types, ((Tuple[Any, ...], ), {}))

  def test_from_callable_partial(self):
    def fn(a: int) -> int:
      return a

    # functools.partial objects don't have __name__ attributes by default.
    fn = functools.partial(fn, 1)
    th = decorators.IOTypeHints.from_callable(fn)
    self.assertRegex(th.debug_str(), r'unknown')

  def test_getcallargs_forhints(self):
    def fn(
        a: int,
        b: str = '',
        *args: Tuple[T],
        foo: List[int],
        **kwargs: Dict[str, str]) -> Tuple[Any, ...]:
      return a, b, args, foo, kwargs

    callargs = decorators.getcallargs_forhints(fn, float, foo=List[str])
    self.assertDictEqual(
        callargs,
        {
            'a': float,
            'b': str,
            'args': Tuple[T],
            'foo': List[str],
            'kwargs': Dict[str, str]
        })

  def test_getcallargs_forhints_default_arg(self):
    # Default args are not necessarily types, so they should be ignored.
    def fn(a=List[int], b=None, *args, foo=(), **kwargs) -> Tuple[Any, ...]:
      return a, b, args, foo, kwargs

    callargs = decorators.getcallargs_forhints(fn)
    self.assertDictEqual(
        callargs,
        {
            'a': Any,
            'b': Any,
            'args': Tuple[Any, ...],
            'foo': Any,
            'kwargs': Dict[Any, Any]
        })

  def test_getcallargs_forhints_missing_arg(self):
    def fn(a, b=None, *args, foo, **kwargs):
      return a, b, args, foo, kwargs

    with self.assertRaisesRegex(decorators.TypeCheckError, "missing.*'a'"):
      decorators.getcallargs_forhints(fn, foo=List[int])
    with self.assertRaisesRegex(decorators.TypeCheckError, "missing.*'foo'"):
      decorators.getcallargs_forhints(fn, 5)

  def test_origin(self):
    def annotated(e: str) -> str:
      return e

    t = Map(annotated)
    th = t.get_type_hints()
    th = th.with_input_types(str)
    self.assertRegex(th.debug_str(), r'with_input_types')
    th = th.with_output_types(str)
    self.assertRegex(
        th.debug_str(),
        r'(?s)with_output_types.*with_input_types.*Map.annotated')


class DecoratorsTest(unittest.TestCase):
  def test_no_annotations(self):
    def fn(a: int) -> int:
      return a

    with self.assertRaisesRegex(TypeCheckError,
                                r'requires .*int.* but got .*str'):
      _ = ['a', 'b', 'c'] | Map(fn)

    # Same pipeline doesn't raise without annotations on fn.
    fn = decorators.no_annotations(fn)
    _ = ['a', 'b', 'c'] | Map(fn)


class DecoratorsTest(unittest.TestCase):
  def test_no_annotations(self):
    def fn(a: int) -> int:
      return a

    _ = [1, 2, 3] | Map(fn)  # Doesn't raise - correct types.

    with self.assertRaisesRegex(TypeCheckError,
                                r'requires .*int.* but got .*str'):
      _ = ['a', 'b', 'c'] | Map(fn)

    @decorators.no_annotations
    def fn2(a: int) -> int:
      return a

    _ = ['a', 'b', 'c'] | Map(fn2)  # Doesn't raise - no input type hints.


if __name__ == '__main__':
  unittest.main()
