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

"""Tests for decorators module."""

# pytype: skip-file

import functools
import sys
import typing
import unittest

from apache_beam import Map
from apache_beam.typehints import Any
from apache_beam.typehints import Dict
from apache_beam.typehints import List
from apache_beam.typehints import Tuple
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints import TypeVariable
from apache_beam.typehints import WithTypeHints
from apache_beam.typehints import decorators
from apache_beam.typehints import typehints

T = TypeVariable('T')
# Name is 'T' so it converts to a beam type with the same name.
# mypy requires that the name of the variable match, so we must ignore this.
# pylint: disable=typevar-name-mismatch
T_typing = typing.TypeVar('T')  # type: ignore


class IOTypeHintsTest(unittest.TestCase):
  def test_get_signature(self):
    # Basic coverage only to make sure function works.
    def fn(a, b=1, *c, **d):
      return a, b, c, d

    s = decorators.get_signature(fn)
    self.assertListEqual(list(s.parameters), ['a', 'b', 'c', 'd'])

  def test_get_signature_builtin(self):
    s = decorators.get_signature(list)
    self.assertListEqual(list(s.parameters), ['iterable'])
    self.assertEqual(s.return_annotation, List[Any])

  def test_from_callable_without_annotations(self):
    def fn(a, b=None, *args, **kwargs):
      return a, b, args, kwargs

    th = decorators.IOTypeHints.from_callable(fn)
    self.assertIsNone(th)

  def test_from_callable_builtin(self):
    th = decorators.IOTypeHints.from_callable(len)
    self.assertIsNone(th)

  def test_from_callable_method_descriptor(self):
    # from_callable() injects an annotation in this special type of builtin.
    th = decorators.IOTypeHints.from_callable(str.strip)
    if sys.version_info >= (3, 7):
      self.assertEqual(th.input_types, ((str, Any), {}))
    else:
      self.assertEqual(
          th.input_types,
          ((str, decorators._ANY_VAR_POSITIONAL), {
              '__unknown__keywords': decorators._ANY_VAR_KEYWORD
          }))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_strip_iterable_not_simple_output_noop(self):
    th = decorators.IOTypeHints(
        input_types=None, output_types=((int, str), {}), origin=[])
    th = th.strip_iterable()
    self.assertEqual(((int, str), {}), th.output_types)

  def _test_strip_iterable(self, before, expected_after):
    th = decorators.IOTypeHints(
        input_types=None, output_types=((before, ), {}), origin=[])
    after = th.strip_iterable()
    self.assertEqual(((expected_after, ), {}), after.output_types)

  def test_with_output_types_from(self):
    th = decorators.IOTypeHints(
        input_types=((int), {
            'foo': str
        }),
        output_types=((int, str), {}),
        origin=[])

    self.assertEqual(
        th.with_output_types_from(decorators.IOTypeHints.empty()),
        decorators.IOTypeHints(
            input_types=((int), {
                'foo': str
            }), output_types=None, origin=[]))

    self.assertEqual(
        decorators.IOTypeHints.empty().with_output_types_from(th),
        decorators.IOTypeHints(
            input_types=None, output_types=((int, str), {}), origin=[]))

  def test_with_input_types_from(self):
    th = decorators.IOTypeHints(
        input_types=((int), {
            'foo': str
        }),
        output_types=((int, str), {}),
        origin=[])

    self.assertEqual(
        th.with_input_types_from(decorators.IOTypeHints.empty()),
        decorators.IOTypeHints(
            input_types=None, output_types=((int, str), {}), origin=[]))

    self.assertEqual(
        decorators.IOTypeHints.empty().with_input_types_from(th),
        decorators.IOTypeHints(
            input_types=((int), {
                'foo': str
            }), output_types=None, origin=[]))

  def _test_strip_iterable_fail(self, before):
    with self.assertRaisesRegex(ValueError, r'not iterable'):
      self._test_strip_iterable(before, None)

  def test_strip_iterable(self):
    self._test_strip_iterable(None, None)
    self._test_strip_iterable(typehints.Any, typehints.Any)
    self._test_strip_iterable(typehints.Iterable[str], str)
    self._test_strip_iterable(typehints.List[str], str)
    self._test_strip_iterable(typehints.Iterator[str], str)
    self._test_strip_iterable(typehints.Generator[str], str)
    self._test_strip_iterable(typehints.Tuple[str], str)
    self._test_strip_iterable(
        typehints.Tuple[str, int], typehints.Union[str, int])
    self._test_strip_iterable(typehints.Tuple[str, ...], str)
    self._test_strip_iterable(typehints.KV[str, int], typehints.Union[str, int])
    self._test_strip_iterable(typehints.Set[str], str)
    self._test_strip_iterable(typehints.FrozenSet[str], str)

    self._test_strip_iterable_fail(typehints.Union[str, int])
    self._test_strip_iterable_fail(typehints.Optional[str])
    self._test_strip_iterable_fail(
        typehints.WindowedValue[str])  # type: ignore[misc]
    self._test_strip_iterable_fail(typehints.Dict[str, int])

  def test_make_traceback(self):
    origin = ''.join(
        decorators.IOTypeHints.empty().with_input_types(str).origin)
    self.assertRegex(origin, __name__)
    self.assertNotRegex(origin, r'\b_make_traceback')

  def test_origin(self):
    th = decorators.IOTypeHints.empty()
    self.assertListEqual([], th.origin)
    th = th.with_input_types(str)
    self.assertRegex(th.debug_str(), r'with_input_types')
    th = th.with_output_types(str)
    self.assertRegex(th.debug_str(), r'(?s)with_output_types.*with_input_types')

    th = decorators.IOTypeHints.empty().with_output_types(str)
    th2 = decorators.IOTypeHints.empty().with_input_types(int)
    th = th.with_defaults(th2)
    self.assertRegex(th.debug_str(), r'(?s)based on:.*\'str\'.*and:.*\'int\'')

  def test_with_defaults_noop_does_not_grow_origin(self):
    th = decorators.IOTypeHints.empty()
    expected_id = id(th)
    th = th.with_defaults(None)
    self.assertEqual(expected_id, id(th))
    th = th.with_defaults(decorators.IOTypeHints.empty())
    self.assertEqual(expected_id, id(th))

    th = th.with_input_types(str)
    expected_id = id(th)
    th = th.with_defaults(th)
    self.assertEqual(expected_id, id(th))

    th2 = th.with_output_types(int)
    th = th.with_defaults(th2)
    self.assertNotEqual(expected_id, id(th))

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

  def test_origin_annotated(self):
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


class WithTypeHintsTest(unittest.TestCase):
  def test_get_type_hints_no_settings(self):
    class Base(WithTypeHints):
      pass

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, None)
    self.assertEqual(th.output_types, None)

  def test_get_type_hints_class_decorators(self):
    @decorators.with_input_types(int, str)
    @decorators.with_output_types(int)
    class Base(WithTypeHints):
      pass

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((int, str), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_get_type_hints_class_defaults(self):
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((int, str), {}), output_types=((int, ), {}), origin=[])

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((int, str), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_get_type_hints_precedence_defaults_over_decorators(self):
    @decorators.with_input_types(int)
    @decorators.with_output_types(str)
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((float, ), {}), output_types=None, origin=[])

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((float, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_get_type_hints_precedence_instance_over_defaults(self):
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((float, ), {}), output_types=((str, ), {}), origin=[])

    th = Base().with_input_types(int).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_inherits_does_not_modify(self):
    # See BEAM-8629.
    @decorators.with_output_types(int)
    class Subclass(WithTypeHints):
      def __init__(self):
        pass  # intentionally avoiding super call

    # These should be equal, but not the same object lest mutating the instance
    # mutates the class.
    self.assertIsNot(
        Subclass()._get_or_create_type_hints(), Subclass._type_hints)
    self.assertEqual(Subclass().get_type_hints(), Subclass._type_hints)
    self.assertNotEqual(
        Subclass().with_input_types(str)._type_hints, Subclass._type_hints)


class DecoratorsTest(unittest.TestCase):
  def tearDown(self):
    decorators._disable_from_callable = False

  def test_disable_type_annotations(self):
    self.assertFalse(decorators._disable_from_callable)
    decorators.disable_type_annotations()
    self.assertTrue(decorators._disable_from_callable)

  def test_no_annotations_on_same_function(self):
    def fn(a: int) -> int:
      return a

    with self.assertRaisesRegex(TypeCheckError,
                                r'requires .*int.* but was applied .*str'):
      _ = ['a', 'b', 'c'] | Map(fn)

    # Same pipeline doesn't raise without annotations on fn.
    fn = decorators.no_annotations(fn)
    _ = ['a', 'b', 'c'] | Map(fn)

  def test_no_annotations_on_diff_function(self):
    def fn(a: int) -> int:
      return a

    _ = [1, 2, 3] | Map(fn)  # Doesn't raise - correct types.

    with self.assertRaisesRegex(TypeCheckError,
                                r'requires .*int.* but was applied .*str'):
      _ = ['a', 'b', 'c'] | Map(fn)

    @decorators.no_annotations
    def fn2(a: int) -> int:
      return a

    _ = ['a', 'b', 'c'] | Map(fn2)  # Doesn't raise - no input type hints.


if __name__ == '__main__':
  unittest.main()
