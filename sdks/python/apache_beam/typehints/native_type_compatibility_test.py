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

"""Test for Beam type compatibility library."""

# pytype: skip-file

import collections.abc
import enum
import re
import typing
import unittest

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_builtin_to_typing
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.native_type_compatibility import convert_to_beam_types
from apache_beam.typehints.native_type_compatibility import convert_to_python_type
from apache_beam.typehints.native_type_compatibility import convert_to_python_types
from apache_beam.typehints.native_type_compatibility import convert_typing_to_builtin
from apache_beam.typehints.native_type_compatibility import is_any

_TestNamedTuple = typing.NamedTuple(
    '_TestNamedTuple', [('age', int), ('name', bytes)])
_TestFlatAlias = tuple[bytes, float]
_TestNestedAlias = list[_TestFlatAlias]
_TestFlatAliasTyping = typing.Tuple[bytes, float]
_TestNestedAliasTyping = typing.List[_TestFlatAliasTyping]


class _TestClass(object):
  pass


T = typing.TypeVar('T')
U = typing.TypeVar('U')


class _TestGeneric(typing.Generic[T]):
  pass


class _TestPair(typing.NamedTuple('TestTuple', [('first', T), ('second', T)]),
                typing.Generic[T]):
  pass


class _TestEnum(enum.Enum):
  FOO = enum.auto()
  BAR = enum.auto()


class _TestTypedDict(typing.TypedDict):
  foo: int
  bar: str


class NativeTypeCompatibilityTest(unittest.TestCase):
  def test_convert_to_beam_type(self):
    test_cases = [
        ('raw bytes', bytes, bytes),
        ('raw int', int, int),
        ('raw float', float, float),
        ('any', typing.Any, typehints.Any),
        ('simple dict', dict[bytes, int],
         typehints.Dict[bytes, int]),
        ('simple list', list[int], typehints.List[int]),
        ('simple iterable', collections.abc.Iterable[int],
         typehints.Iterable[int]),
        ('simple optional', typing.Optional[int], typehints.Optional[int]),
        ('simple set', set[float], typehints.Set[float]),
        ('simple frozenset',
         frozenset[float],
         typehints.FrozenSet[float]),
        ('simple unary tuple', tuple[bytes],
         typehints.Tuple[bytes]),
        ('simple union', typing.Union[int, bytes, float],
         typehints.Union[int, bytes, float]),
        ('namedtuple', _TestNamedTuple, _TestNamedTuple),
        ('test class', _TestClass, _TestClass),
        ('test class in list', list[_TestClass],
         typehints.List[_TestClass]),
        ('generic bare', _TestGeneric, _TestGeneric),
        ('generic subscripted', _TestGeneric[int], _TestGeneric[int]),
        ('complex tuple', tuple[bytes, list[tuple[
            bytes, typing.Union[int, bytes, float]]]],
         typehints.Tuple[bytes, typehints.List[typehints.Tuple[
             bytes, typehints.Union[int, bytes, float]]]]),
        ('arbitrary-length tuple', tuple[int, ...],
         typehints.Tuple[int, ...]),
        ('flat alias', _TestFlatAlias, typehints.Tuple[bytes, float]),  # type: ignore[misc]
        ('nested alias', _TestNestedAlias,
         typehints.List[typehints.Tuple[bytes, float]]),
        ('complex dict',
         dict[bytes, list[tuple[bytes, _TestClass]]],
         typehints.Dict[bytes, typehints.List[typehints.Tuple[
             bytes, _TestClass]]]),
        ('type var', typing.TypeVar('T'), typehints.TypeVariable('T')),
        ('nested type var',
         tuple[typing.TypeVar('K'), typing.TypeVar('V')],
         typehints.Tuple[typehints.TypeVariable('K'),
                         typehints.TypeVariable('V')]),
        ('iterator', collections.abc.Iterator[typing.Any],
         typehints.Iterator[typehints.Any]),
        ('nested generic bare', list[_TestGeneric],
         typehints.List[_TestGeneric]),
        ('nested generic subscripted', list[_TestGeneric[int]],
         typehints.List[_TestGeneric[int]]),
        ('nested generic with any', list[_TestPair[typing.Any]],
         typehints.List[_TestPair[typing.Any]]),
        ('raw enum', _TestEnum, _TestEnum),
    ]

    for test_case in test_cases:
      if test_case is None:
        continue
      # Unlike typing types, Beam types are guaranteed to compare equal.
      description = test_case[0]
      typing_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(typing_type)
      self.assertEqual(converted_beam_type, expected_beam_type, description)
      converted_typing_type = convert_to_python_type(converted_beam_type)
      self.assertEqual(converted_typing_type, typing_type, description)

  def test_convert_to_beam_type_with_typing_types(self):
    test_cases = [
        ('simple dict', typing.Dict[bytes, int],
         typehints.Dict[bytes, int]),
        ('simple list', typing.List[int], typehints.List[int]),
        ('simple iterable', typing.Iterable[int], typehints.Iterable[int]),
        ('simple optional', typing.Optional[int], typehints.Optional[int]),
        ('simple set', typing.Set[float], typehints.Set[float]),
        ('simple frozenset',
         typing.FrozenSet[float],
         typehints.FrozenSet[float]),
        ('simple unary tuple', typing.Tuple[bytes],
         typehints.Tuple[bytes]),
        ('test class in list', typing.List[_TestClass],
         typehints.List[_TestClass]),
        ('complex tuple', typing.Tuple[bytes, typing.List[typing.Tuple[
            bytes, typing.Union[int, bytes, float]]]],
         typehints.Tuple[bytes, typehints.List[typehints.Tuple[
             bytes, typehints.Union[int, bytes, float]]]]),
        ('arbitrary-length tuple', typing.Tuple[int, ...],
         typehints.Tuple[int, ...]),
        ('flat alias', _TestFlatAliasTyping, typehints.Tuple[bytes, float]),  # type: ignore[misc]
        ('nested alias', _TestNestedAliasTyping,
         typehints.List[typehints.Tuple[bytes, float]]),
        ('complex dict',
         typing.Dict[bytes, typing.List[typing.Tuple[bytes, _TestClass]]],
         typehints.Dict[bytes, typehints.List[typehints.Tuple[
             bytes, _TestClass]]]),
        ('nested type var',
         typing.Tuple[typing.TypeVar('K'), typing.TypeVar('V')],
         typehints.Tuple[typehints.TypeVariable('K'),
                         typehints.TypeVariable('V')]),
        ('iterator', typing.Iterator[typing.Any],
         typehints.Iterator[typehints.Any]),
        ('nested generic bare', typing.List[_TestGeneric],
         typehints.List[_TestGeneric]),
        ('nested generic subscripted', typing.List[_TestGeneric[int]],
         typehints.List[_TestGeneric[int]]),
        ('nested generic with any', typing.List[_TestPair[typing.Any]],
         typehints.List[_TestPair[typing.Any]]),
    ]

    for test_case in test_cases:
      description = test_case[0]
      builtins_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(builtins_type)
      self.assertEqual(converted_beam_type, expected_beam_type, description)

  def test_convert_to_beam_type_with_builtin_types(self):
    test_cases = [
        ('builtin dict', dict[str, int], typehints.Dict[str, int]),
        ('builtin list', list[str], typehints.List[str]),
        ('builtin tuple', tuple[str],
         typehints.Tuple[str]), ('builtin set', set[str], typehints.Set[str]),
        ('builtin frozenset', frozenset[int], typehints.FrozenSet[int]),
        (
            'nested builtin',
            dict[str, list[tuple[float]]],
            typehints.Dict[str, typehints.List[typehints.Tuple[float]]]),
        (
            'builtin nested tuple',
            tuple[str, list],
            typehints.Tuple[str, typehints.List[typehints.TypeVariable('T')]],
        )
    ]

    for test_case in test_cases:
      description = test_case[0]
      builtins_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(builtins_type)
      self.assertEqual(converted_beam_type, expected_beam_type, description)

  def test_convert_to_beam_type_with_collections_types(self):
    test_cases = [
        (
            'collection iterable',
            collections.abc.Iterable[int],
            typehints.Iterable[int]),
        (
            'collection generator',
            collections.abc.Generator[int, None, None],
            typehints.Generator[int]),
        (
            'collection iterator',
            collections.abc.Iterator[int],
            typehints.Iterator[int]),
        (
            'nested iterable',
            tuple[bytes, collections.abc.Iterable[int]],
            typehints.Tuple[bytes, typehints.Iterable[int]]),
        (
            'iterable over tuple',
            collections.abc.Iterable[tuple[str, int]],
            typehints.Iterable[typehints.Tuple[str, int]]),
        (
            'mapping',
            collections.abc.Mapping[str, int],
            typehints.Mapping[str, int]),
        ('set', collections.abc.Set[int], typehints.Set[int]),
        ('mutable set', collections.abc.MutableSet[int], typehints.Set[int]),
        (
            'enum mutable set',
            collections.abc.MutableSet[_TestEnum],
            typehints.Set[_TestEnum]),
        (
            'collection enum',
            collections.abc.Collection[_TestEnum],
            typehints.Collection[_TestEnum]),
        (
            'collection of tuples',
            collections.abc.Collection[tuple[str, int]],
            typehints.Collection[typehints.Tuple[str, int]]),
        (
            'nested sequence',
            tuple[collections.abc.Sequence[str], int],
            typehints.Tuple[typehints.Sequence[str], int]),
        (
            'sequence of tuples',
            collections.abc.Sequence[tuple[str, int]],
            typehints.Sequence[typehints.Tuple[str, int]]),
        (
            'ordered dict',
            collections.OrderedDict[str, int],
            typehints.Dict[str, int]),
        (
            'default dict',
            collections.defaultdict[str, int],
            typehints.Dict[str, int]),
        ('typed dict', _TestTypedDict, typehints.Dict[str, typehints.Any]),
        ('count', collections.Counter[str, int], typehints.Dict[str, int]),
        (
            'single param counter',
            collections.Counter[str],
            typehints.Dict[str, int]),
        (
            'bare callable',
            collections.abc.Callable,
            collections.abc.Callable,
        ),
        (
            'parameterized callable',
            collections.abc.Callable[[str], int],
            collections.abc.Callable[[str], int],
        ),
    ]

    for test_case in test_cases:
      description = test_case[0]
      builtins_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(builtins_type)
      self.assertEqual(converted_beam_type, expected_beam_type, description)

  def test_convert_builtin_to_typing(self):
    test_cases = [
        ('dict', dict[str, int], typing.Dict[str, int]),
        ('list', list[str], typing.List[str]),
        ('tuple', tuple[str], typing.Tuple[str]),
        ('set', set[str], typing.Set[str]),
        (
            'nested',
            dict[str, list[tuple[float]]],
            typing.Dict[str, typing.List[typing.Tuple[float]]]),
    ]

    for test_case in test_cases:
      description = test_case[0]
      builtin_type = test_case[1]
      expected_typing_type = test_case[2]
      converted_typing_type = convert_builtin_to_typing(builtin_type)
      self.assertEqual(converted_typing_type, expected_typing_type, description)

  def test_generator_converted_to_iterator(self):
    self.assertEqual(
        typehints.Iterator[int],
        convert_to_beam_type(typing.Generator[int, None, None]))

  def test_newtype(self):
    self.assertEqual(
        typehints.Any, convert_to_beam_type(typing.NewType('Number', int)))

  def test_pattern(self):
    self.assertEqual(re.Pattern, convert_to_beam_type(re.Pattern))
    self.assertEqual(re.Pattern[str], convert_to_beam_type(re.Pattern[str]))
    self.assertEqual(re.Pattern[bytes], convert_to_beam_type(re.Pattern[bytes]))
    self.assertNotEqual(
        re.Pattern[bytes], convert_to_beam_type(re.Pattern[str]))

  def test_match(self):
    self.assertEqual(re.Match, convert_to_beam_type(re.Match))
    self.assertEqual(re.Match[str], convert_to_beam_type(re.Match[str]))
    self.assertEqual(re.Match[bytes], convert_to_beam_type(re.Match[bytes]))
    self.assertNotEqual(re.Match[bytes], convert_to_beam_type(re.Match[str]))

  def test_forward_reference(self):
    self.assertEqual(typehints.Any, convert_to_beam_type('int'))
    self.assertEqual(typehints.Any, convert_to_beam_type('typing.List[int]'))
    self.assertEqual(
        typehints.List[typehints.Any], convert_to_beam_type(typing.List['int']))

  def test_convert_nested_to_beam_type(self):
    self.assertEqual(typehints.List[typing.Any], typehints.List[typehints.Any])
    self.assertEqual(
        typehints.List[typing.Dict[int, str]],
        typehints.List[typehints.Dict[int, str]])

  def test_convert_bare_types(self):
    # Conversions for unsubscripted types that have implicit subscripts.
    test_cases = [
        ('bare list', typing.List, typehints.List[typehints.TypeVariable('T')]),
        (
            'bare dict',
            typing.Dict,
            typehints.Dict[typehints.TypeVariable('KT'),
                           typehints.TypeVariable('VT')]),
        (
            'bare tuple',
            typing.Tuple,
            typehints.Tuple[typehints.TypeVariable('T'), ...]),
        ('bare set', typing.Set, typehints.Set[typehints.TypeVariable('T')]),
        (
            'bare frozenset',
            typing.FrozenSet,
            typehints.FrozenSet[typehints.TypeVariable(
                'T', use_name_in_eq=False)]),
        (
            'bare iterator',
            typing.Iterator,
            typehints.Iterator[typehints.TypeVariable('T_co')]),
        (
            'bare iterable',
            typing.Iterable,
            typehints.Iterable[typehints.TypeVariable('T_co')]),
        (
            'nested bare',
            typing.Tuple[typing.Iterator],
            typehints.Tuple[typehints.Iterator[typehints.TypeVariable('T_co')]]
        ),
        (
            'bare generator',
            typing.Generator,
            typehints.Generator[typehints.TypeVariable('T_co')]),
    ]
    for test_case in test_cases:
      description = test_case[0]
      typing_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(typing_type)
      self.assertEqual(expected_beam_type, converted_beam_type, description)

  def test_convert_bare_collections_types(self):
    # Conversions for unsubscripted types that have implicit subscripts.
    test_cases = [
        ('bare list', list, typehints.List[typehints.TypeVariable('T')]),
        (
            'bare dict',
            dict,
            typehints.Dict[typehints.TypeVariable('KT'),
                           typehints.TypeVariable('VT')]),
        (
            'bare tuple',
            tuple,
            typehints.Tuple[typehints.TypeVariable('T'), ...]),
        ('bare set', typing.Set, typehints.Set[typehints.TypeVariable('T')]),
        (
            'bare frozenset',
            frozenset,
            typehints.FrozenSet[typehints.TypeVariable(
                'T', use_name_in_eq=False)]),
        (
            'bare iterator',
            collections.abc.Iterator,
            typehints.Iterator[typehints.TypeVariable('T_co')]),
        (
            'bare iterable',
            collections.abc.Iterable,
            typehints.Iterable[typehints.TypeVariable('T_co')]),
        (
            'nested bare',
            tuple[collections.abc.Iterator],
            typehints.Tuple[typehints.Iterator[typehints.TypeVariable('T_co')]]
        ),
        (
            'bare generator',
            collections.abc.Generator,
            typehints.Generator[typehints.TypeVariable('T_co')]),
    ]
    for test_case in test_cases:
      description = test_case[0]
      typing_type = test_case[1]
      expected_beam_type = test_case[2]
      converted_beam_type = convert_to_beam_type(typing_type)
      self.assertEqual(expected_beam_type, converted_beam_type, description)

  def test_convert_bare_types_fail(self):
    # These conversions should fail.
    test_cases = [
        ('bare union', typing.Union),
    ]
    for test_case in test_cases:
      description = test_case[0]
      typing_type = test_case[1]
      with self.assertRaises(ValueError, msg=description):
        convert_to_beam_type(typing_type)

  def test_convert_to_beam_types(self):
    typing_types = [
        bytes,
        list[bytes],
        list[tuple[bytes, int]],
        typing.Union[int, list[int]]
    ]
    beam_types = [
        bytes,
        typehints.List[bytes],
        typehints.List[typehints.Tuple[bytes, int]],
        typehints.Union[int, typehints.List[int]]
    ]
    converted_beam_types = convert_to_beam_types(typing_types)
    self.assertEqual(converted_beam_types, beam_types)
    converted_typing_types = convert_to_python_types(converted_beam_types)
    self.assertEqual(converted_typing_types, typing_types)

  def test_is_any(self):
    test_cases = [
        (True, typing.Any),
        (False, typing.List[int]),
        (False, typing.Union),
        (False, 1),
        (False, 'a'),
    ]
    for expected, typ in test_cases:
      self.assertEqual(expected, is_any(typ), msg='%s' % typ)

  def test_convert_typing_to_builtin(self):
    test_cases = [
        ('list', typing.List[int],
         list[int]), ('dict', typing.Dict[str, int], dict[str, int]),
        ('tuple', typing.Tuple[str, int], tuple[str, int]),
        ('set', typing.Set[str], set[str]),
        ('frozenset', typing.FrozenSet[int], frozenset[int]),
        (
            'nested',
            typing.List[typing.Dict[str, typing.Tuple[int]]],
            list[dict[str, tuple[int]]]), ('typevar', typing.List[T], list[T]),
        ('nested_typevar', typing.Dict[T, typing.List[U]], dict[T, list[U]])
    ]

    for description, typing_type, expected_builtin_type in test_cases:
      builtin_type = convert_typing_to_builtin(typing_type)
      self.assertEqual(builtin_type, expected_builtin_type, description)


if __name__ == '__main__':
  unittest.main()
