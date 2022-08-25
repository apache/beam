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

import sys
import typing
import unittest

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.native_type_compatibility import convert_to_beam_types
from apache_beam.typehints.native_type_compatibility import convert_to_typing_type
from apache_beam.typehints.native_type_compatibility import convert_to_typing_types
from apache_beam.typehints.native_type_compatibility import is_any

_TestNamedTuple = typing.NamedTuple(
    '_TestNamedTuple', [('age', int), ('name', bytes)])
_TestFlatAlias = typing.Tuple[bytes, float]
_TestNestedAlias = typing.List[_TestFlatAlias]


class _TestClass(object):
  pass


T = typing.TypeVar('T')


class _TestGeneric(typing.Generic[T]):
  pass


class NativeTypeCompatibilityTest(unittest.TestCase):
  def test_convert_to_beam_type(self):
    test_cases = [
        ('raw bytes', bytes, bytes),
        ('raw int', int, int),
        ('raw float', float, float),
        ('any', typing.Any, typehints.Any),
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
        ('simple union', typing.Union[int, bytes, float],
         typehints.Union[int, bytes, float]),
        ('namedtuple', _TestNamedTuple, _TestNamedTuple),
        ('test class', _TestClass, _TestClass),
        ('test class in list', typing.List[_TestClass],
         typehints.List[_TestClass]),
        ('generic bare', _TestGeneric, _TestGeneric),
        ('generic subscripted', _TestGeneric[int], _TestGeneric[int]),
        ('complex tuple', typing.Tuple[bytes, typing.List[typing.Tuple[
            bytes, typing.Union[int, bytes, float]]]],
         typehints.Tuple[bytes, typehints.List[typehints.Tuple[
             bytes, typehints.Union[int, bytes, float]]]]),
        ('arbitrary-length tuple', typing.Tuple[int, ...],
         typehints.Tuple[int, ...]),
        ('flat alias', _TestFlatAlias, typehints.Tuple[bytes, float]),  # type: ignore[misc]
        ('nested alias', _TestNestedAlias,
         typehints.List[typehints.Tuple[bytes, float]]),
        ('complex dict',
         typing.Dict[bytes, typing.List[typing.Tuple[bytes, _TestClass]]],
         typehints.Dict[bytes, typehints.List[typehints.Tuple[
             bytes, _TestClass]]]),
        ('type var', typing.TypeVar('T'), typehints.TypeVariable('T')),
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
      converted_typing_type = convert_to_typing_type(converted_beam_type)
      self.assertEqual(converted_typing_type, typing_type, description)

  def test_generator_converted_to_iterator(self):
    self.assertEqual(
        typehints.Iterator[int],
        convert_to_beam_type(typing.Generator[int, None, None]))

  def test_newtype(self):
    self.assertEqual(
        typehints.Any, convert_to_beam_type(typing.NewType('Number', int)))

  def test_pattern(self):
    # TODO(https://github.com/apache/beam/issues/20489): Unsupported.
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Pattern))
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Pattern[str]))
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Pattern[bytes]))

  def test_match(self):
    # TODO(https://github.com/apache/beam/issues/20489): Unsupported.
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Match))
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Match[str]))
    self.assertEqual(typehints.Any, convert_to_beam_type(typing.Match[bytes]))

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
    ]
    if sys.version_info >= (3, 7):
      test_cases += [
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
        typing.List[bytes],
        typing.List[typing.Tuple[bytes, int]],
        typing.Union[int, typing.List[int]]
    ]
    beam_types = [
        bytes,
        typehints.List[bytes],
        typehints.List[typehints.Tuple[bytes, int]],
        typehints.Union[int, typehints.List[int]]
    ]
    converted_beam_types = convert_to_beam_types(typing_types)
    self.assertEqual(converted_beam_types, beam_types)
    converted_typing_types = convert_to_typing_types(converted_beam_types)
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


if __name__ == '__main__':
  unittest.main()
