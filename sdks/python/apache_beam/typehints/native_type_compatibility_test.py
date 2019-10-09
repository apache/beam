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

from __future__ import absolute_import

import collections
import sys
import typing
import unittest

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import WindowedValue
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.native_type_compatibility import convert_to_beam_types
from apache_beam.typehints.native_type_compatibility import convert_to_typing_type
from apache_beam.typehints.native_type_compatibility import convert_to_typing_types
from apache_beam.typehints.native_type_compatibility import is_Any
from apache_beam.typehints.native_type_compatibility import is_Dict
from apache_beam.typehints.native_type_compatibility import is_Iterable
from apache_beam.typehints.native_type_compatibility import is_List
from apache_beam.typehints.native_type_compatibility import is_Set
from apache_beam.typehints.native_type_compatibility import is_Tuple
from apache_beam.typehints.native_type_compatibility import is_typing_type

_TestNamedTuple = typing.NamedTuple('_TestNamedTuple',
                                    [('age', int), ('name', bytes)])
_TestFlatAlias = typing.Tuple[bytes, float]
_TestNestedAlias = typing.List[_TestFlatAlias]


class _TestClass(object):
  pass


class NativeTypeCompatibilityTest(unittest.TestCase):

  def test_convert_to_beam_type(self):
    # Tests converting a typing type to Beam type and back to a typing type.
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
        ('simple unary tuple', typing.Tuple[bytes],
         typehints.Tuple[bytes]),
        ('simple union', typing.Union[int, bytes, float],
         typehints.Union[int, bytes, float]),
        ('test class', _TestClass, _TestClass),
        ('test class in list', typing.List[_TestClass],
         typehints.List[_TestClass]),
        ('complex tuple', typing.Tuple[bytes, typing.List[typing.Tuple[
            bytes, typing.Union[int, bytes, float]]]],
         typehints.Tuple[bytes, typehints.List[typehints.Tuple[
             bytes, typehints.Union[int, bytes, float]]]]),
        # TODO(BEAM-7713): This case seems to fail on Py3.5.2 but not 3.5.4.
        ('arbitrary-length tuple', typing.Tuple[int, ...],
         typehints.Tuple[int, ...])
        if sys.version_info >= (3, 5, 4) else None,
        ('flat alias', _TestFlatAlias, typehints.Tuple[bytes, float]),
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
        ('windowed value', WindowedValue[int], typehints.WindowedValue[int]),
        ('kv', typing.Tuple[str, int], typehints.KV[str, int]),
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

  def test_convert_to_beam_type_special_case(self):
    # This conversion is not reversible.
    self.assertEqual(typehints.Any, convert_to_beam_type(_TestNamedTuple))
    # Test conversion from KV to typing type.
    self.assertEqual(typing.Tuple[int, str],
                     convert_to_typing_type(typehints.KV[int, str]))

  def test_generator_converted_to_iterator(self):
    self.assertEqual(
        typehints.Iterator[int],
        convert_to_beam_type(typing.Generator[int, None, None]))

  def test_convert_nested_to_beam_type(self):
    self.assertEqual(
        typehints.List[typing.Any],
        typehints.List[typehints.Any])
    self.assertEqual(
        typehints.List[typing.Dict[int, str]],
        typehints.List[typehints.Dict[int, str]])

  def test_convert_to_beam_types(self):
    typing_types = [bytes, typing.List[bytes],
                    typing.List[typing.Tuple[bytes, int]],
                    typing.Union[int, typing.List[int]]]
    beam_types = [bytes, typehints.List[bytes],
                  typehints.List[typehints.Tuple[bytes, int]],
                  typehints.Union[int, typehints.List[int]]]
    converted_beam_types = convert_to_beam_types(typing_types)
    self.assertEqual(converted_beam_types, beam_types)
    converted_typing_types = convert_to_typing_types(converted_beam_types)
    self.assertEqual(converted_typing_types, typing_types)

  def test_is_typing_type(self):
    for t in [typing.NamedTuple,
              typing.List,
              typing.List[int]]:
      self.assertTrue(is_typing_type(t))

  def test_is_foo_negative(self):
    for t in [None,
              type(None),
              collections.namedtuple('Employee', ['name', 'id']),
              typing.NamedTuple('Employee', [('name', str), ('id', int)]),
              int,
              'a',
              type(self)]:
      self.assertFalse(is_Any(t))
      self.assertFalse(is_Dict(t))
      self.assertFalse(is_Iterable(t))
      self.assertFalse(is_List(t))
      self.assertFalse(is_Set(t))
      self.assertFalse(is_Tuple(t))
      self.assertFalse(is_typing_type(t))

  def test_is_Any(self):
    self.assertTrue(is_Any(typing.Any))
    self.assertFalse(is_Any(typing.List[int]))

  def test_is_dict(self):
    self.assertTrue(is_Dict(typing.Dict))
    self.assertTrue(is_Dict(typing.Dict[typing.Any, str]))
    self.assertFalse(is_Dict(typing.List[int]))

  def test_is_iterable(self):
    self.assertTrue(is_Iterable(typing.Iterable))
    self.assertTrue(is_Iterable(typing.Iterable[str]))
    self.assertFalse(is_Iterable(typing.List[int]))

  def test_is_list(self):
    self.assertTrue(is_List(typing.List))
    self.assertTrue(is_List(typing.List[str]))
    self.assertFalse(is_List(typing.Iterable[int]))

  def test_is_set(self):
    self.assertTrue(is_Set(typing.Set))
    self.assertTrue(is_Set(typing.Set[str]))
    self.assertFalse(is_Set(typing.Iterable[int]))

  def test_is_tuple(self):
    self.assertTrue(is_Tuple(typing.Tuple))
    self.assertTrue(is_Tuple(typing.Tuple[str]))
    self.assertFalse(is_Tuple(typing.Iterable[int]))


if __name__ == '__main__':
  unittest.main()
