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

from __future__ import absolute_import

import sys
import typing
import unittest

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.native_type_compatibility import convert_to_beam_types
from apache_beam.typehints.native_type_compatibility import convert_to_typing_type
from apache_beam.typehints.native_type_compatibility import convert_to_typing_types

_TestNamedTuple = typing.NamedTuple('_TestNamedTuple',
                                    [('age', int), ('name', bytes)])
_TestFlatAlias = typing.Tuple[bytes, float]
_TestNestedAlias = typing.List[_TestFlatAlias]


class _TestClass(object):
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
        ('simple unary tuple', typing.Tuple[bytes],
         typehints.Tuple[bytes]),
        ('simple union', typing.Union[int, bytes, float],
         typehints.Union[int, bytes, float]),
        ('namedtuple', _TestNamedTuple, _TestNamedTuple),
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

  def test_string_literal_converted_to_any(self):
    self.assertEqual(
        typehints.Any,
        convert_to_beam_type('typing.List[int]'))

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


if __name__ == '__main__':
  unittest.main()
