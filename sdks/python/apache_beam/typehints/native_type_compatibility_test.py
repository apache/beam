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

import typing
import unittest

from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import typehints

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
        ('simple optional', typing.Optional[int], typehints.Optional[int]),
        ('simple set', typing.Set[float], typehints.Set[float]),
        ('simple unary tuple', typing.Tuple[bytes],
         typehints.Tuple[bytes]),
        ('simple union', typing.Union[int, bytes, float],
         typehints.Union[int, bytes, float]),
        ('namedtuple', _TestNamedTuple, typehints.Any),
        ('test class', _TestClass, _TestClass),
        ('test class in list', typing.List[_TestClass],
         typehints.List[_TestClass]),
        ('complex tuple', typing.Tuple[bytes, typing.List[typing.Tuple[
            bytes, typing.Union[int, bytes, float]]]],
         typehints.Tuple[bytes, typehints.List[typehints.Tuple[
             bytes, typehints.Union[int, bytes, float]]]]),
        ('flat alias', _TestFlatAlias, typehints.Tuple[bytes, float]),
        ('nested alias', _TestNestedAlias,
         typehints.List[typehints.Tuple[bytes, float]]),
        ('complex dict',
         typing.Dict[bytes, typing.List[typing.Tuple[bytes, _TestClass]]],
         typehints.Dict[bytes, typehints.List[typehints.Tuple[
             bytes, _TestClass]]])
    ]

    for test_case in test_cases:
      # Unlike typing types, Beam types are guaranteed to compare equal.
      description = test_case[0]
      typing_type = test_case[1]
      beam_type = test_case[2]
      self.assertEqual(
          native_type_compatibility.convert_to_beam_type(typing_type),
          beam_type, description)

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
    self.assertEqual(
        native_type_compatibility.convert_to_beam_types(typing_types),
        beam_types)


if __name__ == '__main__':
  unittest.main()
