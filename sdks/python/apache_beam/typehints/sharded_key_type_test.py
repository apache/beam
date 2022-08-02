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

"""Unit tests for the ShardedKeyTypeConstraint."""

# pytype: skip-file

from apache_beam.typehints import Tuple
from apache_beam.typehints import typehints
from apache_beam.typehints.sharded_key_type import ShardedKeyType
from apache_beam.typehints.typehints_test import TypeHintTestCase
from apache_beam.utils.sharded_key import ShardedKey


class ShardedKeyTypeConstraintTest(TypeHintTestCase):
  def test_compatibility(self):
    constraint1 = ShardedKeyType[int]
    constraint2 = ShardedKeyType[str]

    self.assertCompatible(constraint1, constraint1)
    self.assertCompatible(constraint2, constraint2)
    self.assertNotCompatible(constraint1, constraint2)

  def test_repr(self):
    constraint = ShardedKeyType[int]
    self.assertEqual('ShardedKey[int]', repr(constraint))

  def test_type_check_not_sharded_key(self):
    constraint = ShardedKeyType[int]
    obj = 5
    with self.assertRaises(TypeError) as e:
      constraint.type_check(obj)
    self.assertEqual(
        "ShardedKey type-constraint violated. Valid object instance must be of "
        "type 'ShardedKey'. Instead, an instance of 'int' was received.",
        e.exception.args[0])

  def test_type_check_invalid_key_type(self):
    constraint = ShardedKeyType[int]
    obj = ShardedKey(key='abc', shard_id=b'123')
    with self.assertRaises((TypeError, TypeError)) as e:
      constraint.type_check(obj)
    self.assertEqual(
        "ShardedKey[int] type-constraint violated. The type of key in "
        "'ShardedKey' is incorrect. Expected an instance of type 'int', "
        "instead received an instance of type 'str'.",
        e.exception.args[0])

  def test_type_check_valid_simple_type(self):
    constraint = ShardedKeyType[str]
    obj = ShardedKey(key='abc', shard_id=b'123')
    self.assertIsNone(constraint.type_check(obj))

  def test_type_check_valid_composite_type(self):
    constraint = ShardedKeyType[Tuple[int, str]]
    obj = ShardedKey(key=(1, 'a'), shard_id=b'123')
    self.assertIsNone(constraint.type_check(obj))

  def test_match_type_variables(self):
    K = typehints.TypeVariable('K')  # pylint: disable=invalid-name
    constraint = ShardedKeyType[K]
    self.assertEqual({K: int},
                     constraint.match_type_variables(ShardedKeyType[int]))

  def test_getitem(self):
    K = typehints.TypeVariable('K')  # pylint: disable=invalid-name
    T = typehints.TypeVariable('T')  # pylint: disable=invalid-name
    with self.assertRaisesRegex(TypeError,
                                'Parameter to ShardedKeyType hint.*'):
      _ = ShardedKeyType[K, T]
    with self.assertRaisesRegex(TypeError,
                                'Parameter to ShardedKeyType hint.*'):
      _ = ShardedKeyType[(K, T)]
