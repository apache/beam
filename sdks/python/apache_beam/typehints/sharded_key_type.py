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

"""Type constraint for `ShardedKey`.

Can be used like a type-hint, for instance,
  'ShardedKeyType[int]'
  'ShardedKeyType[Tuple[T]]'

The type constraint is registered to be associated with
:class:`apache_beam.coders.coders.ShardedKeyCoder`.
Mostly for internal use.
"""

# pytype: skip-file

from apache_beam import coders
from apache_beam.typehints import typehints
from apache_beam.utils.sharded_key import ShardedKey


class ShardedKeyTypeConstraint(typehints.TypeConstraint,
                               metaclass=typehints.GetitemConstructor):
  def __init__(self, key_type):
    typehints.validate_composite_type_param(
        key_type, error_msg_prefix='Parameter to ShardedKeyType hint')
    self.key_type = typehints.normalize(key_type)

  def _inner_types(self):
    yield self.key_type

  def _consistent_with_check_(self, sub):
    return (
        isinstance(sub, self.__class__) and
        typehints.is_consistent_with(sub.key_type, self.key_type))

  def type_check(self, instance):
    if not isinstance(instance, ShardedKey):
      raise typehints.CompositeTypeHintError(
          "ShardedKey type-constraint violated. Valid object instance "
          "must be of type 'ShardedKey'. Instead, an instance of '%s' "
          "was received." % (instance.__class__.__name__))

    try:
      typehints.check_constraint(self.key_type, instance.key)
    except (typehints.CompositeTypeHintError, typehints.SimpleTypeHintError):
      raise typehints.CompositeTypeHintError(
          "%s type-constraint violated. The type of key in 'ShardedKey' "
          "is incorrect. Expected an instance of type '%s', "
          "instead received an instance of type '%s'." % (
              repr(self),
              typehints._unified_repr(self.key_type),
              instance.key.__class__.__name__))

  def match_type_variables(self, concrete_type):
    if isinstance(concrete_type, ShardedKeyTypeConstraint):
      return typehints.match_type_variables(
          self.key_type, concrete_type.key_type)
    return {}

  def __eq__(self, other):
    return isinstance(
        other, ShardedKeyTypeConstraint) and self.key_type == other.key_type

  def __hash__(self):
    return hash(self.key_type)

  def __repr__(self):
    return 'ShardedKey[%s]' % typehints._unified_repr(self.key_type)


ShardedKeyType = ShardedKeyTypeConstraint
coders.typecoders.registry.register_coder(
    ShardedKeyType, coders.ShardedKeyCoder)
