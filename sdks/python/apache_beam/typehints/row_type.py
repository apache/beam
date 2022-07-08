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

# pytype: skip-file

from __future__ import annotations

from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple

# Name of the attribute added to user types (existing and generated) to store
# the corresponding schema ID
_BEAM_SCHEMA_ID = "_beam_schema_id"


class RowTypeConstraint(typehints.TypeConstraint):
  def __init__(self, fields: List[Tuple[str, type]], user_type=None):
    """For internal use only, no backwards comatibility guaratees.  See
    https://beam.apache.org/documentation/programming-guide/#schemas-for-pl-types
    for guidance on creating PCollections with inferred schemas.

    Note RowTypeConstraint does not currently store arbitrary functions for
    converting to/from the user type. Instead, we only support ``NamedTuple``
    user types and make the follow assumptions:

    - The user type can be constructed with field values as arguments in order
      (i.e. ``constructor(*field_values)``).
    - Field values can be accessed from instances of the user type by attribute
      (i.e. with ``getattr(obj, field_name)``).

    In the future we will add support for dataclasses
    ([#22085](https://github.com/apache/beam/issues/22085)) which also satisfy
    these assumptions.

    The RowTypeConstraint constructor should not be called directly (even
    internally to Beam). Prefer static methods ``from_user_type`` or
    ``from_fields``.

    Parameters:
      fields: a list of (name, type) tuples, representing the schema inferred
        from user_type.
      user_type: constructor for a user type (e.g. NamedTuple class) that is
        used to represent this schema in user code.
    """
    # Recursively wrap row types in a RowTypeConstraint
    self._fields = tuple((name, RowTypeConstraint.from_user_type(typ) or typ)
                         for name,
                         typ in fields)

    self._user_type = user_type

    # Note schema ID can be None if the schema is not registered yet.
    # Currently registration happens when converting to schema protos, in
    # apache_beam.typehints.schemas
    if self._user_type is not None:
      self._schema_id = getattr(self._user_type, _BEAM_SCHEMA_ID, None)
    else:
      self._schema_id = None

  @staticmethod
  def from_user_type(user_type: type) -> Optional[RowTypeConstraint]:
    if match_is_named_tuple(user_type):
      fields = [(name, user_type.__annotations__[name])
                for name in user_type._fields]

      return RowTypeConstraint(fields=fields, user_type=user_type)

    return None

  @staticmethod
  def from_fields(fields: Sequence[Tuple[str, type]]) -> RowTypeConstraint:
    return RowTypeConstraint(fields=fields, user_type=None)

  @property
  def user_type(self):
    return self._user_type

  def set_schema_id(self, schema_id):
    self._schema_id = schema_id
    if self._user_type is not None:
      setattr(self._user_type, _BEAM_SCHEMA_ID, self._schema_id)

  @property
  def schema_id(self):
    return self._schema_id

  def _consistent_with_check_(self, sub):
    return self == sub

  def type_check(self, instance):
    from apache_beam import Row
    return isinstance(instance, (Row, self._user_type))

  def _inner_types(self):
    """Iterates over the inner types of the composite type."""
    return [field[1] for field in self._fields]

  def __eq__(self, other):
    return type(self) == type(other) and self._fields == other._fields

  def __hash__(self):
    return hash(self._fields)

  def __repr__(self):
    return 'Row(%s)' % ', '.join(
        '%s=%s' % (name, typehints._unified_repr(t)) for name,
        t in self._fields)

  def get_type_for(self, name):
    return dict(self._fields)[name]
