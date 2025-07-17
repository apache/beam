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

from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Tuple

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple
from apache_beam.typehints.schema_registry import SchemaTypeRegistry

# Name of the attribute added to user types (existing and generated) to store
# the corresponding schema ID
_BEAM_SCHEMA_ID = "_beam_schema_id"


def _user_type_is_generated(user_type: type) -> bool:
  if not hasattr(user_type, _BEAM_SCHEMA_ID):
    return False

  schema_id = getattr(user_type, _BEAM_SCHEMA_ID)
  type_name = 'BeamSchema_{}'.format(schema_id.replace('-', '_'))
  return user_type.__name__ == type_name


class RowTypeConstraint(typehints.TypeConstraint):
  def __init__(
      self,
      fields: Sequence[Tuple[str, type]],
      user_type,
      schema_options: Optional[Sequence[Tuple[str, Any]]] = None,
      field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]] = None,
      field_descriptions: Optional[Dict[str, str]] = None):
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
      schema_options: A list of (key, value) tuples representing schema-level
        options.
      field_options: A dictionary representing field-level options. Dictionary
        keys are field names, and dictionary values are lists of (key, value)
        tuples representing field-level options for that field.
    """
    # Recursively wrap row types in a RowTypeConstraint
    self._fields = tuple((name, RowTypeConstraint.from_user_type(typ) or typ)
                         for name, typ in fields)

    self._user_type = user_type

    # Note schema ID can be None if the schema is not registered yet.
    # Currently registration happens when converting to schema protos, in
    # apache_beam.typehints.schemas
    self._schema_id = getattr(self._user_type, _BEAM_SCHEMA_ID, None)

    self._schema_options = schema_options or []
    self._field_options = field_options or {}
    self._field_descriptions = field_descriptions or {}

  @staticmethod
  def from_user_type(
      user_type: type,
      schema_options: Optional[Sequence[Tuple[str, Any]]] = None,
      field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]] = None
  ) -> Optional[RowTypeConstraint]:
    if match_is_named_tuple(user_type):
      fields = [(name, user_type.__annotations__[name])
                for name in user_type._fields]

      field_descriptions = getattr(user_type, '_field_descriptions', None)

      if _user_type_is_generated(user_type):
        return RowTypeConstraint.from_fields(
            fields,
            schema_id=getattr(user_type, _BEAM_SCHEMA_ID),
            schema_options=schema_options,
            field_options=field_options,
            field_descriptions=field_descriptions)

      # TODO(https://github.com/apache/beam/issues/22125): Add user API for
      # specifying schema/field options
      return RowTypeConstraint(
          fields=fields,
          user_type=user_type,
          schema_options=schema_options,
          field_options=field_options,
          field_descriptions=field_descriptions)

    return None

  @staticmethod
  def from_fields(
      fields: Sequence[Tuple[str, type]],
      schema_id: Optional[str] = None,
      schema_options: Optional[Sequence[Tuple[str, Any]]] = None,
      field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]] = None,
      schema_registry: Optional[SchemaTypeRegistry] = None,
      field_descriptions: Optional[Dict[str, str]] = None,
  ) -> RowTypeConstraint:
    return GeneratedClassRowTypeConstraint(
        fields,
        schema_id=schema_id,
        schema_options=schema_options,
        field_options=field_options,
        schema_registry=schema_registry,
        field_descriptions=field_descriptions)

  def __call__(self, *args, **kwargs):
    # We make RowTypeConstraint callable (defers to constructing the user type)
    # so that Python will recognize it as a type. This allows RowTypeConstraint
    # to be used in conjunction with native typehints, like Optional.
    # CPython (prior to 3.11) considers anything callable to be a type:
    # https://github.com/python/cpython/blob/d348afa15d5a997e7a8e51c0f789f41cb15cc651/Lib/typing.py#L137-L167
    return self._user_type(*args, **kwargs)

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

  @property
  def schema_options(self):
    return self._schema_options

  def field_options(self, field_name):
    # Raise if field_name is not one of the fields?
    return self._field_options.get(field_name, [])

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
        '%s=%s' % (name, repr(t)) for name, t in self._fields)

  def get_type_for(self, name):
    try:
      return dict(self._fields)[name]
    except KeyError:
      return typehints.Any


class GeneratedClassRowTypeConstraint(RowTypeConstraint):
  """Specialization of RowTypeConstraint which relies on a generated user_type.

  Since the generated user_type cannot be pickled, we supply a custom __reduce__
  function that will regenerate the user_type.
  """
  def __init__(
      self,
      fields,
      schema_id: Optional[str] = None,
      schema_options: Optional[Sequence[Tuple[str, Any]]] = None,
      field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]] = None,
      schema_registry: Optional[SchemaTypeRegistry] = None,
      field_descriptions: Optional[Dict[str, str]] = None,
  ):
    from apache_beam.typehints.schemas import named_fields_to_schema
    from apache_beam.typehints.schemas import named_tuple_from_schema

    kwargs = {'schema_registry': schema_registry} if schema_registry else {}

    schema = named_fields_to_schema(
        fields,
        schema_id=schema_id,
        schema_options=schema_options,
        field_options=field_options,
        field_descriptions=field_descriptions,
        **kwargs)
    user_type = named_tuple_from_schema(schema, **kwargs)

    super().__init__(
        fields,
        user_type,
        schema_options=schema_options,
        field_options=field_options,
        field_descriptions=field_descriptions)

  def __reduce__(self):
    return (
        RowTypeConstraint.from_fields,
        (
            self._fields,
            self._schema_id,
            self._schema_options,
            self._field_options,
            None,
        ))
