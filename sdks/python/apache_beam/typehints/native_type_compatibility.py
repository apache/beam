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

"""Module to convert Python's native typing types to Beam types."""

# pytype: skip-file

import collections
import logging
import sys
import typing

from apache_beam.typehints import typehints

_LOGGER = logging.getLogger(__name__)

# Describes an entry in the type map in convert_to_beam_type.
# match is a function that takes a user type and returns whether the conversion
# should trigger.
# arity is the expected arity of the user type. -1 means it's variadic.
# beam_type is the Beam type the user type should map to.
_TypeMapEntry = collections.namedtuple(
    '_TypeMapEntry', ['match', 'arity', 'beam_type'])


def _get_args(typ):
  """Returns a list of arguments to the given type.

  Args:
    typ: A typing module typing type.

  Returns:
    A tuple of args.
  """
  try:
    if typ.__args__ is None:
      return ()
    return typ.__args__
  except AttributeError:
    if isinstance(typ, typing.TypeVar):
      return (typ.__name__, )
    return ()


def _safe_issubclass(derived, parent):
  """Like issubclass, but swallows TypeErrors.

  This is useful for when either parameter might not actually be a class,
  e.g. typing.Union isn't actually a class.

  Args:
    derived: As in issubclass.
    parent: As in issubclass.

  Returns:
    issubclass(derived, parent), or False if a TypeError was raised.
  """
  try:
    return issubclass(derived, parent)
  except (TypeError, AttributeError):
    if hasattr(derived, '__origin__'):
      try:
        return issubclass(derived.__origin__, parent)
      except TypeError:
        pass
    return False


def _match_issubclass(match_against):
  return lambda user_type: _safe_issubclass(user_type, match_against)


def _match_is_exactly_mapping(user_type):
  # Avoid unintentionally catching all subtypes (e.g. strings and mappings).
  if sys.version_info < (3, 7):
    expected_origin = typing.Mapping
  else:
    expected_origin = collections.abc.Mapping
  return getattr(user_type, '__origin__', None) is expected_origin


def _match_is_exactly_iterable(user_type):
  if user_type is typing.Iterable:
    return True
  # Avoid unintentionally catching all subtypes (e.g. strings and mappings).
  if sys.version_info < (3, 7):
    expected_origin = typing.Iterable
  else:
    expected_origin = collections.abc.Iterable
  return getattr(user_type, '__origin__', None) is expected_origin


def match_is_named_tuple(user_type):
  return (
      _safe_issubclass(user_type, typing.Tuple) and
      hasattr(user_type, '_field_types'))


def _match_is_optional(user_type):
  return _match_is_union(user_type) and sum(
      tp is type(None) for tp in _get_args(user_type)) == 1


def extract_optional_type(user_type):
  """Extracts the non-None type from Optional type user_type.

  If user_type is not Optional, returns None
  """
  if not _match_is_optional(user_type):
    return None
  else:
    return next(tp for tp in _get_args(user_type) if tp is not type(None))


def _match_is_union(user_type):
  # For non-subscripted unions (Python 2.7.14+ with typing 3.64)
  if user_type is typing.Union:
    return True

  try:  # Python 3.5.4+, or Python 2.7.14+ with typing 3.64
    return user_type.__origin__ is typing.Union
  except AttributeError:
    pass

  return False


def is_any(typ):
  return typ is typing.Any


def is_new_type(typ):
  return hasattr(typ, '__supertype__')


try:
  _ForwardRef = typing.ForwardRef  # Python 3.7+
except AttributeError:
  _ForwardRef = typing._ForwardRef


def is_forward_ref(typ):
  return isinstance(typ, _ForwardRef)


# Mapping from typing.TypeVar/typehints.TypeVariable ids to an object of the
# other type. Bidirectional mapping preserves typing.TypeVar instances.
_type_var_cache = {}  # type: typing.Dict[int, typehints.TypeVariable]


def convert_to_beam_type(typ):
  """Convert a given typing type to a Beam type.

  Args:
    typ (`typing.Union[type, str]`): typing type or string literal representing
      a type.

  Returns:
    type: The given type converted to a Beam type as far as we can do the
    conversion.

  Raises:
    ValueError: The type was malformed.
  """
  if isinstance(typ, typing.TypeVar):
    # This is a special case, as it's not parameterized by types.
    # Also, identity must be preserved through conversion (i.e. the same
    # TypeVar instance must get converted into the same TypeVariable instance).
    # A global cache should be OK as the number of distinct type variables
    # is generally small.
    if id(typ) not in _type_var_cache:
      new_type_variable = typehints.TypeVariable(typ.__name__)
      _type_var_cache[id(typ)] = new_type_variable
      _type_var_cache[id(new_type_variable)] = typ
    return _type_var_cache[id(typ)]
  elif isinstance(typ, str):
    # Special case for forward references.
    # TODO(BEAM-8487): Currently unhandled.
    _LOGGER.info('Converting string literal type hint to Any: "%s"', typ)
    return typehints.Any
  elif getattr(typ, '__module__', None) != 'typing':
    # Only translate types from the typing module.
    return typ

  type_map = [
      # TODO(BEAM-9355): Currently unsupported.
      _TypeMapEntry(match=is_new_type, arity=0, beam_type=typehints.Any),
      # TODO(BEAM-8487): Currently unsupported.
      _TypeMapEntry(match=is_forward_ref, arity=0, beam_type=typehints.Any),
      _TypeMapEntry(match=is_any, arity=0, beam_type=typehints.Any),
      _TypeMapEntry(
          match=_match_issubclass(typing.Dict),
          arity=2,
          beam_type=typehints.Dict),
      _TypeMapEntry(
          match=_match_is_exactly_iterable,
          arity=1,
          beam_type=typehints.Iterable),
      _TypeMapEntry(
          match=_match_issubclass(typing.List),
          arity=1,
          beam_type=typehints.List),
      _TypeMapEntry(
          match=_match_issubclass(typing.Set), arity=1,
          beam_type=typehints.Set),
      _TypeMapEntry(
          match=_match_issubclass(typing.FrozenSet),
          arity=1,
          beam_type=typehints.FrozenSet),
      # NamedTuple is a subclass of Tuple, but it needs special handling.
      # We just convert it to Any for now.
      # This MUST appear before the entry for the normal Tuple.
      _TypeMapEntry(
          match=match_is_named_tuple, arity=0, beam_type=typehints.Any),
      _TypeMapEntry(
          match=_match_issubclass(typing.Tuple),
          arity=-1,
          beam_type=typehints.Tuple),
      _TypeMapEntry(match=_match_is_union, arity=-1, beam_type=typehints.Union),
      _TypeMapEntry(
          match=_match_issubclass(typing.Generator),
          arity=3,
          beam_type=typehints.Generator),
      _TypeMapEntry(
          match=_match_issubclass(typing.Iterator),
          arity=1,
          beam_type=typehints.Iterator),
  ]

  # Find the first matching entry.
  matched_entry = next((entry for entry in type_map if entry.match(typ)), None)
  if not matched_entry:
    # Please add missing type support if you see this message.
    _LOGGER.info('Using Any for unsupported type: %s', typ)
    return typehints.Any

  args = _get_args(typ)
  len_args = len(args)
  if len_args == 0 and len_args != matched_entry.arity:
    arity = matched_entry.arity
    # Handle unsubscripted types.
    if _match_issubclass(typing.Tuple)(typ):
      args = (typehints.TypeVariable('T'), Ellipsis)
    elif _match_is_union(typ):
      raise ValueError('Unsupported Union with no arguments.')
    elif _match_issubclass(typing.Generator)(typ):
      raise ValueError('Unsupported Generator with no arguments.')
    elif _match_issubclass(typing.Dict)(typ):
      args = (typehints.TypeVariable('KT'), typehints.TypeVariable('VT'))
    elif (_match_issubclass(typing.Iterator)(typ) or
          _match_issubclass(typing.Generator)(typ) or
          _match_is_exactly_iterable(typ)):
      args = (typehints.TypeVariable('T_co'), )
    else:
      args = (typehints.TypeVariable('T'), ) * arity
  elif matched_entry.arity == -1:
    arity = len_args
  else:
    arity = matched_entry.arity
    if len_args != arity:
      raise ValueError(
          'expecting type %s to have arity %d, had arity %d '
          'instead' % (str(typ), arity, len_args))
  typs = convert_to_beam_types(args)
  if arity == 0:
    # Nullary types (e.g. Any) don't accept empty tuples as arguments.
    return matched_entry.beam_type
  elif arity == 1:
    # Unary types (e.g. Set) don't accept 1-tuples as arguments
    return matched_entry.beam_type[typs[0]]
  else:
    return matched_entry.beam_type[tuple(typs)]


def convert_to_beam_types(args):
  """Convert the given list or dictionary of args to Beam types.

  Args:
    args: Either an iterable of types, or a dictionary where the values are
    types.

  Returns:
    If given an iterable, a list of converted types. If given a dictionary,
    a dictionary with the same keys, and values which have been converted.
  """
  if isinstance(args, dict):
    return {k: convert_to_beam_type(v) for k, v in args.items()}
  else:
    return [convert_to_beam_type(v) for v in args]


def convert_to_typing_type(typ):
  """Converts a given Beam type to a typing type.

  This is the reverse of convert_to_beam_type.

  Args:
    typ: If a typehints.TypeConstraint, the type to convert. Otherwise, typ
      will be unchanged.

  Returns:
    Converted version of typ, or unchanged.

  Raises:
    ValueError: The type was malformed or could not be converted.
  """
  if isinstance(typ, typehints.TypeVariable):
    # This is a special case, as it's not parameterized by types.
    # Also, identity must be preserved through conversion (i.e. the same
    # TypeVariable instance must get converted into the same TypeVar instance).
    # A global cache should be OK as the number of distinct type variables
    # is generally small.
    if id(typ) not in _type_var_cache:
      new_type_variable = typing.TypeVar(typ.name)
      _type_var_cache[id(typ)] = new_type_variable
      _type_var_cache[id(new_type_variable)] = typ
    return _type_var_cache[id(typ)]
  elif not getattr(typ, '__module__', None).endswith('typehints'):
    # Only translate types from the typehints module.
    return typ

  if isinstance(typ, typehints.AnyTypeConstraint):
    return typing.Any
  if isinstance(typ, typehints.DictConstraint):
    return typing.Dict[convert_to_typing_type(typ.key_type),
                       convert_to_typing_type(typ.value_type)]
  if isinstance(typ, typehints.ListConstraint):
    return typing.List[convert_to_typing_type(typ.inner_type)]
  if isinstance(typ, typehints.IterableTypeConstraint):
    return typing.Iterable[convert_to_typing_type(typ.inner_type)]
  if isinstance(typ, typehints.UnionConstraint):
    return typing.Union[tuple(convert_to_typing_types(typ.union_types))]
  if isinstance(typ, typehints.SetTypeConstraint):
    return typing.Set[convert_to_typing_type(typ.inner_type)]
  if isinstance(typ, typehints.FrozenSetTypeConstraint):
    return typing.FrozenSet[convert_to_typing_type(typ.inner_type)]
  if isinstance(typ, typehints.TupleConstraint):
    return typing.Tuple[tuple(convert_to_typing_types(typ.tuple_types))]
  if isinstance(typ, typehints.TupleSequenceConstraint):
    return typing.Tuple[convert_to_typing_type(typ.inner_type), ...]
  if isinstance(typ, typehints.IteratorTypeConstraint):
    return typing.Iterator[convert_to_typing_type(typ.yielded_type)]

  raise ValueError('Failed to convert Beam type: %s' % typ)


def convert_to_typing_types(args):
  """Convert the given list or dictionary of args to typing types.

  Args:
    args: Either an iterable of types, or a dictionary where the values are
    types.

  Returns:
    If given an iterable, a list of converted types. If given a dictionary,
    a dictionary with the same keys, and values which have been converted.
  """
  if isinstance(args, dict):
    return {k: convert_to_typing_type(v) for k, v in args.items()}
  else:
    return [convert_to_typing_type(v) for v in args]
