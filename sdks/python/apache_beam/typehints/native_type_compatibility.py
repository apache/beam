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

from __future__ import absolute_import

import collections
import sys
import typing
from builtins import next
from builtins import range

from apache_beam.typehints import typehints

# Describes an entry in the type map in convert_to_beam_type.
# match is a function that takes a user type and returns whether the conversion
# should trigger.
# arity is the expected arity of the user type. -1 means it's variadic.
# beam_type is the Beam type the user type should map to.
_TypeMapEntry = collections.namedtuple(
    '_TypeMapEntry', ['match', 'arity', 'beam_type'])


def _get_compatible_args(typ):
  # On Python versions < 3.5.3, the Tuple and Union type from typing do
  # not have an __args__ attribute, but a __tuple_params__, and a
  # __union_params__ argument respectively.
  if (3, 0, 0) <= sys.version_info[0:3] < (3, 5, 3):
    if getattr(typ, '__tuple_params__', None) is not None:
      return typ.__tuple_params__
    elif getattr(typ, '__union_params__', None) is not None:
      return typ.__union_params__
  return None


def _get_arg(typ, index):
  """Returns the index-th argument to the given type."""
  try:
    return typ.__args__[index]
  except AttributeError:
    compatible_args = _get_compatible_args(typ)
    if compatible_args is None:
      raise
    return compatible_args[index]


def _len_arg(typ):
  """Returns the length of the arguments to the given type."""
  try:
    return len(typ.__args__)
  except AttributeError:
    compatible_args = _get_compatible_args(typ)
    if compatible_args is None:
      return 0
    return len(compatible_args)


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
  except TypeError:
    return False


def _match_issubclass(match_against):
  return lambda user_type: _safe_issubclass(user_type, match_against)


def _match_same_type(match_against):
  # For Union types. They can't be compared with isinstance either, so we
  # have to compare their types directly.
  return lambda user_type: type(user_type) == type(match_against)


def _match_is_named_tuple(user_type):
  return (_safe_issubclass(user_type, typing.Tuple) and
          hasattr(user_type, '_field_types'))


def convert_to_beam_type(typ):
  """Convert a given typing type to a Beam type.

  Args:
    typ (type): typing type.

  Returns:
    type: The given type converted to a Beam type as far as we can do the
    conversion.

  Raises:
    ~exceptions.ValueError: The type was malformed.
  """

  type_map = [
      _TypeMapEntry(
          match=_match_same_type(typing.Any),
          arity=0,
          beam_type=typehints.Any),
      _TypeMapEntry(
          match=_match_issubclass(typing.Dict),
          arity=2,
          beam_type=typehints.Dict),
      _TypeMapEntry(
          match=_match_issubclass(typing.List),
          arity=1,
          beam_type=typehints.List),
      _TypeMapEntry(
          match=_match_issubclass(typing.Set),
          arity=1,
          beam_type=typehints.Set),
      # NamedTuple is a subclass of Tuple, but it needs special handling.
      # We just convert it to Any for now.
      # This MUST appear before the entry for the normal Tuple.
      _TypeMapEntry(
          match=_match_is_named_tuple, arity=0, beam_type=typehints.Any),
      _TypeMapEntry(
          match=_match_issubclass(typing.Tuple),
          arity=-1,
          beam_type=typehints.Tuple),
      _TypeMapEntry(
          match=_match_same_type(typing.Union),
          arity=-1,
          beam_type=typehints.Union)
  ]

  # Find the first matching entry.
  matched_entry = next((entry for entry in type_map if entry.match(typ)), None)
  if not matched_entry:
    # No match: return original type.
    return typ

  if matched_entry.arity == -1:
    arity = _len_arg(typ)
  else:
    arity = matched_entry.arity
    if _len_arg(typ) != arity:
      raise ValueError('expecting type %s to have arity %d, had arity %d '
                       'instead' % (str(typ), arity, _len_arg(typ)))
  typs = [convert_to_beam_type(_get_arg(typ, i)) for i in range(arity)]
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
