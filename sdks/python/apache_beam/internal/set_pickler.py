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

"""Custom pickling logic for sets to make the serialization semi-deterministic.

To make set serialization semi-deterministic, we must pick an order for the set
elements. Sets may contain elements of types not defining a comparison "<"
operator. To provide an order, we define our own custom comparison function
which supports elements of near-arbitrary types and use that to sort the
contents of each set during serialization. Attempts at determinism are made on a
best-effort basis to improve hit rates for cached workflows and the ordering
does not define a total order for all values.
"""

import enum
import functools


def compare(lhs, rhs):
  """Returns -1, 0, or 1 depending on whether lhs <, =, or > rhs."""
  if lhs < rhs:
    return -1
  elif lhs > rhs:
    return 1
  else:
    return 0


def generic_object_comparison(lhs, rhs, lhs_path, rhs_path, max_depth):
  """Identifies which object goes first in an (almost) total order of objects.

  Args:
    lhs: An arbitrary Python object or built-in type.
    rhs: An arbitrary Python object or built-in type.
    lhs_path: Traversal path from the root lhs object up to, but not including,
      lhs. The original contents of lhs_path are restored before the function
      returns.
    rhs_path: Same as lhs_path except for the rhs.
    max_depth: Maximum recursion depth.

  Returns:
    -1, 0, or 1 depending on whether lhs or rhs goes first in the total order.
    0 if max_depth is exhausted.
    0 if lhs is in lhs_path or rhs is in rhs_path (there is a cycle).
  """
  if id(lhs) == id(rhs):
    # Fast path
    return 0
  if type(lhs) != type(rhs):
    return compare(str(type(lhs)), str(type(rhs)))
  if type(lhs) in [int, float, bool, str, bool, bytes, bytearray]:
    return compare(lhs, rhs)
  if isinstance(lhs, enum.Enum):
    # Enums can have values with arbitrary types. The names are strings.
    return compare(lhs.name, rhs.name)

  # To avoid exceeding the recursion depth limit, set a limit on recursion.
  max_depth -= 1
  if max_depth < 0:
    return 0

  # Check for cycles in the traversal path to avoid getting stuck in a loop.
  if id(lhs) in lhs_path or id(rhs) in rhs_path:
    return 0
  lhs_path.append(id(lhs))
  rhs_path.append(id(rhs))
  # The comparison logic is split across two functions to simplifying updating
  # and restoring the traversal paths.
  result = _generic_object_comparison_recursive_path(
      lhs, rhs, lhs_path, rhs_path, max_depth)
  lhs_path.pop()
  rhs_path.pop()
  return result


def _generic_object_comparison_recursive_path(
    lhs, rhs, lhs_path, rhs_path, max_depth):
  if type(lhs) == tuple or type(lhs) == list:
    result = compare(len(lhs), len(rhs))
    if result != 0:
      return result
    for i in range(len(lhs)):
      result = generic_object_comparison(
          lhs[i], rhs[i], lhs_path, rhs_path, max_depth)
      if result != 0:
        return result
    return 0
  if type(lhs) == frozenset or type(lhs) == set:
    return generic_object_comparison(
        tuple(sort_if_possible(lhs, lhs_path, rhs_path, max_depth)),
        tuple(sort_if_possible(rhs, lhs_path, rhs_path, max_depth)),
        lhs_path,
        rhs_path,
        max_depth)
  if type(lhs) == dict:
    lhs_keys = list(lhs.keys())
    rhs_keys = list(rhs.keys())
    result = compare(len(lhs_keys), len(rhs_keys))
    if result != 0:
      return result
    lhs_keys = sort_if_possible(lhs_keys, lhs_path, rhs_path, max_depth)
    rhs_keys = sort_if_possible(rhs_keys, lhs_path, rhs_path, max_depth)
    for lhs_key, rhs_key in zip(lhs_keys, rhs_keys):
      result = generic_object_comparison(
          lhs_key, rhs_key, lhs_path, rhs_path, max_depth)
      if result != 0:
        return result
      result = generic_object_comparison(
          lhs[lhs_key], rhs[rhs_key], lhs_path, rhs_path, max_depth)
      if result != 0:
        return result

  lhs_fields = dir(lhs)
  rhs_fields = dir(rhs)
  result = compare(len(lhs_fields), len(rhs_fields))
  if result != 0:
    return result
  for i in range(len(lhs_fields)):
    result = compare(lhs_fields[i], rhs_fields[i])
    if result != 0:
      return result
    result = generic_object_comparison(
        getattr(lhs, lhs_fields[i], None),
        getattr(rhs, rhs_fields[i], None),
        lhs_path,
        rhs_path,
        max_depth)
    if result != 0:
      return result
  return 0


def sort_if_possible(obj, lhs_path=None, rhs_path=None, max_depth=4):
  def cmp(lhs, rhs):
    if lhs_path is None:
      # Start the traversal at the root call to cmp.
      return generic_object_comparison(lhs, rhs, [], [], max_depth)
    else:
      # Continue the existing traversal path for recursive calls to cmp.
      return generic_object_comparison(lhs, rhs, lhs_path, rhs_path, max_depth)

  return sorted(obj, key=functools.cmp_to_key(cmp))


def save_set(pickler, obj):
  pickler.save_set(sort_if_possible(obj))


def save_frozenset(pickler, obj):
  pickler.save_frozenset(sort_if_possible(obj))
