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

"""Defines the actions various bytecodes have on the frame.

Each function here corresponds to a bytecode documented in
https://docs.python.org/2/library/dis.html or
https://docs.python.org/3/library/dis.html. The first argument is a (mutable)
FrameState object, the second the integer opcode argument.

Bytecodes with more complicated behavior (e.g. modifying the program counter)
are handled inline rather than here.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from __future__ import absolute_import

import inspect
import logging
import sys
import types
from functools import reduce

from past.builtins import unicode

from apache_beam.typehints import typehints
from apache_beam.typehints.trivial_inference import BoundMethod
from apache_beam.typehints.trivial_inference import Const
from apache_beam.typehints.trivial_inference import element_type
from apache_beam.typehints.trivial_inference import union
from apache_beam.typehints.typehints import Any
from apache_beam.typehints.typehints import Dict
from apache_beam.typehints.typehints import Iterable
from apache_beam.typehints.typehints import List
from apache_beam.typehints.typehints import Tuple
from apache_beam.typehints.typehints import Union

# This is missing in the builtin types module.  str.upper is arbitrary, any
# method on a C-implemented type will do.
_MethodDescriptorType = type(str.upper)


def pop_one(state, unused_arg):
  del state.stack[-1:]


def pop_two(state, unused_arg):
  del state.stack[-2:]


def pop_three(state, unused_arg):
  del state.stack[-3:]


def push_value(v):
  def pusher(state, unused_arg):
    state.stack.append(v)

  return pusher


def nop(unused_state, unused_arg):
  pass


def pop_top(state, unused_arg):
  state.stack.pop()


def rot_n(state, n):
  state.stack[-n:] = [state.stack[-1]] + state.stack[-n:-1]


def rot_two(state, unused_arg):
  rot_n(state, 2)


def rot_three(state, unused_arg):
  rot_n(state, 3)


def rot_four(state, unused_arg):
  rot_n(state, 4)


def dup_top(state, unused_arg):
  state.stack.append(state.stack[-1])


def unary(state, unused_arg):
  state.stack[-1] = Const.unwrap(state.stack[-1])


unary_positive = unary_negative = unary_invert = unary


def unary_not(state, unused_arg):
  state.stack[-1] = bool


def unary_convert(state, unused_arg):
  state.stack[-1] = str


def get_iter(state, unused_arg):
  state.stack.append(Iterable[element_type(state.stack.pop())])


def symmetric_binary_op(state, unused_arg):
  # TODO(robertwb): This may not be entirely correct...
  b, a = Const.unwrap(state.stack.pop()), Const.unwrap(state.stack.pop())
  if a == b:
    state.stack.append(a)
  elif type(a) == type(b) and isinstance(a, typehints.SequenceTypeConstraint):
    state.stack.append(type(a)(union(element_type(a), element_type(b))))
  else:
    state.stack.append(Any)


# Except for int ** -int
binary_power = inplace_power = symmetric_binary_op
binary_multiply = inplace_multiply = symmetric_binary_op
binary_divide = inplace_divide = symmetric_binary_op
binary_floor_divide = inplace_floor_divide = symmetric_binary_op


def binary_true_divide(state, unused_arg):
  u = union(state.stack.pop(), state.stack.pop)
  if u == int:
    state.stack.append(float)
  else:
    state.stack.append(u)


inplace_true_divide = binary_true_divide

binary_modulo = inplace_modulo = symmetric_binary_op
# TODO(robertwb): Tuple add.
binary_add = inplace_add = symmetric_binary_op
binary_subtract = inplace_subtract = symmetric_binary_op


def binary_subscr(state, unused_arg):
  index = state.stack.pop()
  base = Const.unwrap(state.stack.pop())
  if base in (str, unicode):
    out = base
  elif (isinstance(index, Const) and isinstance(index.value, int) and
        isinstance(base, typehints.IndexableTypeConstraint)):
    try:
      out = base._constraint_for_index(index.value)
    except IndexError:
      out = element_type(base)
  elif index == slice and isinstance(base, typehints.IndexableTypeConstraint):
    out = base
  else:
    out = element_type(base)
  state.stack.append(out)


# As far as types are concerned.
binary_lshift = inplace_lshift = binary_rshift = inplace_rshift = pop_top

binary_and = inplace_and = symmetric_binary_op
binary_xor = inplace_xor = symmetric_binary_op
binary_or = inpalce_or = symmetric_binary_op


def store_subscr(unused_state, unused_args):
  # TODO(robertwb): Update element/value type of iterable/dict.
  pass


print_item = pop_top
print_newline = nop


def list_append(state, arg):
  new_element_type = Const.unwrap(state.stack.pop())
  state.stack[-arg] = List[Union[element_type(state.stack[-arg]),
                                 new_element_type]]


def map_add(state, arg):
  if sys.version_info >= (3, 8):
    # PEP 572 The MAP_ADD expects the value as the first element in the stack
    # and the key as the second element.
    new_value_type = Const.unwrap(state.stack.pop())
    new_key_type = Const.unwrap(state.stack.pop())
  else:
    new_key_type = Const.unwrap(state.stack.pop())
    new_value_type = Const.unwrap(state.stack.pop())
  state.stack[-arg] = Dict[Union[state.stack[-arg].key_type, new_key_type],
                           Union[state.stack[-arg].value_type, new_value_type]]


load_locals = push_value(Dict[str, Any])
exec_stmt = pop_three
build_class = pop_three


def unpack_sequence(state, arg):
  t = state.stack.pop()
  if isinstance(t, Const):
    try:
      unpacked = [Const(ti) for ti in t.value]
      if len(unpacked) != arg:
        unpacked = [Any] * arg
    except TypeError:
      unpacked = [Any] * arg
  elif (isinstance(t, typehints.TupleHint.TupleConstraint) and
        len(t.tuple_types) == arg):
    unpacked = list(t.tuple_types)
  else:
    unpacked = [element_type(t)] * arg
  state.stack += reversed(unpacked)


def dup_topx(state, arg):
  state.stack += state[-arg:]


store_attr = pop_top
delete_attr = nop
store_global = pop_top
delete_global = nop


def load_const(state, arg):
  state.stack.append(state.const_type(arg))


load_name = push_value(Any)


def build_tuple(state, arg):
  if arg == 0:
    state.stack.append(Tuple[()])
  else:
    state.stack[-arg:] = [Tuple[[Const.unwrap(t) for t in state.stack[-arg:]]]]


def build_list(state, arg):
  if arg == 0:
    state.stack.append(List[Union[()]])
  else:
    state.stack[-arg:] = [List[reduce(union, state.stack[-arg:], Union[()])]]


# A Dict[Union[], Union[]] is the type of an empty dict.
def build_map(state, arg):
  if sys.version_info <= (2, ) or arg == 0:
    state.stack.append(Dict[Union[()], Union[()]])
  else:
    state.stack[-arg:] = [
        Dict[reduce(union, state.stack[-2 * arg::2], Union[()]),
             reduce(union, state.stack[-2 * arg + 1::2], Union[()])]
    ]


def build_const_key_map(state, arg):
  key_tuple = state.stack.pop()
  if isinstance(key_tuple, typehints.TupleHint.TupleConstraint):
    key_types = key_tuple.tuple_types
  elif isinstance(key_tuple, Const):
    key_types = [Const(v) for v in key_tuple.value]
  else:
    key_types = [Any]
  state.stack[-arg:] = [
      Dict[reduce(union, key_types, Union[()]),
           reduce(union, state.stack[-arg:], Union[()])]
  ]


def load_attr(state, arg):
  """Replaces the top of the stack, TOS, with
  getattr(TOS, co_names[arg])

  Will replace with Any for builtin methods, but these don't have bytecode in
  CPython so that's okay.
  """
  o = state.stack.pop()
  name = state.get_name(arg)
  if isinstance(o, Const) and hasattr(o.value, name):
    state.stack.append(Const(getattr(o.value, name)))
  elif (inspect.isclass(o) and
        isinstance(getattr(o, name, None),
                   (types.MethodType, types.FunctionType))):
    # TODO(luke-zhu): Support other callable objects
    if sys.version_info[0] == 2:
      func = getattr(o, name).__func__
    else:
      func = getattr(o, name)  # Python 3 has no unbound methods
    state.stack.append(Const(BoundMethod(func, o)))
  else:
    state.stack.append(Any)


def load_method(state, arg):
  """Like load_attr. Replaces TOS object with method and TOS."""
  o = state.stack.pop()
  name = state.get_name(arg)
  if isinstance(o, Const):
    method = Const(getattr(o.value, name))
  elif isinstance(o, typehints.AnyTypeConstraint):
    method = typehints.Any
  elif hasattr(o, name):
    attr = getattr(o, name)
    if isinstance(attr, _MethodDescriptorType):
      # Skip builtins since they don't disassemble.
      method = typehints.Any
    else:
      method = Const(BoundMethod(attr, o))
  else:
    method = typehints.Any

  state.stack.append(method)


def compare_op(state, unused_arg):
  # Could really be anything...
  state.stack[-2:] = [bool]


def import_name(state, unused_arg):
  state.stack[-2:] = [Any]


import_from = push_value(Any)


def load_global(state, arg):
  state.stack.append(state.get_global(arg))


store_map = pop_two


def load_fast(state, arg):
  state.stack.append(state.vars[arg])


def store_fast(state, arg):
  state.vars[arg] = state.stack.pop()


def delete_fast(state, arg):
  state.vars[arg] = Any  # really an error


def load_closure(state, arg):
  state.stack.append(state.get_closure(arg))


def load_deref(state, arg):
  state.stack.append(state.closure_type(arg))


def make_function(state, arg):
  """Creates a function with the arguments at the top of the stack.
  """
  # TODO(luke-zhu): Handle default argument types
  globals = state.f.__globals__  # Inherits globals from the current frame
  if sys.version_info[0] == 2:
    func_code = state.stack[-1].value
    func = types.FunctionType(func_code, globals)
    # argc is the number of default parameters. Ignored here.
    pop_count = 1 + arg
  else:  # Python 3.x
    func_name = state.stack[-1].value
    func_code = state.stack[-2].value
    pop_count = 2
    closure = None
    if sys.version_info[:2] == (3, 5):
      # https://docs.python.org/3.5/library/dis.html#opcode-MAKE_FUNCTION
      num_default_pos_args = (arg & 0xff)
      num_default_kwonly_args = ((arg >> 8) & 0xff)
      num_annotations = ((arg >> 16) & 0x7fff)
      pop_count += (
          num_default_pos_args + 2 * num_default_kwonly_args + num_annotations +
          num_annotations > 0)
    elif sys.version_info >= (3, 6):
      # arg contains flags, with corresponding stack values if positive.
      # https://docs.python.org/3.6/library/dis.html#opcode-MAKE_FUNCTION
      pop_count += bin(arg).count('1')
      if arg & 0x08:
        # Convert types in Tuple constraint to a tuple of CPython cells.
        # https://stackoverflow.com/a/44670295
        closure = tuple((lambda _: lambda: _)(t).__closure__[0]
                        for t in state.stack[-3].tuple_types)

    func = types.FunctionType(
        func_code, globals, name=func_name, closure=closure)

  assert pop_count <= len(state.stack)
  state.stack[-pop_count:] = [Const(func)]


def make_closure(state, arg):
  state.stack[-arg - 2:] = [Any]  # a callable


def build_slice(state, arg):
  state.stack[-arg:] = [slice]  # a slice object


def _unpack_lists(state, arg):
  """Extract inner types of Lists and Tuples.

  Pops arg count items from the stack, concatenates their inner types into 1
  list, and returns that list.
  Example: if stack[-arg:] == [[i1, i2], [i3]], the output is [i1, i2, i3]
  """
  types = []
  for i in range(arg, 0, -1):
    type_constraint = state.stack[-i]
    if isinstance(type_constraint, typehints.IndexableTypeConstraint):
      types.extend(type_constraint._inner_types())
    else:
      logging.debug('Unhandled type_constraint: %r', type_constraint)
      types.append(typehints.Any)
  state.stack[-arg:] = []
  return types


def build_list_unpack(state, arg):
  """Joins arg count iterables from the stack into a single list."""
  state.stack.append(List[Union[_unpack_lists(state, arg)]])


def build_tuple_unpack(state, arg):
  """Joins arg count iterables from the stack into a single tuple."""
  state.stack.append(Tuple[_unpack_lists(state, arg)])


def build_tuple_unpack_with_call(state, arg):
  """Same as build_tuple_unpack, with an extra fn argument at the bottom of the
  stack, which remains untouched."""
  build_tuple_unpack(state, arg)
