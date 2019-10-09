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
https://docs.python.org/2/library/dis.html.  The first argument is a (mutable)
FrameState object, the second the integer opcode argument.

Bytecodes with more complicated behavior (e.g. modifying the program counter)
are handled inline rather than here.

For internal use only; no backwards-compatibility guarantees.
"""
from __future__ import absolute_import

import inspect
import sys
import types
import typing
from functools import reduce

from past.builtins import unicode

from apache_beam.typehints.native_type_compatibility import get_args
from apache_beam.typehints.native_type_compatibility import is_Any
from apache_beam.typehints.native_type_compatibility import is_Iterable
from apache_beam.typehints.native_type_compatibility import is_List
from apache_beam.typehints.native_type_compatibility import is_Set
from apache_beam.typehints.native_type_compatibility import is_Tuple
from apache_beam.typehints.trivial_inference import BoundMethod
from apache_beam.typehints.trivial_inference import Const
from apache_beam.typehints.trivial_inference import Empty
from apache_beam.typehints.trivial_inference import element_typing_type
from apache_beam.typehints.trivial_inference import union


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
  state.stack.append(typing.Iterable[element_typing_type(state.stack.pop())])


def symmetric_binary_op(state, unused_arg):
  # Not all binary ops are defined for all types of a and b, but this function
  # does not care about that.
  # Some results may not make sense since this is also used for non-symmetric
  # operations. For example:
  #   Set[str] - Set[int] = Set[Union[str, int]]  (should be Set[str])
  b, a = state.stack.pop(), state.stack.pop()
  if a == b:
    state.stack.append(a)
  elif ((is_List(a) and is_List(b)) or
        (is_Tuple(a) and is_Tuple(b)) or
        (is_Set(a) and is_Set(b)) or
        (is_Iterable(a) and is_Iterable(b))):
    state.stack.append(union(a, b))
  else:
    state.stack.append(typing.Any)


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
  base = state.stack.pop()
  is_base_indexable = is_Tuple(base) or is_List(base) or is_Set(base)
  if base in (str, unicode):
    out = base
  elif (isinstance(index, Const) and isinstance(index.value, int)
        and is_base_indexable):
    try:
      out = get_args(base)[index.value]
    except IndexError:
      out = element_typing_type(base)
  elif index == slice and is_base_indexable:
    out = base
  else:
    out = element_typing_type(base)
  state.stack.append(out)


# As far as types are concerned.
binary_lshift = inplace_lshift = binary_rshift = inplace_rshift = pop_top

binary_and = inplace_and = symmetric_binary_op
binary_xor = inplace_xor = symmetric_binary_op
binary_or = inpalce_or = symmetric_binary_op

# As far as types are concerned.
slice_0 = nop
slice_1 = slice_2 = pop_top
slice_3 = pop_two
store_slice_0 = store_slice_1 = store_slice_2 = store_slice_3 = nop
delete_slice_0 = delete_slice_1 = delete_slice_2 = delete_slice_3 = nop


def store_subscr(unused_state, unused_args):
  # TODO(robertwb): Update element/value type of iterable/dict.
  pass


binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op
binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op
binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op

# print_expr
print_item = pop_top
# print_item_to
print_newline = nop

# print_newline_to


# break_loop
# continue_loop
def list_append(state, arg):
  new_element_type = Const.unwrap(state.stack.pop())
  state.stack[-arg] = union(state.stack[-arg], typing.List[new_element_type])


def map_add(state, arg):
  new_key_type = Const.unwrap(state.stack.pop())
  new_value_type = Const.unwrap(state.stack.pop())
  state.stack[-arg] = union(state.stack[-arg],
                            typing.Dict[new_key_type, new_value_type])


load_locals = push_value(typing.Dict[str, typing.Any])

# return_value
# yield_value
# import_star
exec_stmt = pop_three
# pop_block
# end_finally
build_class = pop_three

# setup_with
# with_cleanup


# store_name
# delete_name
def unpack_sequence(state, arg):
  t = state.stack.pop()
  if isinstance(t, Const):
    try:
      unpacked = [Const(ti) for ti in t.value]
      if len(unpacked) != arg:
        unpacked = [typing.Any] * arg
    except TypeError:
      unpacked = [typing.Any] * arg
  elif is_Tuple(t, allow_ellipsis=False) and len(get_args(t)) == arg:
    unpacked = list(get_args(t))
  else:
    unpacked = [element_typing_type(t)] * arg
  state.stack += reversed(unpacked)


def dup_topx(state, arg):
  state.stack += state[-arg:]


store_attr = pop_top
delete_attr = nop
store_global = pop_top
delete_global = nop


def load_const(state, arg):
  state.stack.append(state.const_type(arg))


load_name = push_value(typing.Any)


def build_tuple(state, arg):
  if arg == 0:
    state.stack.append(typing.Tuple[()])
  else:
    state.stack[-arg:] = [
        typing.Tuple[tuple(Const.unwrap(t) for t in state.stack[-arg:])]]


def build_list(state, arg):
  if arg == 0:
    state.stack.append(Empty[typing.List])
  else:
    state.stack[-arg:] = [typing.List[reduce(union, state.stack[-arg:])]]


def build_map(state, unused_arg):
  state.stack.append(Empty[typing.Dict])


def load_attr(state, arg):
  """Replaces the top of the stack, TOS, with
  getattr(TOS, co_names[arg])
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
      func = getattr(o, name) # Python 3 has no unbound methods
    state.stack.append(Const(BoundMethod(func, o)))
  else:
    state.stack.append(typing.Any)


def load_method(state, arg):
  """Like load_attr. Replaces TOS object with method and TOS."""
  o = state.stack.pop()
  name = state.get_name(arg)
  if isinstance(o, Const):
    method = Const(getattr(o.value, name))
  elif is_Any(o):
    method = typing.Any
  else:
    method = Const(BoundMethod(getattr(o, name), o))

  state.stack.append(method)


def compare_op(state, unused_arg):
  # Could really be anything...
  state.stack[-2:] = [bool]


def import_name(state, unused_arg):
  state.stack[-2:] = [typing.Any]


import_from = push_value(typing.Any)

# jump

# for_iter


def load_global(state, arg):
  state.stack.append(state.get_global(arg))


# setup_loop
# setup_except
# setup_finally
store_map = pop_two


def load_fast(state, arg):
  state.stack.append(state.vars[arg])


def store_fast(state, arg):
  state.vars[arg] = state.stack.pop()


def delete_fast(state, arg):
  state.vars[arg] = typing.Any  # really an error


def load_closure(state, arg):
  state.stack.append(state.get_closure(arg))


def load_deref(state, arg):
  state.stack.append(state.closure_type(arg))
# raise_varargs


def make_function(state, arg):
  """Creates a function with the arguments at the top of the stack.
  """
  # TODO(luke-zhu): Handle default argument types
  globals = state.f.__globals__ # Inherits globals from the current frame
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
      pop_count += (num_default_pos_args + 2 * num_default_kwonly_args +
                    num_annotations + num_annotations > 0)
    elif sys.version_info >= (3, 6):
      # arg contains flags, with corresponding stack values if positive.
      # https://docs.python.org/3.6/library/dis.html#opcode-MAKE_FUNCTION
      pop_count += bin(arg).count('1')
      if arg & 0x08:
        # Convert types in Tuple constraint to a tuple of CPython cells.
        # https://stackoverflow.com/a/44670295
        closure = tuple(
            (lambda _: lambda: _)(t).__closure__[0]
            for t in get_args(state.stack[-3]))

    func = types.FunctionType(func_code, globals, name=func_name,
                              closure=closure)

  assert pop_count <= len(state.stack)
  state.stack[-pop_count:] = [Const(func)]


def make_closure(state, arg):
  state.stack[-arg - 2:] = [typing.Any]  # a callable


def build_slice(state, arg):
  state.stack[-arg:] = [slice]  # a slice object


def _unpack_lists(state, arg):
  """Extract inner types of Lists and Tuples.

  Pops arg count items from the stack, concatenates their inner types into 1
  list, and returns that list.
  Example: if stack[-arg:] == [[i1, i2], [i3]], the output is [i1, i2, i3]
  """
  typs = []
  for i in range(arg, 0, -1):
    typ = state.stack[-i]
    try:
      typs.extend(get_args(typ))
    except AttributeError:
      typs.append(typing.Any)
  state.stack[-arg:] = []
  return tuple(typs)


def build_list_unpack(state, arg):
  """Joins arg count iterables from the stack into a single list."""
  state.stack.append(typing.List[typing.Union[_unpack_lists(state, arg)]])


def build_tuple_unpack(state, arg):
  """Joins arg count iterables from the stack into a single tuple."""
  state.stack.append(typing.Tuple[_unpack_lists(state, arg)])


def build_tuple_unpack_with_call(state, arg):
  """Same as build_tuple_unpack, with an extra fn argument at the bottom of the
  stack, which remains untouched."""
  build_tuple_unpack(state, arg)
